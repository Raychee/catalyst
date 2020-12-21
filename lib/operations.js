"use strict";

const debug = require('debug')('catalyst:operations');
const {set, get, isEmpty, isPlainObject, pick, pickBy, omit, isNil, mapValues, partition} = require('lodash');
const {CronTime} = require('cron');
const moment = require('moment');
const {isMatch} = require('micromatch');
const {ObjectID} = require('mongodb');

const {isEqual, shrink, diff, format} = require('@raychee/utils');
const {
    DEFAULT_TASK_DOMAIN_CONFIG, SCHEDULING_PROPERTIES, COLLECTION_NAMES, OP_BATCH_SIZE,
    TASK_TYPE_INHERITED_PROPERTIES, TASK_INHERITED_PROPERTIES, JOB_INHERITED_PROPERTIES,
    INDEXES
} = require('./config');
const {OperationError} = require('./error');


class Operations {

    /**
     * @param {import('./logger').JobLogger} logger
     * @param {import('mongodb').Db} mongodb
     * @param {import('./loader').TaskLoader} taskLoader
     */
    constructor(logger, mongodb, taskLoader) {
        this.logger = logger;
        this.schedulers = mongodb.collection(COLLECTION_NAMES.Scheduler);
        this.domains = mongodb.collection(COLLECTION_NAMES.Domain);
        this.types = mongodb.collection(COLLECTION_NAMES.Type);
        this.tasks = mongodb.collection(COLLECTION_NAMES.Task);
        this.jobs = mongodb.collection(COLLECTION_NAMES.Job);
        this.store = mongodb.collection(COLLECTION_NAMES.Store);
        this.collections = {
            [COLLECTION_NAMES.Scheduler]: this.schedulers,
            [COLLECTION_NAMES.Domain]: this.domains,
            [COLLECTION_NAMES.Type]: this.types,
            [COLLECTION_NAMES.Task]: this.tasks,
            [COLLECTION_NAMES.Job]: this.jobs,
            [COLLECTION_NAMES.Store]: this.store,
        };
        this.taskLoader = taskLoader;
    }

    async prepare() {
        for (const [name, indexes] of Object.entries(INDEXES)) {
            if (indexes.length > 0) {
                await this.collections[COLLECTION_NAMES[name]].createIndexes(indexes);
            }
        }
        await this.schedulers.updateOne({_id: 0}, {$set: {_id: 0}}, {upsert: true});
        for (const taskDomain of this.taskLoader.getAllTaskDomains()) {
            await this.ensureDomain(taskDomain.name);
        }
        for (const taskType of this.taskLoader.getAllTaskTypes()) {
            await this.ensureType(taskType.domain.name, taskType.name);
        }
    }

    /**
     * @param {string|object} domain
     * @return {Promise<object>}
     */
    async computeDomain(domain) {
        const domainName = typeof domain === 'string' ? domain : domain.domain;
        if (typeof domain === 'string') {
            domain = await this.domains.findOne({domain}) || {};
        }
        if (domain.local) domain = domain.local;
        const {_id, lockedBy, ...rest} = domain;
        return {domain: domainName, ...DEFAULT_TASK_DOMAIN_CONFIG, ...shrink(rest)};
    }

    async ensureDomain(domainName) {
        const domain = await this.computeDomain({domain: domainName});
        const now = new Date();
        const {value: {_id, ...ret} = {}} = await this.domains.findOneAndUpdate(
            {domain: domainName}, {$setOnInsert: {...domain, ctime: now, mtime: now}},
            {upsert: true, returnOriginal: false}
        );
        return ret;
    }

    async updateDomains(filter, update) {
        debug('updateDomains(%j, %j)', filter, update);
        const {domain, ..._update} = update;
        if (isEmpty(_update)) return;
        const nonNull = mapValues(
            pickBy(_update, (v, p) => p !== 'lockedBy' && isNil(v)),
            (v, p) => DEFAULT_TASK_DOMAIN_CONFIG[p]
        );
        const full = {...nonNull, mtime: new Date()};
        const result = {
            filter, update: _update, full,
            updated: await this._fullUpdateMany(this.domains, filter, _update, full)
        };
        const domains = await this.domains.find(filter).toArray();
        const taskTypeUpdate = pick({..._update, ...nonNull}, TASK_TYPE_INHERITED_PROPERTIES);
        if (!isEmpty(taskTypeUpdate)) {
            result.updateTypes = await this.updateTypes(
                {domain: {$in: domains.map(d => d.domain)}}, taskTypeUpdate, {overwrite: false}
            );
        }
        return result;
    }

    /**
     * @param {object|string} domain
     * @param {string} domain.domain
     * @param {string} domain.type
     * @param {*} domain.*
     * @param {string} [type]
     * @return {Promise<object>}
     */
    async computeType(domain, type) {
        let domainName, typeName;
        if (typeof domain === 'string' && typeof type === 'string') {
            domainName = domain;
            typeName = type;
            domain = type = undefined;
        } else if (isPlainObject(domain) && !type) {
            domainName = domain.domain;
            typeName = domain.type;
            type = domain;
            domain = undefined;
        } else {
            throw new OperationError('illegal argument: getTaskTypeConfig(', domain, ', ', type, ')');
        }
        domain = await this.computeDomain(domainName);
        if (!type) {
            type = await this.types.findOne({domain: domainName, type: typeName}) || {};
        }
        if (type.local) type = type.local;
        const {_id, lockedBy, ...rest} = type;
        type = shrink(rest);
        const {maxConcurrency, ...restDomain} = domain;
        return {domain: domainName, type: typeName, ...restDomain, ...type};
    }

    async ensureType(domainName, typeName) {
        const taskType = await this.computeType({domain: domainName, type: typeName});
        const now = new Date();
        const {value: {_id, ...ret} = {}} = await this.types.findOneAndUpdate(
            {domain: domainName, type: typeName},
            {$setOnInsert: {...taskType, ctime: now, mtime: now}},
            {upsert: true, returnOriginal: false}
        );
        return ret;
    }

    async updateTypes(filter, update, {overwrite = true} = {}) {
        debug('updateTypes(%j, %j, %j)', filter, update, {overwrite});
        const {domain, type, ..._update} = update;
        if (isEmpty(_update)) return;
        const taskTypes = await this.types.find(filter).toArray();
        const now = new Date();
        const result = {overwrite};
        if (overwrite) {
            const complexUpdate = pickBy(_update, (v, p) =>
                isNil(v) && TASK_TYPE_INHERITED_PROPERTIES.includes(p)
            );
            const simpleUpdate = omit(_update, Object.keys(complexUpdate));
            if (!isEmpty(simpleUpdate)) {
                result.simple = {
                    filter, update: simpleUpdate, full: {mtime: now},
                    updated: await this._fullUpdateMany(this.types, filter, simpleUpdate, {mtime: now})
                };
                const taskUpdate = pick(simpleUpdate, TASK_INHERITED_PROPERTIES);
                const jobUpdate = pick(taskUpdate, JOB_INHERITED_PROPERTIES);
                if (!isEmpty(taskUpdate)) {
                    if (taskTypes.length > 0) {
                        result.updateTasks = await this.updateTasks(
                            {$or: taskTypes.map(({domain, type}) => ({domain, type}))},
                            taskUpdate, {overwrite: false}
                        );
                    }
                }
                if (!isEmpty(jobUpdate)) {
                    result.updateJobs = await this._updateTypeInheritedJobs(taskTypes, jobUpdate);
                }
            }
            if (!isEmpty(complexUpdate)) {
                result.complex = [];
                for (let taskType of taskTypes) {
                    const {domain, type} = taskType;
                    taskType = await this.computeType(shrink({domain, type, ...taskType.local, ..._update}));
                    const nonNull = pick(taskType, Object.keys(complexUpdate));
                    const full = {...nonNull, mtime: now};
                    const filter = {domain, type};
                    const res = {
                        filter, update: complexUpdate, full,
                        updated: await this._fullUpdateOne(this.types, {domain, type}, complexUpdate, full)
                    };
                    result.complex.push(res);
                    const taskUpdate = pick(nonNull, TASK_INHERITED_PROPERTIES);
                    const jobUpdate = pick(taskUpdate, JOB_INHERITED_PROPERTIES);
                    if (!isEmpty(taskUpdate)) {
                        res.updateTasks = await this.updateTasks({domain, type}, taskUpdate, {overwrite: false});
                    }
                    if (!isEmpty(jobUpdate)) {
                        result.updateJobs = await this._updateTypeInheritedJobs([taskType], jobUpdate);
                    }
                }
            }
        } else {
            if (Object.values(_update).some(isNil)) {
                throw new OperationError('cannot updateTaskTypeConfigs with null values when overwrite = false');
            }
            const taskUpdates = [];
            const jobUpdates = [];
            result.updates = [];
            for (const [p, v] of Object.entries(_update)) {
                const query = {...filter, [`local.${p}`]: null};
                const update = {[p]: v};
                const targets = await this.types.find(query).toArray();
                const {matchedCount} = await this.types.updateMany(query, {$set: update});
                result.updates.push({
                    filter: query, update, updated: matchedCount
                });
                if (targets.length > 0) {
                    if (!TASK_INHERITED_PROPERTIES.includes(p)) continue;
                    taskUpdates.push([{$or: targets.map(({domain, type}) => ({domain, type}))}, update]);
                    if (!JOB_INHERITED_PROPERTIES.includes(p)) continue;
                    jobUpdates.push([targets, update]);
                }
            }
            result.updateTasks = [];
            for (const [filter, update] of taskUpdates) {
                result.updateTasks.push(await this.updateTasks(filter, update, {overwrite: false}));
            }
            result.updateJobs = [];
            for (const [taskTypes, update] of jobUpdates) {
                result.updateJobs.push(...(await this._updateTypeInheritedJobs(taskTypes, update)));
            }
        }
        return result;
    }

    async _updateTypeInheritedJobs(taskTypes, jobUpdate) {
        const updated = [];
        for (const [p, v] of Object.entries(jobUpdate)) {
            for (const {domain, type} of taskTypes) {
                for await (const jobs of this._findInBatch(this.jobs, {
                    domain, type, status: {$in: ['SUSPENDED', 'PENDING', 'DELAYED', 'RUNNING']},
                })) {
                    const tasks = await this.tasks.find({
                        _id: {$in: jobs.map(j => j.task)},
                        subTasks: {$not: {$elemMatch: {domain, type, [p]: null}}}
                    }).toArray();
                    if (tasks.length > 0) {
                        updated.push(await this.updateJobs(
                            {_id: {$in: jobs.map(j => j._id)}, task: {$in: tasks.map(t => t._id)}},
                            {[p]: v}, {overwrite: false}
                        ));
                    }
                }
            }
        }
        return updated;
    }

    /**
     * @typedef {object} TaskConfig
     * @property {string|ObjectID} [id]
     * @property {string|ObjectID} [_id]
     *
     * @param {string|ObjectID|TaskConfig} task
     * @return {Promise<TaskConfig>}
     */
    async computeTask(task) {
        if (ObjectID.isValid(task)) {
            const taskId = new ObjectID(task);
            task = await this.tasks.findOne({_id: taskId});
        }
        if (!task) {
            throw new OperationError('task ', task, ' does not exist.');
        }
        if (task.local) task = task.local;
        const {_id, id, lockedBy, params = {}, context = {}, ...rest} = task;
        task = shrink({_id: _id || id, ...rest});
        const {domain, type, subTasks: subTasks1, ...rest1} = task;
        const taskTypeConfig = await this.computeType(domain, type);
        const {concurrency, subTasks: subTasks0, ...rest0} = taskTypeConfig;
        return {
            enabled: true, lastTime: null, nextTime: null,
            ...shrink({
                domain, type, ...rest0, ...rest1,
                subTasks: this._mergeSubTaskConfigs(subTasks0, subTasks1)
            }),
            params, context
        };
    }

    async insertTask(input) {
        debug('insertTask(%j)', input);
        const full = await this.computeTask(input);
        await this._validateTaskJobCommon(full);
        this._validateTask(full);
        const now = new Date();
        full.ctime = now;
        full.mtime = now;
        if (full.enabled) {
            full.nextTime = full.lastTime;
            this._computeTaskNextTime(full);
        } else {
            full.nextTime = null;
        }
        const {insertedId} = await this.tasks.insertOne({
            ...full, local: {...input, ...omit(full, [...TASK_INHERITED_PROPERTIES, 'ctime', 'mtime'])}
        });
        full._id = insertedId;
        return full;
    }

    async updateTasks(filter, update, {overwrite = true, updateMtime = true} = {}) {
        debug('updateTasks(%j, %j, %j)', filter, update, {overwrite});
        const {_id, ..._update} = update;
        if (isEmpty(_update)) return;
        const taskNextTimeFields = ['mode', 'interval', 'schedule', 'subTasks', 'validBefore', 'validAfter'];
        const updateTasksNextTime = taskNextTimeFields.some(p => p in _update) || _update.enabled === true;
        if (['mode', 'interval', 'schedule'].some(p => p in _update)) {
            this._validateTaskModes(_update);
        }
        if (_update.params) {
            this._validateTaskParams(_update);
        }
        const result = {overwrite, updates: []};
        let hasSimpleUpdated = false;
        for await (const tasks of this._findInBatch(this.tasks, filter)) {
            let complexUpdate = pickBy(_update, (v, p) =>
                isNil(v) && TASK_INHERITED_PROPERTIES.includes(p) ||
                taskNextTimeFields.includes(p) || 
                p === 'enabled' && v === true
            );
            const now = new Date();
            const res = {};
            result.updates.push(res);
            const jobUpdates = [];
            if (overwrite) {
                const simpleUpdate = omit(_update, Object.keys(complexUpdate));
                if (!isEmpty(simpleUpdate)) {
                    if (simpleUpdate.enabled === false) {
                        simpleUpdate.nextTime = null;
                        jobUpdates.push([
                            {task: {$in: tasks.map(t => t._id)}},
                            {status: 'CANCELED'}, {overwrite: true}
                        ]);
                    }
                    if (!hasSimpleUpdated) {
                        const full = updateMtime ? {mtime: now} : {};
                        result.simple = {
                            filter, update: simpleUpdate, full,
                            updated: await this._fullUpdateMany(this.tasks, filter, simpleUpdate, full)
                        };
                        hasSimpleUpdated = true;
                    }
                    const jobUpdate = pick(simpleUpdate, JOB_INHERITED_PROPERTIES);
                    if (!isEmpty(jobUpdate)) {
                        jobUpdates.push([
                            {$or: tasks.map(({_id, domain, type}) => ({task: _id, domain, type}))},
                            jobUpdate
                        ]);
                    }
                }
                if (!isEmpty(complexUpdate)) {
                    res.complex = [];
                    for (let task of tasks) {
                        const {_id, subTasks: oldSubTasks, ctime} = task;
                        task = await this.computeTask(shrink({...task.local, ctime, ...complexUpdate}));
                        const updateFields = Object.keys(complexUpdate);
                        if (updateTasksNextTime) {
                            task.nextTime = task.lastTime;
                            this._computeTaskNextTime(task);
                            updateFields.push('nextTime', 'lastTime');
                        }
                        const complexUpdateValues = pick(task, updateFields);
                        const full = {...complexUpdateValues};
                        if (updateMtime) full.mtime = now;
                        if (task.subTasks) full.subTasks = task.subTasks;
                        let newSubTasks = undefined;
                        if (complexUpdate.subTasks) {
                            newSubTasks = complexUpdate.subTasks
                                .map(s => shrink({...s}))
                                .filter(s => {
                                    const {domain, type, ...config} = s;
                                    return !isEmpty(config);
                                });
                            if (newSubTasks.length <= 0) {
                                newSubTasks = null;
                            }
                            complexUpdate = {...complexUpdate, subTasks: newSubTasks};
                        }
                        if (updateTasksNextTime) {
                            complexUpdate = {...complexUpdate, nextTime: task.nextTime, lastTime: task.lastTime};
                        }
                        const {domain, type} = task;
                        const result = {
                            filter: {_id}, update: complexUpdate, full,
                            updated: await this._fullUpdateOne(this.tasks, {_id}, complexUpdate, full)
                        };
                        res.complex.push(result);

                        const jobUpdate = pick({...complexUpdate, ...complexUpdateValues}, JOB_INHERITED_PROPERTIES);
                        if (!isEmpty(jobUpdate)) {
                            jobUpdates.push([{task: _id, domain, type}, jobUpdate]);
                        }
                        if (newSubTasks || oldSubTasks) {
                            let needRecomputeJobs = false;
                            const subTasksByKey = {};
                            for (const subTask of newSubTasks || []) {
                                const {domain, type, ...config} = subTask;
                                if (Object.values(config).some(isNil)) {
                                    needRecomputeJobs = true;
                                    break;
                                }
                                set(subTasksByKey, [domain, type], subTask);
                            }
                            if (!needRecomputeJobs) {
                                for (const subTask of oldSubTasks || []) {
                                    const {domain, type} = subTask;
                                    const newSubTask = get(subTasksByKey, [domain, type]);
                                    if (!newSubTask) {
                                        needRecomputeJobs = true;
                                        break;
                                    }
                                    for (const p of Object.keys(subTask)) {
                                        if (!(p in newSubTask)) {
                                            needRecomputeJobs = true;
                                            break;
                                        }
                                    }
                                    if (needRecomputeJobs) break;
                                }
                            }
                            for (const subTask of task.subTasks) {
                                const {domain, type} = subTask;
                                jobUpdates.push([
                                    {task: _id, domain, type},
                                    needRecomputeJobs ? {} : subTask,
                                    {force: needRecomputeJobs}
                                ]);
                            }
                        }
                    }
                }
            } else {
                if (Object.values(_update).some(isNil)) {
                    throw new OperationError('cannot updateTasks with null values when overwrite = false');
                }
                const {subTasks: updateSubTasks, ...updateRest} = _update;
                const [taskWithSubTasks, taskWithoutSubTasks] = partition(tasks, t => t.local.subTasks);
                if (updateSubTasks) {
                    res.updateTasksWithSubTasks = [];
                    for (let task of taskWithSubTasks) {
                        const {_id} = task;
                        const subTasks = this._mergeSubTaskConfigs(updateSubTasks, task.local.subTasks);
                        res.updateTasksWithSubTasks.push({
                            filter: {_id}, update: {}, full: {subTasks},
                            updated: await this._fullUpdateOne(this.tasks, {_id}, {}, {subTasks})
                        });
                        for (const subTask of subTasks || []) {
                            const {domain, type} = subTask;
                            jobUpdates.push([{task: _id, domain, type}, subTask]);
                        }
                    }
                    if (taskWithoutSubTasks.length > 0) {
                        const filter = {_id: {$in: taskWithoutSubTasks.map(t => t._id)}};
                        res.updateTasksWithoutSubTasks = {
                            filter, update: {}, full: {subTasks: updateSubTasks},
                            updated: await this._fullUpdateMany(
                                this.tasks, {_id: {$in: taskWithoutSubTasks.map(t => t._id)}},
                                {}, {subTasks: updateSubTasks}
                            )
                        };
                        for (const subTask of updateSubTasks) {
                            const {domain, type} = subTask;
                            jobUpdates.push([
                                {task: {$in: taskWithoutSubTasks.map(t => t._id)}, domain, type}, subTask
                            ]);
                        }
                    }
                }
                res.updateTasks = [];
                for (const [p, v] of Object.entries(updateRest)) {
                    const query = {...filter, [`local.${p}`]: null};
                    const update = {[p]: v};
                    const targets = await this.tasks.find(query).toArray();
                    if (targets.length > 0) {
                        const {matchedCount} = await this.tasks.updateMany(query, {$set: update});
                        res.updateTasks.push({filter: query, update, updated: matchedCount});
                        if (JOB_INHERITED_PROPERTIES.includes(p)) {
                            jobUpdates.push([
                                {$or: targets.map(({_id, domain, type}) => ({task: _id, domain, type}))},
                                update
                            ]);
                        }
                    }
                }
            }
            res.updateJobs = [];
            for (const [filter, update, options] of jobUpdates) {
                res.updateJobs.push(await this.updateJobs(
                    {...filter, status: {$in: ['SUSPENDED', 'PENDING', 'DELAYED', 'RUNNING']}},
                    update, {overwrite: false, ...options}
                ));
            }
        }
        return result;
    }

    /**
     * @param {string|ObjectID|object} job
     * @param {string|ObjectID} [job.id]
     * @param {string|ObjectID} [job._id]
     * @param {string} [job.domain]
     * @param {string} [job.type]
     * @param {string|ObjectID} [job.task]
     * @return {Promise<object>}
     */
    async computeJob(job) {
        if (ObjectID.isValid(job)) {
            const jobId = new ObjectID(job);
            job = await this.jobs.findOne({_id: jobId}, {projection: {local: 0}});
        }
        if (!job) {
            throw new OperationError('job ', job, ' does not exist.');
        }
        let taskTypeScheduling, taskScheduling = undefined, subTaskScheduling = undefined;
        let enabled = true, params = {}, context = {}, task = undefined;
        if (job.task) {
            task = await this.computeTask(job.task);
            if (!job.domain) job.domain = task.domain;
            if (!job.type) job.type = task.type;
            enabled = task.enabled;
            if (!job.createdBy) {
                params = task.params;
                context = task.context;
            }
            if (job.domain === task.domain && job.type === task.type) {
                taskScheduling = pick(task, SCHEDULING_PROPERTIES);
            }
            if (task.subTasks) {
                const {domain, type, ...rest} = task.subTasks.find(
                    s => isMatch(job.domain, s.domain) && isMatch(job.type, s.type)
                ) || {};
                subTaskScheduling = rest;
            }
        }
        const taskType = await this.computeType(job.domain, job.type);
        taskTypeScheduling = pick(taskType, SCHEDULING_PROPERTIES);
        const {_id, id, full, lockedBy, local, ...rest} = job;
        const now = new Date();
        const precomputed = shrink({
            _id: _id || id,
            trials: [], timeCreated: now, timeScheduled: now,
            ...taskTypeScheduling, ...taskScheduling, ...subTaskScheduling,
            ...(!enabled ? {status: 'CANCELED'} : undefined),
            ...shrink(rest),
        });
        const computed = {
            status: precomputed.suspend > 0 ? 'SUSPENDED' : 'PENDING',
            timePending: precomputed.suspend > 0 ? 
                new Date(precomputed.timeScheduled.getTime() + precomputed.suspend * 1000) : 
                precomputed.timeScheduled,
            params, context, ...precomputed,
        };
        this._renderJobParams(computed, task);
        return computed;
    }

    async insertJob(input) {
        const full = await this.computeJob(input);
        await this._validateTaskJobCommon(full);
        this._validateJob(full);
        const {domain, type, params, dedupLimit} = full;
        const dups = await this._findDuplicateJobs(full);
        if (full.dedupRecent) {
            if (dups.length >= dedupLimit) {
                const duplicate = dups[dups.length - 1];
                this.logger.info(
                    'Job scheduling is skipped as a duplicate of job ', duplicate._id,
                    ': ', {domain, type, params}
                );
                full.duplicateOf = duplicate._id;
                full.status = 'CANCELED';
            }
            const {insertedId} = await this.jobs.insertOne({
                ...full, local: {...input, ...omit(full, JOB_INHERITED_PROPERTIES)}
            });
            full._id = insertedId;
        } else {
            const {insertedId} = await this.jobs.insertOne({
                ...full, local: {...input, ...omit(full, JOB_INHERITED_PROPERTIES)}
            });
            full._id = insertedId;
            if (dups.length >= dedupLimit) {
                for (const dup of dups.slice(0, dups.length - dedupLimit + 1)) {
                    await this.updateJobs(
                        {_id: dup._id}, {duplicateOf: full._id},
                    );
                    if (!['SUSPENDED', 'PENDING', 'DELAYED', 'RUNNING'].includes(dup.status)) continue;
                    const updated = await this.updateJobs(
                        {_id: dup._id, status: {$in: ['SUSPENDED', 'PENDING', 'DELAYED', 'RUNNING']}},
                        {status: 'CANCELED'},
                    );
                    if (updated.simple.updated > 0) {
                        this.logger.info(
                            'Job ', dup._id, ' is canceled as a duplicate of the new full ',
                            full._id, ': ', {domain, type, params});
                    }
                }
            }
        }
        return full;
    }

    async updateJobs(filter, update, {overwrite = true, force = false} = {}) {
        debug('updateJobs(%j, %j, %j)', filter, update, {overwrite, force});
        const {_id, ..._update} = update;
        if (['task'].some(p => p in _update)) {
            this._validateJob(_update);
        }
        const complexUpdate = pickBy(_update, (v, p) =>
            isNil(v) && JOB_INHERITED_PROPERTIES.includes(p)
        );
        const result = {overwrite, force};
        if (overwrite) {
            const simpleUpdate = omit(_update, Object.keys(complexUpdate));
            if (!isEmpty(simpleUpdate)) {
                result.simple = {
                    filter, update: simpleUpdate,
                    updated: await this._fullUpdateMany(this.jobs, filter, simpleUpdate)
                };
            }
            if (!isEmpty(complexUpdate)) {
                result.complex = [];
                for await (let job of this.jobs.find(filter)) {
                    const {_id} = job;
                    job = await this.computeJob(shrink({...job.local, ...complexUpdate}));
                    const nonNull = pick(job, Object.keys(complexUpdate));
                    result.complex.push({
                        filter: {_id}, update: complexUpdate, full: nonNull,
                        updated: await this._fullUpdateOne(this.jobs, {_id}, complexUpdate, nonNull)
                    });
                }
            }
        } else if (isEmpty(_update)) {
            if (force) {
                result.updates = [];
                for await (let job of this.jobs.find(filter)) {
                    const {_id, local, ...old} = job;
                    job = await this.computeJob(local);
                    const update = diff(old, job);
                    result.updates.push({
                        filter: {_id}, update: {}, full: update,
                        updated: await this._fullUpdateOne(this.jobs, {_id}, {}, update)
                    });
                }
            }
        } else {
            if (Object.values(_update).some(isNil)) {
                throw new OperationError('cannot updateTasks with null values when overwrite = false');
            }
            result.updates = [];
            let updateParams = undefined;
            for (const [p, v] of Object.entries(_update)) {
                if (p === 'params') {
                    updateParams = v;
                } else {
                    const query = {...filter, [`local.${p}`]: null};
                    const update = {[p]: v};
                    const {matchedCount} = await this.jobs.updateMany(query, {$set: update});
                    result.updates.push({filter: query, update, updated: matchedCount});
                }
            }
            if (updateParams) {
                for await (let job of this.jobs.find(
                    {...filter, 'local.params': null}, {projection: {local: 0}}
                )) {
                    const {_id} = job;
                    job = await this.computeJob(shrink({...job, params: updateParams}));
                    const update = {params: job.params};
                    const {matchedCount} = await this.jobs.updateOne({_id}, {$set: update});
                    result.updates.push({filter: {_id}, update, updated: matchedCount});
                }
            }
        }
        return result;
    }

    async* _findInBatch(collection, filter) {
        let batch = [];
        for await (let doc of collection.find(filter)) {
            batch.push(doc);
            if (batch.length >= OP_BATCH_SIZE) {
                yield batch;
                batch = [];
            }
        }
        if (batch.length > 0) {
            yield batch;
        }
    }

    __makeUpdate(update = {}, full) {
        const fullUpdate = {...full};
        for (const [p, v] of Object.entries(update)) {
            if (['lockedBy', 'local'].includes(p)) {
                if (!(p in fullUpdate)) {
                    fullUpdate[p] = v;
                }
            } else {
                fullUpdate[`local.${p}`] = v;
                if (!(p in fullUpdate)) {
                    fullUpdate[p] = v;
                }
            }
        }
        const all = {};
        if (!isEmpty(fullUpdate)) all.$set = fullUpdate;
        return all;
    }

    async _fullUpdateOne(collection, filter = {}, update = {}, full) {
        const all = this.__makeUpdate(update, full);
        if (!isEmpty(all)) {
            const {value} = await collection.findOneAndUpdate(filter, all, {returnOriginal: false});
            debug('%s.findOneAndUpdate(%j, %j) -> %j', collection.collectionName, filter, all, value);
            return value;
        }
    }

    async _fullUpdateMany(collection, filter = {}, update = {}, full) {
        const all = this.__makeUpdate(update, full);
        if (!isEmpty(all)) {
            const {matchedCount} = await collection.updateMany(filter, all);
            debug('%s.updateMany(%j, %j) -> %s', collection.collectionName, filter, all, matchedCount);
            return matchedCount;
        } else {
            return 0;
        }
    }

    _mergeSubTaskConfigs(one, override) {
        const subTasks = {};
        for (const subTaskConfig of one || []) {
            const {domain, type} = subTaskConfig;
            set(subTasks, [domain, type], shrink({...subTaskConfig}));
        }
        for (const subTaskConfig of override || []) {
            const {domain, type} = subTaskConfig;
            const fullSubTaskConfig = {
                ...get(subTasks, [domain, type]),
                ...shrink({...subTaskConfig})
            };
            set(subTasks, [domain, type], fullSubTaskConfig);
        }
        const merged = Object.values(subTasks)
            .flatMap(s => Object.values(s))
            .filter(config => {
                const {domain, type, ...rest} = config;
                return !isEmpty(rest);
            });
        if (merged.length > 0) {
            return merged;
        }
    }

    /**
     * @param {object} input
     * @param {string} input.domain
     * @param {string} input.type
     * @param {object} input.params
     * @return {Promise<void>}
     * @private
     */
    async _validateTaskJobCommon(input) {
        for (const field of ['domain', 'type']) {
            if (!input[field]) {
                throw new OperationError('field "', field, '" must provided.');
            }
        }
        const {domain, type} = input;
        const taskType = await this.taskLoader.getTaskType(domain, type);
        if (!taskType) {
            throw new OperationError('Task type "', domain, '.', type, '" is not valid.');
        }
        let paramsInvalid = taskType.validate(input.params || {});
        if (paramsInvalid) {
            if (!Array.isArray(paramsInvalid)) paramsInvalid = [paramsInvalid];
            throw new OperationError(...paramsInvalid);
        }
    }

    _validateJob(job) {
    }

    _validateTask(task) {
        this._validateTaskModes(task);
        this._validateTaskParams(task);
    }
    
    _validateTaskModes(task) {
        if (!task.mode) {
            throw new OperationError('field "mode" must be provided for a task');
        }
        if (task.mode === 'ONCE' && (task.interval != null || task.schedule != null)) {
            throw new OperationError('field "interval" and "schedule" must be null when "mode" is "ONCE"');
        }
        if (task.mode === 'REPEATED') {
            if (task.schedule != null) {
                throw new OperationError('field "schedule" must be null when "mode" is "REPEATED"');
            }
            if (task.interval == null || task.interval < 0) {
                throw new OperationError('field "interval" should be an Int >= 0 when "mode" is "REPEATED"');
            }
        }
        if (task.mode === 'SCHEDULED') {
            if (task.interval != null) {
                throw new OperationError('field "interval" must be null when "mode" is "SCHEDULED"');
            }
            if (!task.schedule) {
                throw new OperationError('field "schedule" should not be empty when "mode" is "SCHEDULED"');
            }
            try {
                new CronTime(task.schedule);
            } catch (e) {
                throw new OperationError('field "schedule" is not a valid cron expression: ', e);
            }
        }
    }
    
    _validateTaskParams(task) {
        const now = new Date();
        this._renderJobParams({params: task.params, timeScheduled: now, timeCreated: now}, task);
    }
    
    _computeTaskNextTime(task) {
        const {mode, interval, schedule, timezone, validBefore, validAfter, ctime} = task;
        const now = new Date();
        let nextTime = task.nextTime, lastTime, computeNext = true;
        if (task.nextTime == null && task.lastTime == null) {
            nextTime = ctime;
            if (mode === 'ONCE' || mode === 'REPEATED') {
                computeNext = false;
            }
        }
        const validWindowStart = new Date(Math.max(now.getTime(), validAfter || 0));
        if (computeNext) {
            while (nextTime != null && nextTime <= validWindowStart) {
                lastTime = nextTime;
                if (mode === 'ONCE') {
                    nextTime = null;
                } else if (mode === 'REPEATED') {
                    nextTime = new Date(lastTime.getTime() + interval * 1000);
                } else if (mode === 'SCHEDULED') {

                    // the following is copied from agenda/job/compute-next-run-at.js

                    const dateForTimezone = date => {
                        date = moment(date);
                        if (timezone) {
                            date.tz(timezone);
                        }
                        return date;
                    };
                    const lastTime_ = dateForTimezone(lastTime);
                    const cronTime = new CronTime(schedule);
                    let nextDate = cronTime._getNextDateFrom(lastTime_);
                    if (nextDate.valueOf() === lastTime_.valueOf()) {
                        // Handle cronTime giving back the same date for the next run time
                        nextDate = cronTime._getNextDateFrom(dateForTimezone(new Date(lastTime_.valueOf() + 1001)));
                    }
                    nextTime = nextDate.toDate();

                    // end of copy from agenda/job/compute-next-run-at.js
                    
                }
                if (nextTime > validBefore) {
                    nextTime = null;
                }
            }
        }
        task.lastTime = task.nextTime;
        task.nextTime = nextTime;
    }
    
    _renderJobParams(job, task) {
        const now = new Date();
        const today = new Date(now);
        today.setHours(0, 0, 0, 0);
        const env = {
            task: {
                id: job.task ? job.task.toString() : '',
                mode: (task || {}).mode || '',
                interval: (task || {}).interval || 0,
                schedule: (task || {}).schedule || '',
                timezone: (task || {}).timezone || '',
            },
            timeScheduled: job.timeScheduled,
            timeCreated: job.timeCreated,
            now: now,
            today: today,
        };
        function render(v) {
            if (isPlainObject(v)) {
                return mapValues(v, render);
            } else if (Array.isArray(v)) {
                return v.map(render);
            } else if (typeof v === 'string') {
                return format(v, env, {throwErrorForMissingProperty: true, keepOriginalValueForSingleExpr: true});
            } else {
                return v;
            }
        }
        try {
            job.params = render(job.params);
        } catch (e) {
            if (e instanceof ReferenceError) {
                throw new OperationError('job params rendering failed: ', e);
            }
        }
    }

    async _findDuplicateJobs({_id, domain, type, params, dedupWithin, dedupLimit, timeCreated = new Date()}) {
        if (dedupWithin < 0) {
            return [];
        }
        const dupQuery = {
            ...(_id && {_id: {$ne: _id}}),
            domain, type,
        };
        const taskType = await this.taskLoader.getTaskType(domain, type);
        if (!taskType) {
            throw new OperationError('Task type "', domain, '.', type, '" is not valid.');
        }
        let dupParams = taskType.dedup && taskType.dedup({...params}) || params;
        if (!Array.isArray(dupParams)) dupParams = [dupParams];
        const dupParamsQueries = [];
        for (const dupParam of dupParams) {
            const dupParamsQuery = {};
            for (const [p, v] of Object.entries(dupParam)) {
                dupParamsQuery[`params.${p}`] = v;
            }
            if (!isEmpty(dupParamsQuery)) {
                dupParamsQueries.push(dupParamsQuery);
            }
        }
        if (dupParamsQueries.length > 1) {
            dupQuery.$or = dupParamsQueries;
        } else if (dupParamsQueries.length === 1) {
            Object.assign(dupQuery, dupParamsQueries[0]);
        }
        const duplicates = [];
        for (const statusQuery of [
            {status: 'SUCCESS', timeStopped: {$gte: new Date(timeCreated - dedupWithin * 1000)}},
            {status: 'RUNNING'},
            {status: 'DELAYED'},
            {status: 'PENDING'},
            {status: 'SUSPENDED'},
        ]) {
            const cursor = this.jobs.find(
                {...dupQuery, ...statusQuery},
                {projection: {params: 1, status: 1, timeCreated: 1}, sort: [['timeCreated', 1]]},
            );
            for await (const dup of cursor) {
                if (!taskType.dedup && !isEqual(params, dup.params || {})) continue;
                duplicates.push(dup);
                if (duplicates.length >= dedupLimit) break;
            }
            if (duplicates.length >= dedupLimit) {
                await cursor.close();
                break;
            }
        }
        return duplicates;
    }

}

module.exports = Operations;
