const debug = require('debug')('catalyst:operations');
const {set, get, isEmpty, isPlainObject, pick, pickBy, omit, isNil, mapValues, partition} = require('lodash');
const {ObjectID} = require('mongodb');

const {deepEqual, shrink, diff} = require('@raychee/utils');
const {
    DEFAULT_TASK_DOMAIN_CONFIG, SCHEDULING_PROPERTIES, COLLECTION_NAMES, OP_BATCH_SIZE,
    TASK_TYPE_INHERITED_PROPERTIES, TASK_INHERITED_PROPERTIES, JOB_INHERITED_PROPERTIES,
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
        await this.schedulers.updateOne({_id: 0}, {$set: {_id: 0}}, {upsert: true});
        for (const taskDomain of this.taskLoader.getAllTaskDomains()) {
            await this.ensureDomain(taskDomain.name);
        }
        for (const taskType of this.taskLoader.getAllTaskTypes()) {
            await this.ensureDomain(taskType.domain.name, taskType.name);
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
        const {domain, ..._update} = update;
        if (isEmpty(_update)) return 0;
        const nonNull = mapValues(
            pickBy(_update, (v, p) => p !== 'lockedBy' && isNil(v)),
            (v, p) => DEFAULT_TASK_DOMAIN_CONFIG[p]
        );
        const full = {...nonNull, mtime: new Date()};
        const modified = await this._fullUpdateMany(this.domains, filter, _update, full);
        const domains = await this.domains.find(filter).toArray();
        const {maxConcurrency, ...taskTypeUpdate} = {..._update, ...nonNull};
        await this.updateTypes(
            {domain: {$in: domains.map(d => d.domain)}}, taskTypeUpdate, {overwrite: false}
        );
        return modified;
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
        const {_id, lockedWrite, lockedBy, ...rest} = type;
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
        const {domain, type, ..._update} = update;
        if (isEmpty(_update)) return;
        const taskTypes = await this.types.find(filter).toArray();
        const complexUpdate = pickBy(_update, (v, p) =>
            isNil(v) && TASK_TYPE_INHERITED_PROPERTIES.includes(p)
        );
        const now = new Date();
        let modified = 0;
        if (overwrite) {
            const simpleUpdate = omit(_update, Object.keys(complexUpdate));
            if (!isEmpty(simpleUpdate)) {
                modified = await this._fullUpdateMany(this.types, filter, simpleUpdate, {mtime: now});
                const taskUpdate = pick(simpleUpdate, TASK_INHERITED_PROPERTIES);
                if (!isEmpty(taskUpdate)) {
                    await this.updateTasks(
                        {$or: taskTypes.map(({domain, type}) => ({domain, type}))},
                        taskUpdate, {overwrite: false}
                    );
                }
            }
            if (!isEmpty(complexUpdate)) {
                for (let taskType of taskTypes) {
                    const {domain, type} = taskType;
                    taskType = await this.computeType(shrink({domain, type, ...taskType.local, ..._update}));
                    const nonNull = pick(taskType, Object.keys(complexUpdate));
                    const full = {...nonNull, mtime: now};
                    const updated = await this._fullUpdateOne(this.types, {domain, type}, complexUpdate, full);
                    if (updated) modified++;
                    const taskUpdate = pick(nonNull, TASK_INHERITED_PROPERTIES);
                    if (!isEmpty(taskUpdate)) {
                        await this.updateTasks({domain, type}, taskUpdate, {overwrite: false});
                    }
                }
            }
        } else {
            if (Object.values(_update).some(isNil)) {
                throw new OperationError('cannot updateTaskTypeConfigs with null values when overwrite = false');
            }
            const taskUpdates = [];
            for (const [p, v] of Object.entries(_update)) {
                const targets = await this.types.find({...filter, [`local.${p}`]: null}).toArray();
                const {matchedCount} = await this.types.updateMany({
                    ...filter,
                    [`local.${p}`]: null
                }, {$set: {[p]: v}});
                modified += matchedCount;
                if (p === 'concurrency') continue;
                taskUpdates.push([{$or: targets.map(({domain, type}) => ({domain, type}))}, {[p]: v}]);
            }
            for (const [filter, update] of taskUpdates) {
                await this.updateTasks(filter, update, {overwrite: false});
            }
        }
        return modified;
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
        const {_id, id, lockedBy, ...rest} = task;
        task = shrink({_id: _id || id, ...rest});
        const {domain, type, subTasks: subTasks1, ...rest1} = task;
        const taskTypeConfig = await this.computeType(domain, type);
        const {concurrency, subTasks: subTasks0, ...rest0} = taskTypeConfig;
        return shrink({
            domain, type,
            enabled: true, params: {}, context: {}, nextTime: new Date(),
            ...rest0, ...rest1,
            subTasks: this._mergeSubTaskConfigs(subTasks0, subTasks1)
        });
    }

    async insertTask(input) {
        const full = await this.computeTask(input);
        await this._validateTask(full);
        const now = new Date();
        full.ctime = now;
        full.mtime = now;
        const {insertedId} = await this.tasks.insertOne({
            ...full, local: {...input, ...omit(full, [...TASK_INHERITED_PROPERTIES, 'ctime', 'mtime'])}
        });
        full._id = insertedId;
        return full;
    }

    async updateTasks(filter, update, {overwrite = true} = {}) {
        const {_id, ..._update} = update;
        if (isEmpty(_update)) return;
        let modified = 0;
        for await (const tasks of this._findInBatch(this.tasks, filter)) {
            let complexUpdate = pickBy(_update, (v, p) =>
                isNil(v) && TASK_INHERITED_PROPERTIES.includes(p) || p === 'subTasks'
            );
            const now = new Date();
            const jobUpdates = [];
            if (overwrite) {
                const simpleUpdate = omit(_update, Object.keys(complexUpdate));
                if (!isEmpty(simpleUpdate)) {
                    modified += await this._fullUpdateMany(this.tasks, filter, simpleUpdate, {mtime: now});
                    if (simpleUpdate.enabled !== undefined) {
                        if (!simpleUpdate.enabled) {
                            jobUpdates.push([
                                {task: {$in: tasks.map(t => t._id)}},
                                {status: 'CANCELED'}, {overwrite: true}
                            ]);
                        }
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
                    for (let task of tasks) {
                        const {_id, subTasks: oldSubTasks} = task;
                        task = await this.computeTask(shrink({...task.local, ...complexUpdate}));
                        const nonNull = pick(task, Object.keys(complexUpdate));
                        const full = {...nonNull, mtime: now};
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
                        const {domain, type} = task;
                        const updated = await this._fullUpdateOne(this.tasks, {_id}, complexUpdate, full);
                        if (updated) modified++;

                        const jobUpdate = pick({...complexUpdate, ...nonNull}, JOB_INHERITED_PROPERTIES);
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
                    for (let task of taskWithSubTasks) {
                        const {_id} = task;
                        const subTasks = this._mergeSubTaskConfigs(updateSubTasks, task.local.subTasks);
                        const updated = await this._fullUpdateOne(this.tasks, {_id}, {}, {subTasks});
                        if (updated) modified++;
                        for (const subTask of subTasks || []) {
                            const {domain, type} = subTask;
                            jobUpdates.push([{task: _id, domain, type}, subTask]);
                        }
                    }
                    if (taskWithoutSubTasks.length > 0) {
                        modified += await this._fullUpdateMany(
                            this.tasks, {_id: {$in: taskWithoutSubTasks.map(t => t._id)}},
                            {}, {subTasks: updateSubTasks}
                        );
                        for (const subTask of updateSubTasks) {
                            const {domain, type} = subTask;
                            jobUpdates.push([
                                {task: {$in: taskWithoutSubTasks.map(t => t._id)}, domain, type}, subTask
                            ]);
                        }
                    }
                }
                for (const [p, v] of Object.entries(updateRest)) {
                    const targets = await this.tasks.find({...filter, [`local.${p}`]: null}).toArray();
                    modified += await this.tasks.updateMany({...filter, [`local.${p}`]: null}, {$set: {[p]: v}});
                    if (SCHEDULING_PROPERTIES.includes(p)) {
                        jobUpdates.push([
                            {$or: targets.map(({_id, domain, type}) => ({task: _id, domain, type}))},
                            {[p]: v}
                        ]);
                    }
                }
            }
            for (const [filter, update, options] of jobUpdates) {
                await this.updateJobs(
                    {...filter, status: {$in: ['PENDING', 'DELAYED', 'RUNNING']}},
                    update, {overwrite: false, ...options}
                );
            }
        }
        return modified;
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
            job = await this.jobs.findOne({_id: jobId});
        }
        if (!job) {
            throw new OperationError('job ', job, ' does not exist.');
        }
        let taskTypeScheduling, taskScheduling = undefined, subTaskScheduling = undefined;
        let enabled = true, params = undefined, context = undefined;
        if (job.task) {
            const task = await this.computeTask(job.task);
            if (!job.domain) job.domain = task.domain;
            if (!job.type) job.type = task.type;
            enabled = task.enabled;
            params = task.params;
            context = task.context;
            if (job.domain === task.domain && job.type === task.type) {
                taskScheduling = pick(task, SCHEDULING_PROPERTIES);
            }
            if (task.subTasks) {
                const {domain, type, ...rest} = task.subTasks.find(
                    s => s.domain === job.domain && s.type === job.type
                ) || {};
                subTaskScheduling = rest;
            }
        }
        const taskType = await this.computeType(job.domain, job.type);
        taskTypeScheduling = pick(taskType, SCHEDULING_PROPERTIES);
        const {_id, id, full, lockedWrite, lockedBy, ...rest} = job;
        return shrink({
            _id: _id || id,
            params: params || {}, context: context || {},
            status: 'PENDING', trials: [], timeCreated: new Date(),
            ...taskTypeScheduling, ...taskScheduling, ...subTaskScheduling,
            ...shrink(rest),
            ...(!enabled ? {status: 'CANCELED'} : undefined)
        });
    }

    async insertJob(input) {
        const full = await this.computeJob(input);
        await this._validateJob(full);
        const insert = {
            ...full, local: {...input, ...omit(full, JOB_INHERITED_PROPERTIES)}
        };
        const {domain, type, params} = full;
        for await (const dup of this._findDuplicateJobs(full)) {
            if (full.dedupRecent) {
                this.logger.info(
                    'Job scheduling is skipped as a duplicate of job ', dup._id,
                    ': ', {domain, type, params}
                );
                return;
            } else {
                if (!full._id) {
                    const {insertedId} = await this.jobs.insertOne(insert);
                    full._id = insertedId;
                }
                const modified = await this.jobs.updateJobs(
                    {_id: dup._id, status: {$in: ['PENDING', 'DELAYED', 'RUNNING']}},
                    {status: 'CANCELED'}
                );
                if (modified > 0) {
                    this.logger.info(
                        'Job ', dup._id, ' is canceled as a duplicate of the new full ',
                        full._id, ': ', {domain, type, params});
                }
            }
        }
        if (!full._id) {
            const {insertedId} = await this.jobs.insertOne(insert);
            full._id = insertedId;
        }
        return full;
    }

    async updateJobs(filter, update, {overwrite = true, force = false} = {}) {
        const {_id, ..._update} = update;
        const complexUpdate = pickBy(_update, (v, p) =>
            isNil(v) && JOB_INHERITED_PROPERTIES.includes(p)
        );
        let modified = 0;
        if (overwrite) {
            const simpleUpdate = omit(_update, Object.keys(complexUpdate));
            if (!isEmpty(simpleUpdate)) {
                const m = await this._fullUpdateMany(this.jobs, filter, simpleUpdate);
                modified += m;
            }
            if (!isEmpty(complexUpdate)) {
                for await (let job of this.jobs.find(filter)) {
                    const {_id} = job;
                    job = await this.computeJob(shrink({...job.local, ...complexUpdate}));
                    const nonNull = pick(job, Object.keys(complexUpdate));
                    const result = await this._fullUpdateOne(this.jobs, {_id}, complexUpdate, nonNull);
                    if (result) modified++;
                }
            }
        } else if (isEmpty(_update)) {
            if (force) {
                for await (let job of this.jobs.find(filter)) {
                    const {_id, local, ...old} = job;
                    job = await this.computeJob(local);
                    const update = diff(old, job);
                    const result = await this._fullUpdateOne(this.jobs, {_id}, {}, update);
                    if (result) modified++;
                }
            }
        } else {
            if (Object.values(_update).some(isNil)) {
                throw new OperationError('cannot updateTasks with null values when overwrite = false');
            }
            for (const [p, v] of Object.entries(_update)) {
                const {matchedCount} = await this.jobs.updateMany(
                    {...filter, [`local.${p}`]: null}, {$set: {[p]: v}}
                );
                modified += matchedCount;
            }
        }
        return modified;
    }

    async *_findInBatch(collection, filter) {
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
            return value;
        }
    }

    async _fullUpdateMany(collection, filter = {}, update = {}, full) {
        const all = this.__makeUpdate(update, full);
        if (!isEmpty(all)) {
            const {matchedCount} = await collection.updateMany(filter, all);
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
    async _validateCommon(input) {
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

    async _validateJob(job) {
        await this._validateCommon(job);
        if (!job.task) {
            throw new OperationError('job must come from a task.');
        }
    }

    async _validateTask(task) {
        await this._validateCommon(task);
        if (!task.mode) {
            throw new OperationError('field "mode" must be provided for a task.');
        }
        if (task.mode === 'REPEATED' && (task.interval === undefined || task.interval < 0)) {
            throw new OperationError('field "interval" should be an Int >= 0 when "mode" is "REPEATED"');
        }
        if (task.mode === 'SCHEDULED' && task.schedule === undefined) {
            throw new OperationError('field "schedule" should a valid cron expression when "mode" is "SCHEDULED"');
        }
    }

    async* _findDuplicateJobs({domain, type, params, dedupWithin, timeCreated = new Date()}) {
        if (dedupWithin <= 0) {
            return;
        }
        const dupQuery = {
            domain, type, timeCreated: {$gte: new Date(timeCreated - dedupWithin * 1000)},
            status: {$in: ['PENDING', 'RUNNING', 'DELAYED', 'SUCCESS']}
        };
        const dupQueryParams = [];
        const taskType = await this.taskLoader.getTaskType(domain, type);
        if (!taskType) {
            throw new OperationError('Task type "', domain, '.', type, '" is not valid.');
        }
        let dedup = taskType.dedup ? taskType.dedup({...params}) : params;
        if (!Array.isArray(dedup)) dedup = [dedup];
        for (const d of dedup) {
            const dp = {};
            for (const [p, v] of Object.entries(d)) {
                dp[`params.${p}`] = v;
            }
            dupQueryParams.push(dp);
        }
        if (dupQueryParams.length > 1) {
            dupQuery.$or = dupQueryParams;
        } else if (dupQueryParams.length === 1) {
            Object.assign(dupQuery, dupQueryParams[0]);
        }
        if (taskType.dedup) {
            yield* this.jobs.find(dupQuery);
        } else {
            for await (const dup of this.jobs.find(dupQuery)) {
                if (deepEqual(dedup, dup.dedup)) {
                    yield dup;
                }
            }
        }
    }

}

module.exports = Operations;