const {set, get, isEmpty} = require('lodash');
const DataLoader = require('dataloader');
const {UserInputError} = require('apollo-server-koa');

const {DEFAULT_TASK_DOMAIN_CONFIG, TYPES} = require('./config');
const {safeJSON, deepEqual, shrink, diff, stringify, sleep} = require('./utils');


const SCHEDULING_PROPERTIES = [
    'delay', 'delayRandomize', 'retry', 'retryDelayFactor', 'priority', 'dedupWithin', 'dedupRecent'
];


class Operations {

    constructor(mongodb, taskLoader, jobContextCache) {
        this.mongodb = mongodb;
        this.taskLoader = taskLoader;
        this.jobContextCache = jobContextCache;
    }

    // domain is either a domain config object, or an array like ["domainName"].
    // optional "config" is the one from internal db
    async getTaskDomainConfig(domain, dataloaders) {
        if (isQueryBy(domain) === "key") {
            let domainConfig;
            while (true) {
                domainConfig = await this.query('TaskDomainConfig', domain, undefined, dataloaders);
                if (domainConfig && domainConfig._locked) {
                    await sleep(100);
                } else {
                    break;
                }
            }
            domain = domainConfig ? domainConfig : makeQuery('TaskDomainConfig', domain);
        }
        const taskDomain = await this.taskLoader.getDomain(domain.domain);
        if (!taskDomain) {
            throw new UserInputError(`domain ${domain.domain} is not valid.`);
        }
        domain = {...DEFAULT_TASK_DOMAIN_CONFIG, ...taskDomain.config, ...domain};
        const {_full, _locked, ...ret} = domain;
        return ret;
    }

    // this function ensures agenda to execute correctly on config change;
    // "domain" is either a domain config object (the old config from internal db), or an array like ["domainName"].
    // "update" is the diff of this update
    async updateTaskDomainConfig(domain, updates, dataloaders) {
        if (!domain) {
            const domainKey = getKeyValues('TaskDomainConfig', updates);
            if (domainKey.every(v => v)) {
                domain = domainKey;
            }
        }
        if (!domain) {
            throw new UserInputError('domain name must be specified');
        }
        if (updates) {
            let domainKey = getKeyValues('TaskDomainConfig', updates);
            if (!domainKey.every(v => v)) {
                domainKey = isQueryBy(domain) === 'key' ? domain : getKeyValues('TaskDomainConfig', domain);
                setKeyValues('TaskDomainConfig', updates, domainKey);
            }
            await this.upsert('TaskDomainConfig', makeQuery('TaskDomainConfig', domainKey), true);
            domain = await this.upsert(
                'TaskDomainConfig', {...updates, _locked: true}, false, dataloaders,
                {_locked: {$ne: true}}, true
            );
        }
        const fullConfig = await this.getTaskDomainConfig(domain);
        if (updates) {
            await this.upsert('TaskDomainConfig', {
                ...makeQuery('TaskDomainConfig', getKeyValues('TaskDomainConfig', updates)),
                _locked: false, _full: fullConfig
            }, true, dataloaders);
        }
        if (fullConfig.maxConcurrency !== undefined) {
            const concurrency = fullConfig.maxConcurrency > 0 ? fullConfig.maxConcurrency : Number.MAX_SAFE_INTEGER;
            const agenda = await this.taskLoader.getAgenda(fullConfig.domain);
            // console.log(`agenda.maxConcurrency(${concurrency}).lockLimit(${concurrency});`);
            agenda.maxConcurrency(concurrency).lockLimit(concurrency);
        }
        await this.updateTaskTypeConfigs(fullConfig, dataloaders);
        return fullConfig;
    }

    // get full config of a task type.
    // "taskType" is either a TaskType object, or a task type name like ["domain", "type"].
    // optional "config" is the one from internal db.
    async getTaskTypeConfig(taskType, config, taskDomainConfig, dataloaders) {
        taskType = await this._ensureTaskType(taskType, config);
        if (!config) {
            while (true) {
                config = await this.query('TaskTypeConfig', taskType.key, undefined, dataloaders);
                if (config && config._locked) {
                    await sleep(100);
                } else {
                    break;
                }
            }
        }
        if (!taskDomainConfig) {
            taskDomainConfig = await this.getTaskDomainConfig([taskType.domain.name], dataloaders);
        }
        const {subTasks, ...otherConfig} = config || {};
        config = {
            ...getTaskTypeConfigFromTaskDomainConfig(DEFAULT_TASK_DOMAIN_CONFIG),
            ...getTaskTypeConfigFromTaskDomainConfig(taskType.domain.config),
            ...taskDomainConfig,
            ...taskType.config,
            ...otherConfig,
        };
        config.subTasks = this.mergeSubTaskConfigs(config.subTasks, subTasks);
        const {_full, _locked, ...ret} = config;
        return ret;
    }

    // this function ensures agenda to execute correctly on config change;
    // "taskType" is either a TaskType object, or a task type name like ["domain", "type"].
    // "config" is the one from internal db, "update" is the diff of this update
    async updateTaskTypeConfig(taskType, config, updates, dataloaders) {
        taskType = await this._ensureTaskType(taskType, config, updates);
        const taskTypeKey = taskType.key;
        const taskTypeName = taskTypeKey.join('.');
        if (updates) {
            let updateKey = getKeyValues('TaskTypeConfig', updates);
            if (!updateKey.every(v => v)) {
                setKeyValues('TaskTypeConfig', updates, taskTypeKey);
            }
            updateKey = getKeyValues('TaskTypeConfig', updates);
            const updateTaskTypeName = updateKey.join('.');
            if (!deepEqual(taskTypeKey, updateKey)) {
                throw new UserInputError(`Task type "${taskTypeName}" must be identical to the updates "${updateTaskTypeName}".`);
            }
            await this.upsert('TaskTypeConfig', makeQuery('TaskTypeConfig', taskTypeKey), true);
            config = await this.upsert(
                'TaskTypeConfig', {...updates, _locked: true}, false, dataloaders,
                {_locked: {$ne: true}}, true
            );
        }
        const fullConfig = await this.getTaskTypeConfig(taskType, config, undefined, dataloaders);
        if (updates) {
            await this.upsert('TaskTypeConfig', {
                ...makeQuery('TaskTypeConfig', getKeyValues('TaskTypeConfig', updates)),
                _locked: false, _full: fullConfig
            }, true, dataloaders);
        }
        const agendaConfig = {};
        if (fullConfig.concurrency !== undefined) {
            const concurrency = fullConfig.concurrency > 0 ? fullConfig.concurrency : Number.MAX_SAFE_INTEGER;
            agendaConfig.concurrency = concurrency;
            agendaConfig.lockLimit = concurrency;
        }
        if (fullConfig.timeout !== undefined) {
            agendaConfig.lockLifetime = fullConfig.timeout > 0 ? fullConfig.timeout * 1000 : Number.MAX_SAFE_INTEGER;
        }
        if (fullConfig.priority !== undefined) {
            agendaConfig.priority = fullConfig.priority;
        }
        const agenda = await this.taskLoader.getAgenda(taskType.domain.name);
        // console.log(`agenda.define(${taskTypeName}, ${JSON.stringify(agendaConfig)}, taskType.toAgendaJobFn());`)
        agenda.define(taskTypeName, agendaConfig, taskType.toAgendaJobFn());
        await this.updateTasks(fullConfig, dataloaders);
        return fullConfig;
    }

    // get full config of a task.
    // "task" is either a task object from internal db, or an array like [taskId].
    // optional taskTypeConfig is a FULL task type config object
    // this function will supplement all the fields.
    async getTask(task, taskTypeConfig, dataloaders) {
        if (isQueryBy(task) === 'key') {
            while (true) {
                task = await this.query('Task', task, undefined, dataloaders);
                if (task && task._locked) {
                    await sleep(100);
                } else {
                    break;
                }
            }
            if (!task) {
                return task;
            }
        } else {
            task = {...task};
        }
        task.params = task.params || {};
        task.context = task.context || {};

        if (!taskTypeConfig) {
            taskTypeConfig = await this.getTaskTypeConfig(getKeyValues('TaskTypeConfig', task), undefined, undefined, dataloaders);
        }
        await this.populateTaskSchedulingProperties(task, taskTypeConfig, dataloaders);
        task.subTasks = this.mergeSubTaskConfigs(taskTypeConfig.subTasks, task.subTasks);

        const {_full, _locked, ...ret} = task;
        return ret;
    }

    mergeSubTaskConfigs(one, override) {
        const subTasks = {};
        for (const subTaskConfig of one || []) {
            set(subTasks, getKeyValues('TaskTypeConfig', subTaskConfig), subTaskConfig);
        }
        for (const subTaskConfig of override || []) {
            const taskTypeKey = getKeyValues('TaskTypeConfig', subTaskConfig);
            const fullSubTaskConfig = {...get(subTasks, taskTypeKey), ...subTaskConfig};
            set(subTasks, taskTypeKey, fullSubTaskConfig);
        }
        return Object.values(subTasks).flatMap(s => Object.values(s));
    }

    async populateTaskSchedulingProperties(task, taskTypeConfig, dataloaders, force) {
        const diff = {};
        for (const property of SCHEDULING_PROPERTIES) {
            if (task[property] === undefined || force) {
                if (!taskTypeConfig) {
                    taskTypeConfig = await this.getTaskTypeConfig(getKeyValues('TaskTypeConfig', task), undefined, undefined, dataloaders);
                }
                if (task[property] !== taskTypeConfig[property]) {
                    diff[property] = taskTypeConfig[property];
                    task[property] = taskTypeConfig[property];
                }
            }
        }
        return diff;
    }

    async populateJobSchedulingProperties(job, task, dataloaders, force) {
        if (!task) {
            task = await this.getTask([job.task], undefined, dataloaders);
        }
        const jobTypeKey = getKeyValues('TaskTypeConfig', job);
        const schedulingConfig = (task.subTasks || []).find(t =>
            deepEqual(jobTypeKey, getKeyValues('TaskTypeConfig', t))
        );
        return await this.populateTaskSchedulingProperties(job, schedulingConfig, dataloaders, force);
    }

    // schedule a task. "task" is a Task config object.
    // if task id is provided, this is considered an update. if the task does not exist, an error will be raised.
    // if task id is missing, this is considered an insert.
    // this function DO NOT check if task is valid, so use with care.
    // this function DO insert config into internal db
    async scheduleTask(task, dataloaders) {
        task = {...task};
        if (task.id) {
            // TODO: need lock for race condition
            const updates = task;
            const oldFullTask = await this.getTask([updates.id], undefined, dataloaders);
            if (!oldFullTask) {
                throw new UserInputError(`task "${updates.id}" does not exist.`);
            }
            let fullTask = {...oldFullTask, ...updates};
            await this._validateTask(fullTask);
            // if (fullTask.mode === 'ONCE') {
            //     const jobs = await this.query('Job', {task: task.id}, undefined, dataloaders);
            //     if (jobs.length > 0) {
            //         throw new UserInputError(`task of mode "ONCE" is already executed, so cannot be updated any more`);
            //     }
            // }
            task = await this.upsert(
                'Task', {...updates, _locked: true}, false, dataloaders,
                {_locked: {$ne: true}}, true
            );
            fullTask = await this.getTask(task, undefined, dataloaders);
            await this.upsert(
                'Task', {id: task.id, _locked: false, _full: fullTask}, true, dataloaders
            );
            const [agendaJob] = await this.taskLoader.taskAgenda.jobs({'data.taskId': task.id});
            let scheduleTime = undefined;
            if (fullTask.validAfter > Date.now()) {
                scheduleTime = fullTask.validAfter;
            }
            if (agendaJob) {
                await this.saveAgendaJobForTask(agendaJob, fullTask, scheduleTime);
            } else {
                console.error(`Warning - task ${task.id} does not have a corresponding agenda job instance, task scheduling is skipped.`);
            }
            return fullTask;
        } else {
            await this._validateTask(task);
            if (task.enabled === undefined) {
                task.enabled = true;
            }
            task = await this.upsert('Task', {...task, _locked: true}, false, dataloaders);
            let fullTask = await this.getTask(task, undefined, dataloaders);
            await this.upsert(
                'Task', {id: task.id, _locked: false, _full: fullTask}, true, dataloaders
            );
            const agendaJob = this.taskLoader.taskAgenda.create('_task');
            await this.saveAgendaJobForTask(agendaJob, fullTask, task.mode === 'ONCE' ? new Date() : undefined);
            return fullTask;
        }
    }

    updateAgendaJobForTask(agendaJob, task) {
        let {id, mode, interval, validBefore, schedule, enabled} = task;
        agendaJob.attrs.data = {taskId: id};
        if (validBefore && Date.now() >= new Date(validBefore)) {
            enabled = false;
        }
        if (mode === 'REPEATED') {
            agendaJob.repeatEvery(interval * 1000);
        } else if (mode === 'SCHEDULED') {
            agendaJob.repeatEvery(schedule);
        }
        if (enabled) {
            agendaJob.enable();
        } else {
            agendaJob.disable();
        }
    }

    async saveAgendaJobForTask(agendaJob, task, scheduleTime) {
        this.updateAgendaJobForTask(agendaJob, task, scheduleTime);
        if (scheduleTime) {
            agendaJob.schedule(scheduleTime);
        }
        await agendaJob.save();
    }

    async findDuplicate({domain, type, params, dedupWithin, timeCreated = new Date()}) {
        if (dedupWithin <= 0) {
            return;
        }
        const dupQuery = {
            domain, type, timeCreated: {$gte: new Date(timeCreated - dedupWithin * 1000)},
            status: {$in: ['PENDING', 'RUNNING', 'DELAYED', 'SUCCESS']}
        };
        const dupQueryParams = [];
        const task = await this.taskLoader.get([domain, type]);
        let dedup = task.dedup ? task.dedup({...params}) : params;
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
        if (task.dedup) {
            const [dup] = await this.query('Job', dupQuery, {limit: 1});
            return dup;
        } else {
            const possibleDups = await this.query('Job', dupQuery);
            for (const dup of possibleDups) {
                if (deepEqual(dedup, dup.dedup)) {
                    return dup;
                }
            }
        }
    }

    // schedule a new job. called from outside agenda.
    async scheduleJob(job) {
        if (job.id) {
            job = await this.upsert('Job', job, true);
            const agenda = await this.taskLoader.getAgenda(job.domain);
            const [agendaJob] = await agenda.jobs({'data.jobId': job.id});
            if (agendaJob) {
                agendaJob.schedule(new Date());
                agendaJob.priority(job.priority);
                await agendaJob.save();
                return job;
            } else {
                console.error(`Warning - job ${job.id} does not have a corresponding agenda job instance, job scheduling is skipped.`);
            }
        } else {
            const taskTypeKey = getKeyValues('TaskTypeConfig', job);
            const taskType = await this.taskLoader.get(taskTypeKey);
            if (!taskType) {
                console.log(JSON.stringify(job));
                throw new UserInputError(`Task type "${taskTypeKey.join('.')}" is not valid.`);
            }
            let paramsInvalid = taskType.validate(job.params || {});
            if (paramsInvalid) {
                if (!Array.isArray(paramsInvalid)) paramsInvalid = [paramsInvalid];
                throw new UserInputError(stringify(...paramsInvalid));
            }
            await this.populateJobSchedulingProperties(job);
            if (this.jobContextCache) {
                this.jobContextCache.cache(job);
            }
            job.context = safeJSON(job.context);
            job.status = 'PENDING';
            job.timeCreated = new Date();
            job.trials = [];
            const dup = await this.findDuplicate(job);
            if (dup && job.dedupRecent) {
                const {domain, type, params} = job;
                console.log(`Info - Job scheduling is skipped as a duplicate of job ${dup.id}: ${JSON.stringify({domain, type, params})}`);
            } else {
                job = await this.upsert('Job', job);
                const agenda = await this.taskLoader.getAgenda(job.domain);
                const agendaJob = agenda.create(`${job.domain}.${job.type}`, {jobId: job.id});
                agendaJob.schedule(new Date());
                agendaJob.priority(job.priority);
                await agendaJob.save();
                if (dup) {
                    const upserted = await this.upsert(
                        'Job', {id: dup.id, status: 'CANCELED'}, true, undefined,
                        {status: {$nin: ['SUCCESS', 'FAILED']}}
                    );
                    if (upserted) {
                        const {domain, type, params} = upserted;
                        console.log(`Info - Job ${dup.id} is canceled as a duplicate of the new job ${job.id}: ${JSON.stringify({domain, type, params})}`);
                    }
                }
                return job;
            }
        }
    }

    // for those whose key is undefined (e.g. Config which is globally unique),
    // just assign "q" with a random scalar value, like 1. and you will get the one object.
    async query(typeName, q, options, dataloaders, count) {
        let dataloader = get(dataloaders, typeName);
        if (!dataloader) {
            dataloader = this._newDataLoader(typeName);
            set(dataloaders, typeName, dataloader);
        }
        const key = TYPES[typeName].key;
        if (isQueryBy(q) === 'query') {
            if (key && key.length === 1 && Object.keys(q).length === 1) {
                const [subQ] = getKeyValues(typeName, q);
                if (typeof subQ === 'object') {
                    const {$eq} = subQ;
                    if ($eq) {
                        q = [[$eq]];
                    } else {
                        const {$in} = subQ;
                        if (Array.isArray($in)) {
                            q = [$in.map(k => [k])];
                        }
                    }
                } else if (Array.isArray(subQ)) {
                    q = [subQ.map(k => [k])];
                } else if (subQ) {
                    q = [[subQ]];
                }
            }
        }
        switch (isQueryBy(q)) {
            case 'key':
                return await dataloader.load(q);
            case 'keys':
                return await dataloader.loadMany(q);
            case 'query':
                let cursor = this.mongodb.collection(typeName).find(q);
                const {sort, limit, offset} = options || {};
                if (sort) {
                    const sortArg = {};
                    for (const s of sort) {
                        sortArg[s.by] = s.desc ? -1 : 1;
                    }
                    cursor = cursor.sort(sortArg);
                }
                if (limit > 0) cursor.limit(limit);
                if (offset > 0) cursor.skip(offset);
                if (count) {
                    return await cursor.count();
                } else {
                    const results = await cursor.toArray();
                    for (const result of results) {
                        if (key) {
                            dataloader.prime(getKeyValues(typeName, result), result);
                        }
                        shrink(result);
                    }
                    return results;
                }
        }
    }

    // doc must be a valid object subject to the typeName.
    // the primary key of the object is optional;
    // if missing, this function will make an insert; otherwise it will try upsert.
    // returns the upserted doc.
    async upsert(typeName, doc, rawChange, dataloaders, filter, waitForFilter) {
        const {key} = TYPES[typeName];
        if (key) {
            const keyValues = getKeyValues(typeName, doc);
            if (keyValues.every(v => v)) {
                const upsert = !filter;
                const query = {...filter, ...makeQuery(typeName, keyValues)};
                doc = await this._update(typeName, doc, query, rawChange, upsert, waitForFilter);
            } else {
                doc = await this._insert(typeName, doc, rawChange);
            }
        } else {
            doc = await this._update(typeName, doc, filter || {}, rawChange, !filter, waitForFilter);
        }
        let dataloader = get(dataloaders, typeName);
        if (dataloader) {
            const k = key ? getKeyValues(typeName, doc) : 1;
            dataloader.prime(k, doc);
        }
        shrink(doc);
        return doc;
    }

    async updateTaskDomainConfigs(dataloaders) {
        for (const {config: domain} of Object.values(await this.taskLoader.getAllTaskDomains())) {
            const domainKey = getKeyValues('TaskDomainConfig', domain);
            const stored = await this.query('TaskDomainConfig', domainKey, undefined, dataloaders);
            const current = await this.getTaskDomainConfig(domainKey, dataloaders);
            if (stored) {
                const configDiff = diff(stored._full, current);
                if (!isEmpty(configDiff)) {
                    console.log(`Refresh config for task domain "${domainKey.join('.')}".`);
                    // const updates = {...makeQuery('TaskDomainConfig', domainKey), ...configDiff};
                    await this.updateTaskDomainConfig(stored, {}, dataloaders);
                } else {
                    await this.updateTaskDomainConfig(stored, undefined, dataloaders);
                }
            } else {
                console.log(`Create default config for task domain "${domainKey.join('.')}".`);
                await this.updateTaskDomainConfig(makeQuery('TaskDomainConfig', domainKey), {}, dataloaders);
            }
        }
    }

    async updateTaskTypeConfigs(taskDomainConfig, dataloaders) {
        for (const taskType of Object.values(await this.taskLoader.getAllTaskTypes())) {
            if (taskDomainConfig &&
                !deepEqual(
                    getKeyValues('TaskDomainConfig', taskDomainConfig),
                    getKeyValues('TaskDomainConfig', taskType.domain.config)
                )) {
                continue;
            }
            const taskTypeKey = taskType.key;
            const stored = await this.query('TaskTypeConfig', taskTypeKey, undefined, dataloaders);
            const current = await this.getTaskTypeConfig(taskType, stored, taskDomainConfig, dataloaders);
            if (stored) {
                const configDiff = diff(stored._full, current);
                if (isEmpty(configDiff)) {
                    console.log(`Refresh config for task type "${taskTypeKey.join('.')}".`);
                    // const updates = {...makeQuery('TaskTypeConfig', taskTypeKey), ...configDiff};
                    await this.updateTaskTypeConfig(taskType, undefined, {}, dataloaders);
                } else {
                    await this.updateTaskTypeConfig(taskType, stored, undefined, dataloaders);
                }
            } else {
                console.log(`Create default config for task type "${taskTypeKey.join('.')}".`);
                await this.updateTaskTypeConfig(taskType, undefined, {}, dataloaders);
            }
        }
    }

    async updateTasks(taskTypeConfig, dataloaders) {
        const taskTypeKey = getKeyValues('TaskTypeConfig', taskTypeConfig);
        const query = taskTypeConfig ? makeQuery('TaskTypeConfig', taskTypeKey) : {};
        const cursor = this.mongodb.collection('Task').find({...query, mode: {$ne: 'ONCE'}});
        const promises = [];
        while (await cursor.hasNext()){
            const task = await cursor.next();
            const fullTask = await this.getTask(task, taskTypeConfig, dataloaders);
            const taskDiff = diff(task._full, fullTask);
            if (isEmpty(taskDiff)) {
                const taskKey = getKeyValues('Task', task);
                setKeyValues('Task', taskDiff, taskKey);
                promises.push(this.scheduleTask({id: task.id}));
                console.log(`Refresh config for task "${taskKey.join('.')}" of type "${taskTypeKey.join('.')}".`);
            }
        }
        if (promises.length > 0) {
            try {
                await Promise.all(promises);
            } catch (e) {
                if (!(e instanceof UserInputError)) {
                    throw e;
                }
            }
        }
    }

    async _ensureTaskType(taskType, config, updates) {
        if (!taskType) {
            let keyValues = getKeyValues('TaskTypeConfig', config || {});
            if (!keyValues.every(v => v)) {
                keyValues = getKeyValues('TaskTypeConfig', updates || {});
            }
            if (keyValues.every(v => v)) {
                taskType = keyValues;
            }
        }
        if (!taskType) {
            throw new UserInputError('Task domain and type must be specified.');
        }
        if (isQueryBy(taskType) === 'key') {
            const taskTypeKey = taskType;
            taskType = await this.taskLoader.get(taskTypeKey);
            if (taskType === undefined) {
                throw new UserInputError(`Task type "${taskTypeKey.join('.')}" is not valid`);
            }
        }
        return taskType;
    }

    async _validateTask(task) {
        for (const field of ['domain', 'type', 'mode']) {
            if (!task[field]) {
                throw new UserInputError(`field "${field}" must provided when creating a new Task (creation is auto detected due to "id" missing).`)
            }
        }
        const taskTypeKey = getKeyValues('TaskTypeConfig', task);
        const taskType = await this.taskLoader.get(taskTypeKey);
        if (!taskType) {
            throw new UserInputError(`Task type "${taskTypeKey.join('.')}" is not valid.`);
        }
        if (task.mode === 'REPEATED' && (task.interval === undefined || task.interval < 0)) {
            throw new UserInputError('field "interval" should be an Int >=0 when "mode" is "REPEATED"');
        }
        if (task.mode === 'SCHEDULED' && task.schedule === undefined) {
            throw new UserInputError('field "schedule" should a valid cron expression when "mode" is "SCHEDULED"');
        }
        let paramsInvalid = taskType.validate(task.params || {});
        if (paramsInvalid) {
            if (!Array.isArray(paramsInvalid)) paramsInvalid = [paramsInvalid];
            throw new UserInputError(stringify(...paramsInvalid));
        }
    }

    async _update(typeName, doc, query, rawChange, upsert, waitForResult) {
        const update = {$set: doc};
        if (!rawChange) {
            const {mtime, ctime} = TYPES[typeName];
            const now = new Date();
            if (mtime) {
                doc[mtime] = now;
            }
            if (ctime) {
                update.$setOnInsert = {[ctime]: now};
            }
        }
        if (false && process.env.KCARD_RUNTIME_STAGE === 'local') {
            console.log(`Debug - mongodb.collection(${JSON.stringify(typeName)}).findOneAndUpdate(${JSON.stringify(query)}, ${JSON.stringify(update)}, ${JSON.stringify({upsert: true, returnOriginal: false})})`);
        }
        while (true) {
            const result = await this.mongodb.collection(typeName).findOneAndUpdate(
                query, update, {upsert, returnOriginal: false}
            );
            if (!upsert && waitForResult && !result.value) {
                await sleep(100);
            } else {
                return result.value;
            }
        }
    }

    async _insert(typeName, doc, rawChange) {
        const {key, mtime, ctime} = TYPES[typeName];
        if (key && key.length > 1) {
            throw new Error('internal error: insert is not allowed for type that has keys.length > 1');
        }
        if (!rawChange) {
            const now = new Date();
            if (mtime) {
                doc[mtime] = now;
            }
            if (ctime) {
                doc[ctime] = now;
            }
        }
        if (false && process.env.KCARD_RUNTIME_STAGE === 'local') {
            console.log(`Debug - mongodb.collection(${JSON.stringify(typeName)}).insertOne(${JSON.stringify(doc)})`);
        }
        const result = await this.mongodb.collection(typeName).insertOne(doc);
        if (key) {
            const set = {};
            setKeyValues(typeName, set, [result.insertedId.toHexString()]);
            await this.mongodb.collection(typeName).updateOne({_id: result.insertedId}, {$set: set});
            Object.assign(doc, set);
        }
        return doc;
    }


    _newDataLoader(typeName) {
        const key = TYPES[typeName].key;
        if (key) {
            return new DataLoader(async ids => {
                if (false && process.env.KCARD_RUNTIME_STAGE === 'local') {
                    console.log(`Debug - mongodb.collection(${JSON.stringify(typeName)}).find(${JSON.stringify({$or: ids.map(i => makeQuery(typeName, i))})})`);
                }
                const results = await this.mongodb.collection(typeName).find({
                    $or: ids.map(i => makeQuery(typeName, i))
                }).toArray();
                const resultObject = {};
                for (const result of results) {
                    shrink(result);
                    resultObject[getKeyValues(typeName, result).join('.')] = result;
                }
                return ids.map(i => resultObject[i.join('.')]);
            });
        } else {
            return new DataLoader(async ids => {
                if (false && process.env.KCARD_RUNTIME_STAGE === 'local') {
                    console.log(`Debug - mongodb.collection(${JSON.stringify(typeName)}).findOne()`);
                }
                const result = await this.mongodb.collection(typeName).findOne();
                shrink(result);
                return ids.map(() => result);
            });
        }
    }

}


function getTaskTypeConfigFromTaskDomainConfig(domain) {
    const {maxConcurrency, ...ret} = domain;
    return ret;
}

function isQueryBy(query) {
    if (Array.isArray(query)) {
        if (query.length <= 0 || Array.isArray(query[0])) {
            return 'keys';
        } else {
            return 'key';
        }
    } else if (typeof query === 'object') {
        return 'query';
    }
}

function getKeyValues(typeName, doc) {
    if (!doc) doc = {};
    return TYPES[typeName].key.map(k => doc[k]);
}

function setKeyValues(typeName, doc, values) {
    let i = 0;
    for (const k of TYPES[typeName].key) {
        doc[k] = values[i++];
    }
}

function makeQuery(typeName, values) {
    const query = {};
    let i = 0;
    for (const k of TYPES[typeName].key) {
        query[k] = values[i++];
    }
    return query;
}


module.exports = Operations;