const {isEmpty, isPlainObject, get} = require('lodash');
const {UserInputError} = require('apollo-server-koa');
const {diff} = require('deep-object-diff');
const uuid = require('uuid/v4');

const {sleep, safeJSON, random, ensureThunkCall, errorToString, stringifyWith, merge2Level, limit, shrink} = require('@raychee/utils');
const {Logger, JobLogger, StoreLogger} = require('./logger');
const {
    CatalystError,
    JobCancellation,
    JobCrash,
    JobInterruption,
    JobTimeout,
    JobHeartAttack
} = require('./error');


class TaskDomain {

    constructor(name, pluginLoader, storeCollection) {
        this.name = name;
        this.config = {domain: this.name};
        this.store = {};
        this.pluginLoader = pluginLoader;
        this.storeCollection = storeCollection;
    }

    async load({config, plugins, store, validate, dedup, catch: catch_, failed, final}) {
        const logger = new StoreLogger(this.storeCollection, {domain: this.name}, 'Domain', [this.name]);
        this.config = {domain: this.name, ...await ensureThunkCall(config, logger)};
        if (this.pluginLoader) {
            this.plugins = merge2Level(this.pluginLoader && this.pluginLoader.defaultPlugins, await ensureThunkCall(plugins, logger, this.config));
            this.store = await ensureThunkCall(store, logger, this.config, await this.plugins, {pluginLoader: this.pluginLoader}) || {};
        }
        this.validate = validate || (() => {});
        this.dedup = dedup;
        this.catch = catch_ || (() => false);
        this.failed = failed || (() => {});
        this.final = final || (() => {});
    }

}


class TaskType {

    constructor(domain, name, operations, pluginLoader, storeCollection, jobContextCache) {
        this.domain = domain;
        this.name = name;
        this.agendaFn = undefined;
        this.operations = operations;
        this.pluginLoader = pluginLoader;
        this.jobContextCache = jobContextCache;
        this.storeCollection = storeCollection;
    }

    async load({config, plugins, store, validate, dedup, run, catch: catch_, failed, final}) {
        const logger = new StoreLogger(this.storeCollection, {taskType: this.fullName}, 'TaskType', [this.fullName]);
        this.config = {domain: this.domain.name, type: this.name, ...await ensureThunkCall(config, logger)};
        if (this.pluginLoader) {
            this.plugins = merge2Level(this.domain.plugins, await ensureThunkCall(plugins, logger, this.config));
            this.store = {...this.domain.store, ...await ensureThunkCall(store, logger, this.config, this.plugins, {pluginLoader: this.pluginLoader})};
        }
        this.validate = validate || this.domain.validate;
        this.dedup = dedup || this.domain.dedup;
        this.run = run || this.domain.run;
        this.catch = catch_ || this.domain.catch;
        this.failed = failed || this.domain.failed;
        this.final = final || this.domain.final;
    }

    get key() {
        return [this.domain.name, this.name];
    }

    get fullName() {
        return this.key.join('.');
    }

    toAgendaJobFn() {
        if (!this.agendaFn) {
            this.agendaFn = async (agendaJob, callback) => {

                async function handleCrawlerError(job, e) {
                    if (e instanceof JobCancellation) {
                        job._loggerSys.cancel('Job is canceled: ', e);
                        await job._updateTrial({
                            status: 'CANCELED', code: e.code || '_cancel', message: e.message,
                        }, true);
                        throw e;
                    } else if (e instanceof JobTimeout) {
                        job._loggerSys.timeout('Job timeout: ', e);
                        await job._updateTrial({
                            status: 'FAILED', code: e.code || '_timeout', message: e.message,
                        }, true);
                        throw e;
                    } else if (e instanceof JobCrash) {
                        job._loggerSys.crash('Job crashes on purpose: ', e);
                        await job._updateTrial({
                            status: 'FAILED', code: e.code || '_crash_on_purpose', message: e.message, timeStopped: new Date(),
                        }, true);
                        throw e;
                    } else if (e instanceof JobInterruption) {
                        throw e;
                    } else if (e instanceof JobHeartAttack) {
                        throw e;
                    }
                }

                let jobConfig = undefined;
                if (isPlainObject(agendaJob)) {
                    jobConfig = agendaJob;
                    agendaJob = undefined;
                }
                let job = undefined;

                try {
                    if (!jobConfig) {
                        if (agendaJob) {
                            const jobId = get(agendaJob, ['attrs', 'data', 'jobId']);
                            jobConfig = await this.operations.query('Job', [jobId]);
                        }
                    }
                    if (!jobConfig) {
                        throw new Error('neither jobId nor job is given');
                    }
                    job = Job.create(this, agendaJob);
                    await job._load();
                } catch (e) {
                    console.error(`System Failure - ${errorToString(e)}`);
                    callback(e);
                    return;
                }

                try {

                    for (let trialCounter = 0;
                         trialCounter <= (job.config.retry || 0);
                         trialCounter++) {

                        try {
                            if (trialCounter === 0) {
                                if (this.jobContextCache) {
                                    this.jobContextCache.loadBackTo(jobConfig);
                                }
                                const updates = {};
                                let initContext = jobConfig.initContext;
                                if (!initContext) {
                                    updates.initContext = initContext = {...jobConfig.context};
                                }
                                job._setConfig(jobConfig);
                                updates.status = 'DELAYED';
                                updates.trials = job.config.trials || [];
                                if (updates.trials.length > 0) {
                                    const lastTrial = updates.trials[updates.trials.length - 1];
                                    if (['SUCCESS', 'CANCELED'].indexOf(lastTrial.status) >= 0) {
                                        return;
                                    }
                                    updates.trials.pop();
                                }
                                updates.trials.push({status: 'DELAYED', context: initContext});
                                trialCounter = updates.trials.length - 1;
                                if (trialCounter > (job.config.retry || 0)) {
                                    return;
                                }
                                await job._update(updates);
                            } else {
                                job.config.context = {...job.config.context};
                                job.config.trials.push({});
                                job._loggerSys.retry(`${trialCounter}/${job.config.retry || 0}`);
                                await job._updateTrial({status: 'DELAYED'}, true);
                            }
                            await job._delay({}, {trial: trialCounter, updateStatus: true});
                            await job._updateTrial({status: 'RUNNING'});
                            job._loggerSys.start('Job starts.');
                            job._started = Date.now();
                            await this.run.call(job, job.config.params, job.config.context, this.store);
                            await job._updateTrial({status: 'SUCCESS'}, true);
                            job._loggerSys.complete('Job completes.');

                            break;

                        } catch (e) {
                            if (e instanceof CatalystError) {
                                await handleCrawlerError(job, e);
                                await job._updateTrial({
                                    status: 'FAILED', code: e.code || '_failed', message: e.message,
                                }, true);
                                job._loggerSys.fail('Job fails: ', e);
                                if (job._started) {
                                    try {
                                        await this.failed.call(job, e.code, e.message, job.config.params, job.config.context, this.store);
                                    } catch (ee) {
                                        await handleCrawlerError(job, ee);
                                        await job._updateTrial({
                                            status: 'FAILED', code: '_crash_failed', message: errorToString(e), timeStopped: new Date(),
                                        }, true);
                                        job._loggerSys.crash('Job crashes in failed(): ', ee);
                                        throw ee;
                                    }
                                }
                            } else {
                                let catchMessages = undefined;
                                if (job._started) {
                                    try {
                                        catchMessages = await this.catch.call(job, e, job.config.params, job.config.context, this.store);
                                    } catch (ee) {
                                        await handleCrawlerError(job, ee);
                                        await job._updateTrial({
                                            status: 'FAILED', code: '_crash_catch', message: errorToString(e), timeStopped: new Date(),
                                        }, true);
                                        job._loggerSys.crash('Job crashes in catch(): ', ee);
                                        throw ee;
                                    }
                                }
                                if (catchMessages) {
                                    if (!Array.isArray(catchMessages)) {
                                        catchMessages = [catchMessages];
                                    }
                                    await job._updateTrial({
                                        status: 'FAILED', code: e.code || '_catch', message: stringifyWith(catchMessages),
                                    }, true);
                                    job._loggerSys.catch('Job fails by catching: ', ...catchMessages);
                                } else {
                                    await job._updateTrial({
                                        status: 'FAILED', code: '_crash_run', message: errorToString(e), timeStopped: new Date(),
                                    }, true);
                                    job._loggerSys.crash('Job crashes in run(): ', e);
                                    throw e;
                                }
                            }

                        } finally {
                            if (!job._interrupted && !job._timeout && !job._heartAttack && job._started) {
                                try {
                                    await this.final.call(job, job.config.params, job.config.context, this.store);
                                } catch (e) {
                                    await handleCrawlerError(job, e);
                                    await job._updateTrial({
                                        status: 'FAILED', code: e.code || '_crash_final', message: errorToString(e), timeStopped: new Date(),
                                    }, true);
                                    job._loggerSys.crash('Job crashes in final(): ', e);
                                    throw e;
                                }
                            }
                            if (this.jobContextCache) {
                                this.jobContextCache.clearCache(job.config);
                            }
                            await job._unload();
                        }
                    }

                    callback();
                } catch (e) {
                    if (e instanceof JobHeartAttack) {
                        return;
                    }
                    callback(e);
                }
            }
        }
        return this.agendaFn;
    }

}

class Job {

    constructor(taskType, agendaJob) {
        this.config = {};
        this._taskType = taskType;
        this._agendaJob = agendaJob;


        this._logger = undefined;
        this._loggerSys = undefined;
        this._plugins = undefined;
        this._started = undefined;
        this._done = false;
        this._timeout = undefined;
        this._interrupted = false;
        this._heartAttack = false;
        this._canceled = undefined;

        this._update = limit(Job.prototype._update.bind(this), 1);
    }

    static create(taskType) {
        const job = new Job(taskType);
        return new Proxy(job, {
            get(target, p) {
                if (!p.startsWith('_')) {
                    target._checkStatusChange();
                }
                const v = target[p];
                return typeof v === 'function' ? v.bind(target) : v;
            }
        });
    }

    // schedule a new job (NOT a task!)
    // this function does NOT create a job entry in internal db;
    // it only calls agenda which in turn will execute job functions in which the job entry is created.
    async schedule(taskTypeFullName, params, context, extra) {
        if (!this._taskType.operations) return {};
        let domainName, taskTypeName;
        if (taskTypeFullName instanceof Job) {
            domainName = taskTypeFullName._taskType.domain.name;
            taskTypeName = taskTypeFullName._taskType.name;
        } else if (taskTypeFullName instanceof TaskType) {
            domainName = taskTypeFullName.domain.name;
            taskTypeName = taskTypeFullName.name;
        } else {
            let taskTypeNameFields;
            [domainName, ...taskTypeNameFields] = taskTypeFullName.split('.');
            taskTypeName = taskTypeNameFields.join('.');
        }
        const taskTypeKey = [domainName, taskTypeName];
        if (!(await this._taskType.operations.taskLoader.get(taskTypeKey))) {
            throw new UserInputError(`Task type "${taskTypeFullName}" is not valid.`);
        }
        const job = {
            domain: domainName, type: taskTypeName, params: params || {}, context: context || {},
            task: this.config.task, createdBy: this.config.id,
        };
        if (this.config.createdFrom) {
            job.createdFrom = this.config.createdFrom;
        } else {
            job.createdFrom = this.config.id;
        }
        Object.assign(job, extra); // {delay, delayRandomize, retry, retryDelayFactor, priority} = extra
        return this._taskType.operations.scheduleJob(job);
    }

    async delay(delay, delayRandomize) {
        const config = {delay, delayRandomize};
        shrink(config);
        return await this._delay(config);
    }

    cancel(code, ...values) {
        this._logger.cancel(code, ...values);
    }

    crash(code, ...values) {
        this._logger.crash(code, ...values);
    }

    fail(code, ...values) {
        this._logger.fail(code, ...values);
    }

    debug(...values) {
        this._logger.debug(...values);
    }

    info(...values) {
        this._logger.info(...values);
    }

    warn(...values) {
        this._logger.warn(...values);
    }

    error(...values) {
        this._logger.error(...values);
    }

    async _load() {
        if (!this._logger) {
            this._logger = new Logger(
                '',
                () => [this._taskType.fullName, this.config.task, this.config.id],
                (value) => {
                    if (typeof value === 'object' && (JOB_CONTEXT_CACHE_KEY in value)) {
                        value = {...value};
                        delete value[JOB_CONTEXT_CACHE_KEY];
                    }
                    return value;
                }
            );
        }
        if (!this._loggerSys) {
            this._loggerSys = new JobLogger(
                '',
                () => [this._taskType.fullName, this.config.task, this.config.id, this.config.params, this.config.context],
                (value) => {
                    if (typeof value === 'object' && (JOB_CONTEXT_CACHE_KEY in value)) {
                        value = {...value};
                        delete value[JOB_CONTEXT_CACHE_KEY];
                    }
                    return value;
                }
            );
        }
        this._plugins = await this._taskType.pluginLoader.getAll(this._taskType.plugins, this);
        for (const [pluginName, {instance}] of Object.entries(this._plugins)) {
            this[pluginName] = instance;
        }

        if (this._taskType.operations) {
            this._syncStatus().catch(e => console.log(`This should never happen: ${e}`));
        }
    }

    async _delay(config, {trial = 0, updateStatus = false} = {}) {
        function calcDelay() {
            const {delayRandomize, retryDelayFactor} = config;
            const base = config.delay * Math.pow(retryDelayFactor, trial);
            const delayMin = base * (1 - delayRandomize);
            const delayMax = base * (1 + delayRandomize);
            return [random(delayMin, delayMax), delayMin, delayMax];
        }

        config = {...config, ...this.config};
        let [delay, delayMin, delayMax] = calcDelay();
        let configDelay = config.delay;
        if (delay > 0) {
            this._loggerSys.delay(
                'Delay ', delay, ' seconds (randomly chosen between ',
                delayMin, ' and ', delayMax, ' seconds)'
            );
        }
        if (updateStatus) {
            await this._updateTrial({delay});
        }
        if (delay > 0) {
            const sleepAt = Date.now();
            const heartbeat = this._taskType.operations ? this._taskType.operations.options.heartbeat * 1000 : Number.MAX_SAFE_INTEGER;
            while (true) {
                let interval = sleepAt + delay * 1000 - Date.now();
                if (interval > heartbeat) interval = heartbeat;
                if (interval <= 0) break;
                await sleep(interval);
                this._checkStatusChange();
                if (config.delay !== configDelay) {
                    const [delayNew, delayMinNew, delayMaxNew] = calcDelay();
                    this._loggerSys.delay(
                        'Changed delay from ', delay, ' seconds to ', delayNew, ' seconds (randomly chosen between ',
                        delayMinNew, ' and ', delayMaxNew, ' seconds)'
                    );
                    [delay, delayMin, delayMax] = [delayNew, delayMinNew, delayMaxNew];
                    configDelay = config.delay;
                    if (updateStatus) {
                        await this._updateTrial({delay});
                    }
                }
            }
        }
    }

    async _unload() {
        this._done = true;
        await Promise.all(Object.values(this._plugins).map(
            async ({key, destroy, config = {}}) => {
                if (!key || config.destroyOnJobDone) {
                    await destroy();
                }
            }
        ));
    }

    async _updateTrial({status, delay, code, message, timeStopped}, updateContext) {
        const updates = {};
        const trial = this.config.trials[this.config.trials.length - 1];
        if (delay !== undefined) {
            trial.delay = delay;
        }
        if (status) {
            updates.status = status;
            trial.status = status;
            if (timeStopped) {
                updates.timeStopped = timeStopped;
            }
            if (status === 'RUNNING') {
                trial.timeStarted = new Date(); // TODO
                if (this.config.trials.length === 1) {
                    updates.timeStarted = trial.timeStarted;
                }
            } else if (['SUCCESS', 'FAILED', 'CANCELED'].indexOf(status) >= 0) {
                trial.timeStopped = timeStopped || new Date();
                if (
                    ['SUCCESS', 'CANCELED'].indexOf(status) >= 0 ||
                    this.config.trials.length > (this.config.retry || 0)
                ) {
                    updates.timeStopped = trial.timeStopped;
                }
            }
        }
        if (code || message) {
            const failStatus = {code, message};
            updates.fail = failStatus;
            trial.fail = failStatus;
        }
        if (updateContext) {
            trial.context = this.config.context;
        }
        updates.trials = this.config.trials;
        await this._update(updates, updateContext);
    }

    async _update(updates, updateContext) {
        updates = {...updates};
        if (!this.config.id && this._taskType.operations) {
            console.log(`This should never happen: job.config.id = ${this.config.id}`);
            return;
        }
        updates.id = this.config.id;
        if (updateContext) {
            updates.context = this.config.context;
        }
        let contextUpdate = updates.context;
        let initContextUpdate = updates.initContext;
        if (contextUpdate) {
            const safeContext = safeJSON(contextUpdate);
            const contextDiff = diff(safeContext, contextUpdate);
            if (!isEmpty(contextDiff)) {
                for (const [p, v] of Object.entries(contextDiff)) {
                    this._loggerSys.warn(`"${p}" in context is not serializable, so its persistence is skipped: ${v}`);
                }
                updates.context = safeContext;
            }
        }
        if (updates.initContext) {
            updates.initContext = safeJSON(updates.initContext);
        }
        if (updates.trials) {
            updates.trials = updates.trials.map(t => {
                if (t.context) {
                    t = {...t};
                    t.context = safeJSON(t.context);
                }
                return t;
            });
        }
        let updated;
        if (this._taskType.operations) {
            if (
                updates.status === 'SUCCESS' ||
                updates.status === 'FAILED' && updates.timeStopped
            ) {
                updated = await this._taskType.operations.upsert('Job', updates, false);
            } else {
                updated = await this._taskType.operations.upsert('Job', updates, false, undefined, {status: {$ne: 'INTERRUPTED'}});
            }
        } else {
            updated = {...this.config, ...updates};
        }
        if (updated) {
            this._setConfig(updated);
        } else {
            this._interrupted = true;
            this.config = {...this.config, ...updates};
        }
        if (contextUpdate) this.config.context = contextUpdate;
        if (initContextUpdate) this.config.initContext = initContextUpdate;
        if (this._taskType.jobContextCache) {
            this._taskType.jobContextCache.cache(this.config);
        }
    }

    _checkStatusChange() {
        if (this._canceled) {
            const message = this._canceled === '_cancel_status' ?
                'Job cancels because of manual status change.' :
                'Job cancels because the task is disabled.';
            this.cancel(this._canceled, message);
        }
        if (this._interrupted) {
            this._loggerSys.interrupt('Job is interrupted due to possible system shutdown.');
            this._logger.interrupt('_interrupt', 'Job is interrupted due to possible system shutdown.');
        }
        if (this._timeout) {
            this._logger.timeout('_timeout', 'Job execution time exceeds ', this.config.timeout, ' seconds and is terminated.');
        }
        if (this._heartAttack) {
            this._loggerSys.heartAttack(
                'Job\'s heartbeat is slower than ',
                this._taskType.operations.options.heartAttack,
                ' seconds and will be considered dead. A new job will be / is being spawned and replace this job.'
            );
            this._logger.heartAttack(
                '_heart_attack', 'Job\'s heartbeat is slower than ',
                this._taskType.operations.options.heartAttack,
                ' seconds and will be considered dead. A new job will be / is being spawned and replace this job.'
            );
        }
    }

    async _syncStatus() {
        let lastTouchedAt = undefined;
        while (!this._done && !this._interrupted && !this._canceled && !this._timeout) {
            await sleep(this._taskType.operations.options.heartbeat * 1000);
            if (!this.config.id) continue;
            try {
                const task = await this._taskType.operations.getTask([this.config.task]);
                const diff = await this._taskType.operations.populateJobSchedulingProperties(this.config, task, undefined, true);
                await this._update(diff, true);
                if (this.config.status === 'CANCELED') {
                    this._canceled ='_cancel_status';
                }
                if (!task.enabled) {
                    this._canceled = '_cancel_disabled';
                }
                if (this._agendaJob) {
                    const now = Date.now();
                    if (now - lastTouchedAt > this._taskType.operations.options.heartAttack * 1000) {
                        this._heartAttack = true;
                    } else {
                        lastTouchedAt = now;
                        await this._agendaJob.touch();
                    }
                }
                if (this.config.timeout > 0 && this._started < Date.now() - this.config.timeout * 1000) {
                    this._timeout = true;
                }
            } catch (e) {
                this._loggerSys.warn('Job status sync failed: ', e);
            }
        }
    }

    // "config" is the one from db
    _setConfig(config) {
        const jobContextCacheKey = this.config[JOB_CONTEXT_CACHE_KEY];
        if (jobContextCacheKey) {
            config[JOB_CONTEXT_CACHE_KEY] = jobContextCacheKey;
        }
        config.context = mergeContext(this.config.context, config.context);
        config.initContext = mergeContext(this.config.initContext, config.initContext);
        this.config = config;
    }

}

function mergeContext(oldContext, newContext) {
    const merged = {...newContext};
    oldContext = oldContext || {};
    const safeContext = safeJSON(oldContext);
    const contextDiff = diff(safeContext, oldContext);
    for (const [p, v] of Object.entries(contextDiff)) {
        if (merged[p] === undefined || merged[p] === null) {
            merged[p] = v;
        }
    }
    return merged;
}


const JOB_CONTEXT_CACHE_KEY = 'contextCacheId';
const JOB_INIT_CONTEXT_CACHE_KEY = 'initContextCacheId';

class JobContextCache {

    constructor() {
        this._cache = {};
    }

    cache(jobConfig) {
        if (!jobConfig[JOB_CONTEXT_CACHE_KEY]) jobConfig[JOB_CONTEXT_CACHE_KEY] = uuid();
        this._cache[jobConfig[JOB_CONTEXT_CACHE_KEY]] = jobConfig.context;
        // if (!jobConfig[JOB_INIT_CONTEXT_CACHE_KEY]) jobConfig[JOB_INIT_CONTEXT_CACHE_KEY] = uuid();
        // this._cache[jobConfig[JOB_INIT_CONTEXT_CACHE_KEY]] = jobConfig.initContext;
    }

    loadBackTo(jobConfig) {
        if (jobConfig[JOB_CONTEXT_CACHE_KEY]) {
            const cached = this._cache[jobConfig[JOB_CONTEXT_CACHE_KEY]];
            if (cached) {
                jobConfig.context = mergeContext(cached, jobConfig.context);
            }
        }
        // if (jobConfig[JOB_INIT_CONTEXT_CACHE_KEY]) {
        //     const cached = this._cache[jobConfig[JOB_INIT_CONTEXT_CACHE_KEY]];
        //     if (cached) {
        //         jobConfig.initContext = mergeContext(cached, jobConfig.initContext);
        //     }
        // }
    }

    clearCache(jobConfig) {
        if (jobConfig[JOB_CONTEXT_CACHE_KEY]) {
            delete this._cache[jobConfig[JOB_CONTEXT_CACHE_KEY]];
        }
        // if (jobConfig[JOB_INIT_CONTEXT_CACHE_KEY]) {
        //     delete this._cache[jobConfig[JOB_INIT_CONTEXT_CACHE_KEY]];
        // }
    }

}


module.exports = {
    TaskDomain,
    TaskType,
    Job,
    JobContextCache,
};