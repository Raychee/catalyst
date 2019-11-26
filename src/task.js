const {UserInputError} = require('apollo-server-koa');
const uuid = require('uuid/v4');

const {JOB_DELAY_CHECK_INTERVAL, JOB_MIN_CHECK_INTERVAL} = require('./config');
const {Logger, JobLogger, StoreLogger} = require('./logger');
const {sleep, safeJSON, random, ensureThunkCall, errorToString, merge2Level, dedup, diff} = require('./utils');
const {
    CrawlerError,
    CrawlerCancellation,
    CrawlerIntentionalCrash,
    CrawlerInterruption
} = require('./error');


class TaskDomain {

    constructor(name, operations, pluginLoader, storeCollection) {
        this.name = name;
        this.config = {domain: this.name};
        this.store = {};
        this.operations = operations;
        this.pluginLoader = pluginLoader;
        this.storeCollection = storeCollection;
    }

    async load({config, plugins, store, dedup}) {
        const logger = new StoreLogger(this.storeCollection, {domain: this.name}, 'Domain', [this.name]);
        this.config = {domain: this.name, ...await ensureThunkCall(config, logger)};
        if (this.pluginLoader) {
            this.plugins = merge2Level(this.pluginLoader && this.pluginLoader.defaultPlugins, await ensureThunkCall(plugins, logger, this.config));
            this.store = await ensureThunkCall(store, logger, this.config, await this.plugins, {pluginLoader: this.pluginLoader}) || {};
        }
        this.dedup = dedup;
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
        this.validate = validate || (() => {});
        this.dedup = dedup || this.domain.dedup;
        this.run = run || (() => {});
        this.catch = catch_ || (() => false);
        this.failed = failed || (() => {});
        this.final = final || (() => {});
    }

    get key() {
        return [this.domain.name, this.name];
    }

    get fullName() {
        return this.key.join('.');
    }

    toAgendaJobFn() {
        if (!this.agendaFn) {
            this.agendaFn = async (agendaJob) => {

                const {jobId} = agendaJob.attrs.data;
                if (!jobId) {
                    throw new Error('System internal error: neither taskId nor jobId is given');
                }

                const job = new Job(this, agendaJob);

                for (let trialCounter = 0;
                     trialCounter <= (job.config.retry || 0);
                     trialCounter++) {

                    try {
                        if (trialCounter === 0) {
                            // this job is triggered by Operations.scheduleJob(),
                            // or from an incomplete job (probably because of a system crash)
                            const jobConfig = await this.operations.query('Job', [jobId]);
                            this.jobContextCache.loadBackTo(jobConfig);
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
                            await job._load();
                        } else {
                            job.config.context = {...job.config.context};
                            job.config.trials.push({});
                            job._loggerSys.retry(`${trialCounter}/${job.config.retry || 0}`);
                            await job._updateTrial({status: 'DELAYED'}, true);
                        }
                        await job._checkStatusChange();
                        let delay = 0;
                        if (job.config.delay > 0) {
                            const {delayRandomize, retryDelayFactor} = job.config;
                            const base = job.config.delay * Math.pow(retryDelayFactor, trialCounter);
                            const delayMin = base * (1 - delayRandomize);
                            const delayMax = base * (1 + delayRandomize);
                            delay = random(delayMin, delayMax);
                            if (delay > 0) {
                                job._loggerSys.delay(`Delay ${delay} seconds (randomly chosen between ${delayMin} and ${delayMax} seconds)`);
                            }
                        }
                        await job._updateTrial({delay});
                        if (delay > 0) {
                            const awakeAt = Date.now() + delay * 1000;
                            let interval = 1;
                            while (interval > 0) {
                                interval = awakeAt - Date.now();
                                if (interval > JOB_DELAY_CHECK_INTERVAL * 1000) interval = JOB_DELAY_CHECK_INTERVAL * 1000;
                                await sleep(interval);
                                await job._checkStatusChange();
                            }
                        }
                        job._loggerSys.start('Job starts.');
                        await job._updateTrial({status: 'RUNNING'});
                        await this.run.call(job, job.config.params, job.config.context, this.store);
                        job._loggerSys.complete('Job completes.');
                        await job._updateTrial({status: 'SUCCESS'}, true);

                        break;

                    } catch (e) {
                        if (e instanceof CrawlerError) {
                            if (e instanceof CrawlerCancellation) {
                                job._loggerSys.cancel('Job is canceled: ', e);
                                await job._updateTrial({
                                    status: 'CANCELED', code: e.code || '_cancel', message: e.message,
                                }, true);
                                throw e;
                            } else if (e instanceof CrawlerIntentionalCrash) {
                                job._loggerSys.crash('Job crashes on purpose: ', e);
                                await job._updateTrial({
                                    status: 'FAILED', code: e.code || '_crash_on_purpose', message: e.message, timeStopped: new Date(),
                                }, true);
                                throw e;
                            } else if (e instanceof CrawlerInterruption) {
                                throw e;
                            } else {
                                job._loggerSys.fail('Job fails: ', e);
                                await job._updateTrial({
                                    status: 'FAILED', code: e.code || '_failed', message: e.message,
                                }, true);
                                try {
                                    await this.failed.call(job, e.code, e.message, job.config.params, job.config.context, this.store);
                                } catch (ee) {
                                    job._loggerSys.crash('Job crashes in failed(): ', ee);
                                    await job._updateTrial({
                                        status: 'FAILED', code: '_crash_failed', message: errorToString(e), timeStopped: new Date(),
                                    }, true);
                                    throw ee;
                                }
                            }
                        } else {
                            let catchMessage;
                            try {
                                catchMessage = await this.catch.call(job, e, job.config.params, job.config.context, this.store);
                            } catch (ee) {
                                if (ee instanceof CrawlerInterruption) {
                                    throw ee;
                                }
                                job._loggerSys.crash('Job crashes in catch(): ', ee);
                                if (job.config.id) {
                                    await job._updateTrial({
                                        status: 'FAILED', code: '_crash_catch', message: errorToString(e), timeStopped: new Date(),
                                    }, true);
                                }
                                throw ee;
                            }
                            if (catchMessage) {
                                job._loggerSys.catch('Job fails by catching: ', e);
                                if (job.config.id) {
                                    await job._updateTrial({
                                        status: 'FAILED', code: e.code || '_catch', message: `${catchMessage}: ${errorToString(e)}`,
                                    }, true);
                                }
                            } else {
                                job._loggerSys.crash('Job crashes in run(): ', e);
                                if (job.config.id) {
                                    await job._updateTrial({
                                        status: 'FAILED', code: '_crash_run', message: errorToString(e), timeStopped: new Date(),
                                    }, true);
                                }
                                throw e;
                            }
                        }

                    } finally {
                        if (!job._interrupted) {
                            this.jobContextCache.clearCache(job.config);
                            try {
                                await this.final.call(job, job.config.params, job.config.context, this.store);

                                // the following is a hack to agenda which tries to solve a bug that
                                // disabling an agenda job during its execution will end up resetting the "disabled" flag
                                // back to "false" when the execution is over. hard refresh the job to make it
                                // behave as expected.
                                // if (agendaJob.attrs._id) {
                                //     const [agendaJobFromDb] = await agendaJob.agenda.jobs({_id: agendaJob.attrs._id});
                                //     agendaJob.attrs = {
                                //         ...agendaJobFromDb.attrs,
                                //         nextRunAt: agendaJob.attrs.nextRunAt,
                                //         type: agendaJob.attrs.type
                                //     };
                                // }

                                await job._unload();

                            } catch (e) {
                                if (e instanceof CrawlerInterruption) {
                                    throw e;
                                }
                                job._loggerSys.crash('Job crashes in final(): ', e);
                                if (job.config.id) {
                                    await job._updateTrial({
                                        status: 'FAILED', code: e.code || '_crash_final', message: errorToString(e), timeStopped: new Date(),
                                    }, true);
                                }
                                throw e;
                            }
                        }
                    }
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

        this._interrupted = false;
        this._logger = undefined;
        this._loggerSys = undefined;

        this._checkStatusChange = dedup(Job.prototype._checkStatusChange.bind(this), {within: JOB_MIN_CHECK_INTERVAL * 1000});
    }

    // schedule a new job (NOT a task!)
    // this function does NOT create a job entry in internal db;
    // it only calls agenda which in turn will execute job functions in which the job entry is created.
    async schedule(taskTypeFullName, params, context, extra) {
        await this._checkStatusChange();
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
        await this._checkStatusChange();
        if (delay === undefined) delay = this.config.delay;
        if (delayRandomize === undefined) delayRandomize = this.config.delayRandomize;
        if (delay > 0) {
            const base = delay;
            const delayMin = base * (1 - delayRandomize);
            const delayMax = base * (1 + delayRandomize);
            delay = random(delayMin, delayMax);

            const awakeAt = Date.now() + delay * 1000;
            let interval = 1;
            while (interval > 0) {
                interval = awakeAt - Date.now();
                if (interval > JOB_DELAY_CHECK_INTERVAL * 1000) interval = JOB_DELAY_CHECK_INTERVAL * 1000;
                await sleep(interval);
                await this._checkStatusChange();
            }
        }
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
        const plugins = await this._taskType.pluginLoader.ensurePluginInstances(this._taskType.plugins, this);
        for (const [pluginName, plugin] of Object.entries(plugins)) {
            this[pluginName] = plugin;
        }
    }

    async _unload() {
        for (const pluginName of Object.keys(this._taskType.plugins)) {
            const plugin = this[pluginName];
            if (plugin && plugin._destroy) {
                await plugin._destroy();
            }
        }
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
        if (updateContext) {
            updates.context = this.config.context;
        }
        let contextUpdate = updates.context;
        let initContextUpdate = updates.initContext;
        if (contextUpdate) {
            const safeContext = safeJSON(contextUpdate);
            const contextDiff = diff(safeContext, contextUpdate);
            if (Object.keys(contextDiff).length > 0) {
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
        if (this.config.id) updates.id = this.config.id;
        let updated;
        if (
            updates.status === 'SUCCESS' ||
            updates.status === 'FAILED' && updates.timeStopped
        ) {
            updated = await this._taskType.operations.upsert('Job', updates, false);
        } else {
            updated = await this._taskType.operations.upsert('Job', updates, false, undefined, {status: {$ne: 'INTERRUPTED'}});
        }
        if (updated) {
            this._setConfig(updated);
        } else {
            this._interrupted = true;
            this.config = {...this.config, ...updates};
        }
        if (contextUpdate) this.config.context = contextUpdate;
        if (initContextUpdate) this.config.initContext = initContextUpdate;
        this._taskType.jobContextCache.cache(this.config);
    }

    async _checkStatusChange() {
        await this._update(undefined, true);
        if (this._interrupted) {
            this._loggerSys.interrupt('Job is interrupted due to possible system shutdown.');
            this._logger.interrupt('_interrupt', 'Job is interrupted due to possible system shutdown.');
        }
        if (this.config.status === 'CANCELED') {
            this.cancel('_cancel_status', 'Job cancels because of manual status change.');
        }
        const task = await this._taskType.operations.getTask([this.config.task]);
        if (!task.enabled) {
            this.cancel('_cancel_disabled', 'Job cancels because the task is disabled.');
        }
        const diff = await this._taskType.operations.populateJobSchedulingProperties(this.config, task, undefined, true);
        if (Object.keys(diff).length > 0) {
            await this._update(diff);
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