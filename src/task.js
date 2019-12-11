const {isEmpty, get} = require('lodash');
const {UserInputError} = require('apollo-server-koa');
const {diff} = require('deep-object-diff');
const uuid = require('uuid/v4');

const {sleep, safeJSON, random, ensureThunkCall, errorToString, stringifyWith, merge2Level, limit} = require('@raychee/utils');
const {JOB_DELAY_CHECK_INTERVAL, JOB_STATUS_CHECK_INTERVAL} = require('./config');
const {Logger, JobLogger, StoreLogger} = require('./logger');
const {
    CrawlerError,
    CrawlerCancellation,
    CrawlerIntentionalCrash,
    CrawlerInterruption
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
            this.agendaFn = async (agendaJob) => {

                async function handleCrawlerError(job, e) {
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
                    }
                }

                let jobId = get(agendaJob, ['attrs', 'data', 'jobId']);
                let jobConfig = agendaJob;

                const job = Job.create(this);

                for (let trialCounter = 0;
                     trialCounter <= (job.config.retry || 0);
                     trialCounter++) {

                    await job._load();
                    try {
                        if (trialCounter === 0) {
                            if (jobId) {
                                jobConfig = await this.operations.query('Job', [jobId]);
                            }
                            if (!jobConfig) {
                                throw new Error('System internal error: neither jobId nor job is given');
                            }
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
                                job._checkStatusChange();
                            }
                        }
                        await job._updateTrial({status: 'RUNNING'});
                        job._loggerSys.start('Job starts.');
                        job._started = true;
                        await this.run.call(job, job.config.params, job.config.context, this.store);
                        await job._updateTrial({status: 'SUCCESS'}, true);
                        job._loggerSys.complete('Job completes.');

                        break;

                    } catch (e) {
                        if (e instanceof CrawlerError) {
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
                        if (!job._interrupted && job._started) {
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
            }
        }
        return this.agendaFn;
    }

}

class Job {

    constructor(taskType) {
        this.config = {};
        this._taskType = taskType;


        this._logger = undefined;
        this._loggerSys = undefined;
        this._started = false;
        this._done = false;
        this._interrupted = false;
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
                this._checkStatusChange();
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
        const plugins = await this._taskType.pluginLoader.ensurePluginInstances(this._taskType.plugins, this);
        for (const [pluginName, plugin] of Object.entries(plugins)) {
            this[pluginName] = plugin;
        }

        if (this._taskType.operations) {
            this._syncStatus().catch(e => console.log(`This should never happen: ${e}`));
        }
    }

    async _unload() {
        this._done = true;
        for (const pluginName of Object.keys(this._taskType.plugins)) {
            const plugin = this[pluginName];
            if (plugin && plugin._onJobDone) {
                await plugin._onJobDone();
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
    }

    async _syncStatus() {
        while (!this._done && !this._interrupted && !this._canceled) {
            await sleep(JOB_STATUS_CHECK_INTERVAL * 1000);
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