const {EventEmitter} = require('events');
const debug = require('debug')('catalyst:task');

const {isEmpty} = require('lodash');
const {ObjectID} = require('mongodb');

const {
    sleep, random, ensureThunkCall, ensureThunkSync, merge, readOnly, dedup, limit, shrink
} = require('@raychee/utils');
const {Logger, JobLogger, StoreLogger} = require('./logger');
const {
    CatalystError, CatchableError, JobRuntimeError, JobRuntime, JobEarlyExit, JobCancellation, JobCrash, JobTimeout,
} = require('./error');


class TaskDomain {

    /**
     * @param {string} name
     * @param {import('./loader').PluginLoader} [pluginLoader]
     */
    constructor(name, pluginLoader) {
        this.name = name;
        this.store = {};
        this.pluginLoader = pluginLoader;
    }

    async load({plugins, store, validate, dedup, catch: catch_, failed, final}) {
        if (this.pluginLoader) {
            const logger = new StoreLogger({
                category: 'Domain', prefixes: [this.name],
                collection: this.pluginLoader.storeCollection, filter: {domain: this.name},
            });
            this.plugins = merge(
                this.pluginLoader.defaultPlugins,
                await ensureThunkCall(plugins, logger) || {},
                {inplace: false, depth: 1}
            );
            this.store = await ensureThunkCall(
                store, logger, {plugins: this.plugins, pluginLoader: this.pluginLoader}
            ) || {};
        }
        this.dedup = dedup;
        if (validate) this.validate = validate;
        if (catch_) this.catch = catch_;
        if (failed) this.failed = failed;
        if (final) this.final = final;
    }
    
    validate() {}
    catch() {}
    failed() {}
    final() {}

}


class TaskType {

    /**
     * @param {TaskDomain} domain
     * @param {string} name
     * @param {import('./loader').PluginLoader} [pluginLoader]
     */
    constructor(domain, name, pluginLoader) {
        this.domain = domain;
        this.name = name;
        this.pluginLoader = pluginLoader;
    }

    async load({plugins, store, validate, dedup, run, catch: catch_, failed, final}) {
        if (this.pluginLoader) {
            const logger = new StoreLogger({
                category: 'TaskType', prefixes: [this.fullName],
                collection: this.pluginLoader.storeCollection, filter: {taskType: this.fullName},
            });
            this.plugins = merge(
                this.domain.plugins,
                await ensureThunkCall(plugins, logger) || {},
                {inplace: false, depth: 1}
            );
            this.store = {
                ...this.domain.store,
                ...await ensureThunkCall(store, logger, {plugins: this.plugins, pluginLoader: this.pluginLoader})
            };
        }
        this.validate = validate || this.domain.validate;
        this.dedup = dedup || this.domain.dedup;
        this.run = run || (() => {});
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

}

class Job extends EventEmitter {

    /**
     * @param {object} config
     * @param {TaskType} taskType
     * @param {import('./scheduler').Scheduler} [scheduler]
     * @param {import('./operations').Operations} [operations]
     */
    constructor(config, taskType, scheduler, operations) {
        super();

        this.config = config;

        /** @type {TaskType} @private */
        this._taskType = taskType;
        /** @type {import('./scheduler').Scheduler} @private */
        this._scheduler = scheduler;
        /** @type {import('./operations').Operations} @private */
        this._operations = operations;

        /** @type {import('./logger').Logger} @private */
        this._logger = undefined;
        /** @type {import('./logger').JobLogger} @private */
        this._jobLogger = undefined;
        this._plugins = {};

        this._done = false;
        this._timeout = undefined;
        this._interrupted = false;
        this._canceled = false;

        this._jobWatcher = new JobWatcher(
            this._operations && this._operations.jobs,
            {
                pipeline(jobId) {
                    return [{
                        $match: {
                            'operationType': 'update',
                            'documentKey._id': ObjectID(jobId),
                            'updateDescription.updatedFields.status': {$in: ['SUCCESS', 'FAILED', 'CANCELED']},
                        }
                    }]
                },
                options: {fullDocument: 'updateLookup'},
            },
        );

        this._logger = new Logger({
            prefixes: () => [
                this._taskType.fullName,
                this.config.task && this.config.task.toString(),
                this.config._id && this.config._id.toString(),
                this.config.params,
                this.config.context
            ].filter(v => v),
        });
        this._jobLogger = new JobLogger({
            prefixes: () => [
                this._taskType.fullName,
                this.config.task && this.config.task.toString(),
                this.config._id && this.config._id.toString()
            ].filter(v => v)
        });

        this._update = limit(Job.prototype._update.bind(this), 1);
        this._syncStatus = dedup(Job.prototype._syncStatus.bind(this));
    }

    async schedule(taskTypeFullName, params = {}, {wait} = {}) {
        let [domainName, ...taskTypeNameFields] = taskTypeFullName.split('.');
        let taskTypeName = taskTypeNameFields.join('.');
        if (!taskTypeName) {
            [domainName, taskTypeName] = [this._taskType.domain.name, domainName];
        }
        let job = {
            domain: domainName, type: taskTypeName, params, context: {},
            task: this.config.task, createdBy: this.config._id,
            createdFrom: this.config.createdFrom || this.config._id,
        };
        if (!this._operations) {
            job._id = new ObjectID();
        } else {
            job = await this._operations.insertJob(job);
        }
        this.emit('schedule', job);
        if (wait) {
            if (typeof wait !== 'object') wait = {};
            job = await this.wait(job, wait);
        }
        return job;
    }

    async wait(job, {forSuccess = true, timeout = -1} = {}) {
        let jobId = ObjectID.isValid(job) ? job : job._id;
        if (!jobId) {
            this.crash('_wait', 'cannot wait for a non-existing job ', job);
        }
        this.emit('wait', jobId);
        while (true) {
            this._logger.wait('Wait for job ', jobId, ' to ', forSuccess ? 'succeed' : 'stop', '.');
            if (timeout >= 0) {
                setTimeout(() => {
                    if (this._jobWatcher.isWatching(jobId)) {
                        this._jobWatcher.watched(
                            jobId,
                            this._operations ? this._operations.jobs.findOne({_id: jobId}) : job
                        )
                    }
                }, timeout * 1000);
            }
            const watching = this._jobWatcher.watch(jobId);
            if (this._operations) {
                job = await this._operations.jobs.findOne({_id: jobId});
                if (['SUCCESS', 'FAILED', 'CANCELED'].includes(job.status)) {
                    this._jobWatcher.watched(jobId, job);
                }
            }
            job = await watching;
            if (job.duplicateOf) {
                this._logger.wait('Job ', jobId, ' is duplicate and canceled.');
                jobId = job.duplicateOf;
            } else {
                break;
            }
        }
        if (forSuccess && job.status !== 'SUCCESS') {
            this.fail(
                '_wait_failed', 'wait for job ', job._id, ' of type ',
                job.domain, '.', job.type, ' with params ', job.params, ' that failed'
            );
        }
        return job;
    }

    async delay(delay, delayRandomize) {
        const config = {delay, delayRandomize};
        shrink(config);
        return await this._delay(config);
    }

    cancel(code, ...values) {
        this._logger.cancel(...values);
        this._jobLogger.cancel(code, ...values);
    }

    crash(code, ...values) {
        this._jobLogger.crash(code, ...values);
    }

    fail(code, ...values) {
        this._jobLogger.fail(code, ...values);
    }

    debug(...values) {
        this._jobLogger.debug(...values);
    }

    info(...values) {
        this._jobLogger.info(...values);
    }

    warn(...values) {
        this._jobLogger.warn(...values);
    }

    error(...values) {
        this._jobLogger.error(...values);
    }

    _asAgent() {
        return new Proxy(this, {
            get(target, p) {
                if (p.startsWith('_')) {
                    target.crash('_bad_op', `access to this.${p} is invalid in task runners`);
                }
                target._checkStatusChange();
                let v = target[p];
                if (p === 'config') {
                    return readOnly(v);
                }
                if (typeof v === 'function') v = v.bind(target);
                if (!v) {
                    const {instance} = target._plugins[p] || {};
                    v = instance;
                }
                return v;
            },
            set(target, p, value, receiver) {
                target.crash('_bad_op', 'cannot set property values to "this" in task runners');
            }
        });
    }

    async _delay(conf, {trial = 0, updateStatus = false} = {}) {
        const calcDelay = () => {
            const {delayRandomize, retryDelayFactor} = config;
            const base = config.delay * Math.pow(retryDelayFactor, trial);
            const delayMin = base * (1 - delayRandomize);
            const delayMax = base * (1 + delayRandomize);
            return [random(delayMin, delayMax), delayMin, delayMax];
        };

        let config = {...this.config, ...conf};
        let [delay, delayMin, delayMax] = calcDelay();
        let configDelay = config.delay;
        if (delay > 0) {
            const message = ['Delay ', delay, ' seconds.'];
            if (delayMin < delayMax) {
                message.push(' (randomly chosen between ', delayMin, ' and ', delayMax, ' seconds)');
            }
            this._logger.delay(...message);
        }
        if (updateStatus) {
            await this._updateTrial({status: 'DELAYED', delay});
        }
        if (delay > 0) {
            const sleepAt = Date.now();
            const heartbeat = this._scheduler && this._scheduler.options.heartbeat * 1000 || 1000;
            while (true) {
                let interval = sleepAt + delay * 1000 - Date.now();
                if (interval > heartbeat) interval = heartbeat;
                if (interval <= 0) break;
                await sleep(interval);
                this._checkStatusChange();
                config = {...this.config, ...conf};
                if (config.delay !== configDelay) {
                    const [delayNew, delayMinNew, delayMaxNew] = calcDelay();
                    const message = ['Changed delay from ', delay, ' seconds to ', delayNew, ' seconds.'];
                    if (delayMinNew < delayMaxNew) {
                        message.push(' (randomly chosen between ', delayMinNew, ' and ', delayMaxNew, ' seconds)');
                    }
                    this._logger.delay(...message);
                    [delay, delayMin, delayMax] = [delayNew, delayMinNew, delayMaxNew];
                    configDelay = config.delay;
                    if (updateStatus) {
                        await this._updateTrial({delay});
                    }
                }
            }
        }
    }

    async _load() {
        if (this._taskType.pluginLoader) {
            this._plugins = await this._taskType.pluginLoader.getAll(this._taskType.plugins, this._asAgent());
        }
    }

    async _unload() {
        const promises = Object.values(this._plugins).map(
            async ({key, destroy, config = {}}) => {
                if (!key || config.destroyOnJobCompleted) {
                    await destroy();
                }
            }
        );
        if (promises.length > 0) {
            await Promise.all(promises);
        }
        this._plugins = {};
    }

    async _updateTrial({status, delay, code, message, timeStopped}, {includeContext = false} = {}) {
        const update = {};
        const trial = this.config.trials[this.config.trials.length - 1];
        if (delay !== undefined) {
            trial.delay = delay;
        }
        if (status) {
            trial.status = status;
            if (status === 'RUNNING') {
                update.status = status;
                trial.timeStarted = new Date();
                if (this.config.trials.length === 1) {
                    update.timeStarted = trial.timeStarted;
                }
            } else if (['SUCCESS', 'FAILED', 'CANCELED'].includes(status)) {
                trial.timeStopped = timeStopped || new Date();
                if (status !== 'FAILED' || this.config.trials.length > this.config.retry) {
                    update.status = status;
                    update.timeStopped = trial.timeStopped;
                }
            }
        }
        if (timeStopped) {
            update.timeStopped = timeStopped;
            trial.timeStopped = timeStopped;
            if (status) {
                update.status = status;
            }
        }
        if (code || message) {
            const failStatus = {code, message};
            update.fail = failStatus;
            trial.fail = failStatus;
        }
        if (includeContext) {
            trial.context = this.config.context;
        }
        update.trials = this.config.trials;
        await this._update(update, {includeContext});
    }

    /**
     * @param {object} [update]
     * @param {boolean} includeContext
     * @return {Promise<void>}
     * @private
     */
    async _update(update, {includeContext = false} = {}) {
        update = {...update};
        if (!this.config._id && this._operations) {
            this._logger.error('This should never happen: job.config._id = ', this.config._id);
            return;
        }
        if (includeContext) {
            update.context = this.config.context;
        }
        if (this._operations) {
            const filter = {_id: this.config._id};
            if (this._scheduler && this._scheduler.id) {
                filter.lockedBy = this._scheduler.id;
            }
            if (this.config.status !== 'PENDING') {
                filter.status = {$nin: ['PENDING', 'CANCELED']};
            } else {
                filter.status = {$ne: 'CANCELED'};
            }
            if (!isEmpty(update)) {
                await this._operations.updateJobs(filter, update);
            }
            const config = await this._operations.jobs.findOne({_id: this.config._id});
            const {local, lockedBy, context, trials, timeCreated, timeStarted, timeStopped, fail, ...up} = config;
            if (this._scheduler && (!this._scheduler.isActive || !this._scheduler.id.equals(lockedBy))) {
                this._interrupted = true;
            }
            if (up.status === 'CANCELED') this._canceled = true;
            if (up.status === 'PENDING' && this.config.status !== 'PENDING') this._interrupted = true;
            this.config = {...this.config, ...update, ...up};
        } else {
            this.config = {...this.config, ...update};
        }
    }

    _checkStatusChange() {
        if (this._scheduler && !this._scheduler.isActive) this._interrupted = true;
        if (this._interrupted) {
            this._logger.interrupt('Job is interrupted due to possibly system shutdown.');
            this._jobLogger.interrupt('_interrupt', 'Job is interrupted due to possibly system shutdown.');
        }
        if (this._canceled) {
            this._logger.cancel('Job is canceled due to manual intervention or task being disabled.');
            this._jobLogger.cancel('_cancel', 'Job is canceled due to manual intervention or task being disabled.');
        }
        if (this._timeout) {
            this._logger.timeout('Job execution time exceeds ', this.config.timeout, ' seconds and is terminated.');
            this._jobLogger.timeout('_timeout', 'Job execution time exceeds ', this.config.timeout, ' seconds and is terminated.');
        }
    }

    get _isActive() {
        return !this._done && !this._interrupted && !this._canceled && !this._timeout;
    }

    async _syncStatus() {
        while (this._isActive) {
            const sleepInterval = this._scheduler && this._scheduler.options.heartbeat * 1000 || 1000;
            await sleep(sleepInterval);
            if (!this._isActive) break;
            try {
                if (this.config._id && this._operations) {
                    await this._update({}, {includeContext: true});
                    if (!this._isActive) break;
                }
                if (!this._timeout && this.config.timeout > 0 && this.config.trials.length > 0) {
                    const trial = this.config.trials[this.config.trials.length - 1];
                    if (trial.timeStarted < Date.now() - this.config.timeout * 1000) {
                        this._timeout = true;
                    }
                }
                if (this._operations) {
                    for await (const {local, ...job} of this._operations.jobs.find(
                        {_id: {$in: this._jobWatcher.watchingJobIds}, status: {$in: ['SUCCESS', 'FAILED', 'CANCELED']}}
                    )) {
                        this._jobWatcher.watched(job._id, job);
                    }
                    if (!this._isActive) break;
                }
            } catch (e) {
                this._logger.warn('Job status sync failed: ', e);
            }
        }
        try {
            this._checkStatusChange();
        } catch (e) {
            this._jobWatcher.abortAll(e);
        }
    }

    async _execute() {
        this._syncStatus();
        try {
            for (let trial = this.config.trials.length; trial <= this.config.retry; trial++) {
                this._timeout = false;
                this.config.trials.push({});
                await this._executeTrial();
                if (this.config.status === 'SUCCESS') {
                    break;
                }
            }
        } finally {
            this._done = true;
        }
    }

    async _executeTrial() {
        try {
            const trial = this.config.trials.length - 1;
            await this._load();
            this._checkStatusChange();
            if (trial > 0) {
                this._logger.retry(trial, ' / ', this.config.retry);
            }
            await this._delay({}, {trial, updateStatus: true});
            await this._updateTrial({status: 'RUNNING'});
            this._checkStatusChange();
            this._logger.start('Job starts.');
            await this._withCatchable(this._taskType.run.bind(
                this._asAgent(), readOnly(this.config.params), this.config.context, this._taskType.store
            ));
            await this._updateTrial({status: 'SUCCESS'}, {includeContext: true});
            this._checkStatusChange();
            this._logger.complete('Job completes successfully.');
        } catch (e) {
            await this._checkEarlyExit(e);
            if (e instanceof CatchableError) {
                try {
                    let catchMessage = await this._withCatchable(this._taskType.catch.bind(
                        this._asAgent(), e.error, readOnly(this.config.params), this.config.context, this._taskType.store
                    ));
                    if (catchMessage) {
                        if (!Array.isArray(catchMessage)) {
                            catchMessage = [catchMessage];
                        }
                        e = new JobRuntime('_catch', ...catchMessage);
                        this._logger.catch(...catchMessage);
                    } else {
                        this._logger.crash('In run(): ', e.error);
                    }
                } catch (ee) {
                    await this._checkEarlyExit(ee);
                    if (!(ee instanceof JobRuntimeError)) {
                        this._logger.crash('In catch(): ', ee);
                    }
                    e = ee;
                }
            }
            if (e instanceof JobRuntimeError) {
                if (!(e instanceof JobTimeout)) {
                    this._logger.fail(e);
                }
                await this._updateTrial(
                    {status: 'FAILED', code: e.code || '_fail', message: e.message},
                    {includeContext: true}
                );
            } else {
                if (e instanceof JobCrash) {
                    this._logger.crash(e);
                }
                const err = e instanceof CatchableError ? e.error : e;
                await this._updateTrial(
                    {status: 'FAILED', code: err.code || '_crash', message: err.message, timeStopped: new Date()},
                    {includeContext: true}
                );
                throw e;
            }
        } finally {
            this._jobWatcher.closeAll();
            await this._unload();
        }
    }

    async _checkEarlyExit(e) {
        if (e instanceof JobEarlyExit) {
            if (e instanceof JobCancellation) {
                await this._updateTrial(
                    {status: 'CANCELED', code: e.code || '_cancel', message: e.message},
                    {includeContext: true}
                );
            }
            throw e;
        }
    }

    async _withCatchable(fn) {
        try {
            return await fn();
        } catch (e) {
            if (!(e instanceof CatalystError)) {
                e = new CatchableError(e);
            }
            throw e;
        }
    }

}


class JobWatcher {

    constructor(jobs, defaults = {}) {
        this.jobs = jobs;
        this.defaults = defaults;
        this._watches = {};
    }

    get watchingJobIds() {
        return Object.keys(this._watches).map(i => ObjectID(i));
    }
    
    isWatching(jobId) {
        return this._watches[jobId] != null;
    }

    async watch(jobId, options = {}) {
        return new Promise((resolve, reject) => {
            this._watches[jobId] = {resolve, reject};
            this._watch(jobId, options);
        });
    }

    _watch(jobId, opts = {}) {
        const {pipeline, options, fallback} = {...this.defaults, ...opts};
        if (this._watches[jobId] && this.jobs) {
            debug('JobWatcher watching job %s to stop', jobId);
            const watch = this.jobs.watch(
                ensureThunkSync(pipeline, jobId),
                ensureThunkSync(options, jobId),
            );
            watch
                .on('change', (event) => {
                    const {local, ...job} = event.fullDocument;
                    this.watched(jobId, job);
                })
                .on('error', (error) => {
                    debug('JobWatcher encounters an error watching job %s to stop: %s', jobId, error);
                    this._close(jobId);
                    if (error.name === 'MongoError' && error.code === 40573) {
                        if (fallback) {
                            debug('JobWatcher stops watching job %s and use fallback method', jobId);
                            this.watched(jobId, fallback(jobId));
                        }
                    } else {
                        this._watch(jobId, opts);
                    }
                });
            this._watches[jobId].watch = watch;
        }
    }

    _close(jobId) {
        const {watch} = this._watches[jobId] || {};
        if (watch) {
            watch.close();
            delete this._watches[jobId].watch;
        }

    }

    watched(jobId, value) {
        debug('JobWatcher watches job %s has stopped', jobId);
        const {resolve, watch} = this._watches[jobId] || {};
        if (resolve) resolve(value);
        if (watch) watch.close();
        delete this._watches[jobId];
    }

    abortAll(error) {
        debug('JobWatcher aborts watching for jobs: %s', this.watchingJobIds.join(', '));
        for (const {reject} of Object.values(this._watches)) {
            if (reject) reject(error);
        }
        this.closeAll();
    }

    closeAll() {
        debug('JobWatcher closes watching for jobs: %s', this.watchingJobIds.join(', '));
        for (const {watch} of Object.values(this._watches)) {
            if (watch) watch.close();
        }
        this._watches = {};
    }
}


module.exports = {
    TaskDomain,
    TaskType,
    Job,
    JobWatcher,
};
