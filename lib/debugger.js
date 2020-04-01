const {resolve, sep} = require('path');

const {MongoClient, ObjectID} = require('mongodb');
const {Runnable, dedup} = require('@raychee/utils');

const {Logger} = require('./logger');
const {TaskLoader, PluginLoader} = require('./loader');
const {TaskDomain, TaskType, Job} = require('./task');
const {DEFAULT_TASK_DOMAIN_CONFIG, COLLECTION_NAMES} = require('./config');
const {OperationError, SystemError} = require('./error');


module.exports = class extends Runnable {

    constructor(options) {
        super();

        this.options = options || {};
        this.options.name = this.options.name || 'Catalyst';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new Error('db.host must be provided');
        this.options.db.port = this.options.db.port || 27017;
        this.options.db.user = this.options.db.user || '';
        this.options.db.password = this.options.db.password || '';
        if (!this.options.db.db) throw new SystemError('db.db must be provided');
        this.options.tasks = this.options.tasks || {};
        this.options.tasks.paths = (this.options.tasks.paths || []).map(
            p => p.replace(/[\\/]/g, sep)
        );
        this.options.plugins = this.options.plugins || {};
        this.options.plugins.paths = (this.options.plugins.paths || []).map(
            p => p.replace(/^\/\//, resolve(__dirname, 'plugins') + sep)
                .replace(/[\\/]/g, sep)
        );
        this.options.plugins.defaults = this.options.plugins.defaults || {};
        this.options.plugins.config = this.options.plugins.config || {};
        this.options.debugger = this.options.debugger || {};
        this.options.debugger.jobs = this.options.debugger.jobs || [];
        this.options.debugger.concurrency = this.options.debugger.concurrency || 1;
        this.options.debugger.includeScheduledJobs = this.options.debugger.includeScheduledJobs || false;
        this.options.debugger.exitOnComplete = this.options.debugger.exitOnComplete || false;
        this.options.logging = this.options.logging || {};
        this.options.logging.level = this.options.logging.level || 'INFO';
        this.options.logging.showTimestamp = this.options.logging.showTimestamp || false;

        this.logger = new Logger();
        /** @type {PluginLoader} */
        this.pluginLoader = undefined;
        /** @type {TaskLoader} */
        this.taskLoader = undefined;

        this._stop = false;

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async start(options = {}) {
        return super.start({waitForReady: true, ...options});
    }

    async run({signal}) {
        this.logger.println(this.options.name, ' (debugger) is starting up.');

        const exitHandler = this.stop.bind(this, {exit: true});
        process.once('SIGTERM', exitHandler);
        process.once('SIGINT', exitHandler);

        const {host, port, user, password, db} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        const mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, {useNewUrlParser: true, useUnifiedTopology: true}
        );

        this.logger.print('Connecting to database... ');
        await mongodb.connect();
        this.logger.reprintln('Database is connected.');

        const storeCollection = mongodb.db(db).collection(COLLECTION_NAMES.Store);

        this.logger.print('Loading plugins... ');
        const {paths: pluginPaths, defaults, config} = this.options.plugins;
        this.pluginLoader = new PluginLoader(pluginPaths, storeCollection, defaults, config);
        await this.pluginLoader.load();
        this.logger.reprintln('Plugins are loaded.');

        this.logger.print('Loading task domains and types... ');
        this.taskLoader = new TaskLoader(this.options.tasks.paths, this.pluginLoader);
        await this.taskLoader.load();
        this.logger.reprintln('Task domains and types are loaded.');

        this.logger.println(this.options.name, ' (debugger) is started.');

        let exit = false, exitImmediately = false;
        signal.then(() => this._stop = true);

        if (this.options.debugger.jobs.length > 0) {
            await this.execute(this.options.debugger.jobs);
            if (this.options.debugger.exitOnComplete) {
                exitImmediately = true;
                exit = true;
            }
        }

        if (!exitImmediately) {
            const {exit: e} = await signal;
            exit = e;
        }

        this.logger.println(this.options.name, ' (debugger) is shutting down.');

        this.logger.print('Unloading plugins... ');
        await this.pluginLoader.unload();
        this.logger.reprintln('Plugins are unloaded.');

        this.logger.print('Closing connections to database... ');
        await mongodb.close();
        this.logger.reprintln('Database connections are closed.');

        this.logger.println(this.options.name, ' (debugger) is stopped.');

        this.taskLoader = undefined;
        this.pluginLoader = undefined;

        if (exit) {
            this.logger.println(this.options.name, ' (debugger) exits.');
            process.exit(0);
        }

    }

    async execute(jobs, taskTypes = {}) {
        if (!Array.isArray(jobs)) {
            jobs = [jobs];
        }
        const pending = [...jobs], running = [], succeeded = [], failed = [];
        await new Promise(resolve => {
            const spawnJob = async (job_) => {
                const job = await this.spawnJob(job_, taskTypes[`${job_.domain}.${job_.type}`]);
                job.on('scheduleJob', (scheduled) => {
                    if (this.options.debugger.includeScheduledJobs) {
                        pending.push(scheduled);
                        enqueue();
                    } else {
                        succeeded.push({config: {...scheduled, status: 'SUCCESS'}});
                    }
                });
                job.on('waitForJob', (jobId) => {
                    for (const j of [...succeeded, ...failed]) {
                        if (j.config._id.toString() === jobId) {
                            const resolve = job._waitingForJobs[jobId];
                            if (resolve) resolve(j.config);
                        }
                    }
                });
                running.push({
                    job,
                    promise: job._execute()
                        .then(() => succeeded.push(job))
                        .catch(e => {
                            failed.push(job);
                            const {domain, type, params, context} = job.config;
                            this.logger.error(
                                'Job ', {domain, type, params, context}, ' encounters an error: ', e
                            );
                        })
                        .finally(() => {
                            const jobId = job.config._id.toString();
                            const index = running.findIndex(j => j.job.config._id.toString() === jobId);
                            if (index >= 0) {
                                running.splice(index, 1);
                            }
                            for (const {job: j} of running) {
                                const resolve = j._waitingForJobs[jobId];
                                if (resolve) resolve(job.config);
                            }
                            enqueue();
                        })
                });
            };
            const enqueue = dedup(async () => {
                if (this._stop || (pending.length <= 0 && running.length <= 0)) {
                    resolve();
                } else {
                    while (pending.length > 0 && running.length < this.options.debugger.concurrency) {
                        const job_ = pending.shift();
                        await spawnJob(job_);
                    }
                }
                this.logger.info(
                    this.options.name, ' (debugger) progress: ',
                    pending.length, ' pending / ',
                    running.length, ' running / ',
                    succeeded.length, ' succeeded / ',
                    failed.length, ' failed.'
                );
            });
            enqueue();
        });
        if (running.length > 0) {
            await Promise.all(running.map(r => r.promise));
        }
    }

    async spawn(jobConfig, taskTypeSpec) {
        const job = await this.spawnJob(jobConfig, taskTypeSpec);
        await job._execute();
        return job;
    }

    async spawnJob(jobConfig, taskTypeSpec) {
        jobConfig = {
            _id: new ObjectID(), params: {}, context: {},
            status: 'DELAYED', trials: [], timeCreated: new Date(),
            ...DEFAULT_TASK_DOMAIN_CONFIG,
            ...jobConfig
        };
        let taskType;
        if (taskTypeSpec) {
            const domain = new TaskDomain(jobConfig.domain || '', this.pluginLoader);
            await domain.load({});
            taskType = new TaskType(domain, jobConfig.type || '', this.pluginLoader);
            await taskType.load(taskTypeSpec);
        } else {
            const {domain, type} = jobConfig;
            taskType = await this.taskLoader.getTaskType(domain, type);
            if (!taskType) {
                throw new SystemError('Task type "', domain, '.', type, '" is not valid.');
            }
        }
        let paramsInvalid = taskType.validate(jobConfig.params || {});
        if (paramsInvalid) {
            if (!Array.isArray(paramsInvalid)) paramsInvalid = [paramsInvalid];
            throw new OperationError(...paramsInvalid);
        }
        const job = new Job(jobConfig, taskType);
        process.once('SIGTERM', () => job._interrupted = true);
        process.once('SIGINT', () => job._interrupted = true);
        return job;
    }

};