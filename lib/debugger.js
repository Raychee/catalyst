const {resolve} = require('path');

const {MongoClient} = require('mongodb');

const {Logger} = require('./logger');
const {TaskLoader, PluginLoader} = require('./loader');
const {TaskDomain, TaskType, Job} = require('./task');
const {DEFAULT_TASK_DOMAIN_CONFIG, COLLECTION_NAMES} = require('./config');
const {SystemError} = require('./error');


module.exports = class {

    constructor(options) {
        this.options = options || {};
        this.options.name = this.options.name || 'Catalyst';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new Error('db.host must be provided');
        this.options.db.port = this.options.db.port || 27017;
        this.options.db.user = this.options.db.user || '';
        this.options.db.password = this.options.db.password || '';
        if (!this.options.db.db) throw new SystemError('db.db must be provided');
        this.options.tasks = this.options.tasks || {};
        this.options.tasks.paths = this.options.tasks.paths || [];
        this.options.plugins = this.options.plugins || {};
        this.options.plugins.paths = (this.options.plugins.paths || []).map(p =>
            p.replace(/^\/\//, resolve(__dirname, 'plugins') + '/')
        );
        this.options.plugins.defaults = this.options.plugins.defaults || {};
        this.options.plugins.config = this.options.plugins.config || {};
        this.options.debugger = this.options.debugger || {};
        this.options.debugger.jobs = this.options.debugger.jobs || [];
        this.options.debugger.concurrent = this.options.debugger.concurrent || false;
        this.options.debugger.exitOnComplete = this.options.debugger.exitOnComplete || false;
        this.options.logging = this.options.logging || {};
        this.options.logging.level = this.options.logging.level || 'INFO';
        this.options.logging.showTimestamp = this.options.logging.showTimestamp || false;

        /** @type {MongoClient} */
        this.mongodb = undefined;
        /** @type {PluginLoader} */
        this.pluginLoader = undefined;
        /** @type {TaskLoader} */
        this.taskLoader = undefined;

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;

        this.starting = undefined;
        this.stopping = undefined;
        this.exitHandler = undefined;
    }

    async start(options) {
        if (this.stopping) {
            await this.stopping;
            this.stopping = undefined;
        }
        if (!this.starting) {
            this.starting = this._start(options);
        }
        await this.starting;
        this.starting = undefined;
    }

    async stop(options) {
        if (this.starting) {
            await this.starting;
            this.starting = undefined;
        }
        if (!this.stopping) {
            this.stopping = this._stop(options);
        }
        await this.stopping;
        this.stopping = undefined;
    }

    async _start({verbose = true} = {}) {
        if (this.mongodb) return;
        const logger = new Logger({level: verbose ? 'INFO' : 'ERROR'});
        logger.println(this.options.name, ' (debugger) is starting up.');

        if (this.exitHandler) {
            process.off('SIGTERM', this.exitHandler);
            process.off('SIGINT' , this.exitHandler);
            this.exitHandler = undefined;
        }
        this.exitHandler = this.stop.bind(this, {verbose, exit: true});
        process.on('SIGTERM', this.exitHandler);
        process.on('SIGINT' , this.exitHandler);

        const {host, port, user, password, db} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        this.mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, {useNewUrlParser: true, useUnifiedTopology: true}
        );

        logger.print('Connecting to database... ');
        await this.mongodb.connect();
        logger.reprintln('Database is connected.');

        const storeCollection = this.mongodb.db(db).collection(COLLECTION_NAMES.Store);

        logger.print('Loading plugins... ');
        const {paths: pluginPaths, defaults, config} = this.options.plugins;
        this.pluginLoader = new PluginLoader(pluginPaths, storeCollection, defaults, config);
        await this.pluginLoader.load();
        logger.reprintln('Plugins are loaded.');

        this.taskLoader = new TaskLoader(this.options.tasks.paths, this.pluginLoader);
        await this.taskLoader.load({verbose});

        logger.println(this.options.name, ' (debugger) is started.');

        if (this.options.debugger.jobs.length > 0) {
            if (this.options.debugger.concurrent) {
                const promises = this.options.debugger.jobs.map(
                    job => this.run(job)
                        .catch(e => logger.error('Job ', job, ' encounters an error: ', e))
                );
                await Promise.all(promises);
            } else {
                for (const job of this.options.debugger.jobs) {
                    await this.run(job)
                        .catch(e => logger.error('Job ', job, ' encounters an error: ', e))
                }
            }
            if (this.options.debugger.exitOnComplete) {
                await this.stop({verbose, exit: true});
            }
        }
    }

    async run(jobConfig, taskTypeSpec) {
        jobConfig = {...DEFAULT_TASK_DOMAIN_CONFIG, ...jobConfig};
        let taskType;
        if (taskTypeSpec) {
            const domain = new TaskDomain('', this.pluginLoader);
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
        const job = new Job(jobConfig, taskType);
        await job._execute();
    }

    async _stop({verbose = true, exit = false} = {}) {
        const logger = new Logger({level: verbose ? 'INFO' : 'ERROR'});
        const pluginLoader = this.pluginLoader;
        const mongodb = this.mongodb;
        this.taskLoader = undefined;
        this.pluginLoader = undefined;
        this.mongodb = undefined;
        logger.println(this.options.name, ' (debugger) is shutting down.');
        if (pluginLoader) {
            logger.print('Unloading plugins... ');
            await pluginLoader.unload();
            logger.reprintln('Plugins are unloaded.');
        }
        logger.print('Closing connections to database... ');
        await mongodb.close();
        logger.reprintln('Database connections are closed.');
        logger.println(this.options.name, ' (debugger) is stopped.');

        if (exit) {
            logger.println(this.options.name, ' (debugger) exits.');
            process.exit(0);
        }
    }

};