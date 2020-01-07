const {resolve} = require('path');

const {MongoClient} = require('mongodb');
const {Runnable} = require('@raychee/utils');

const {Logger} = require('./logger');
const {TaskLoader, PluginLoader} = require('./loader');
const {TaskDomain, TaskType, Job} = require('./task');
const {DEFAULT_TASK_DOMAIN_CONFIG, COLLECTION_NAMES} = require('./config');
const {SystemError} = require('./error');


module.exports = class extends Runnable{

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

        this.logger = new Logger();
        /** @type {PluginLoader} */
        this.pluginLoader = undefined;
        /** @type {TaskLoader} */
        this.taskLoader = undefined;

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async run({signal}) {
        this.logger.println(this.options.name, ' (debugger) is starting up.');

        const exitHandler = this.stop.bind(this, {exit: true});
        process.on('SIGTERM', exitHandler);
        process.on('SIGINT', exitHandler);

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

        if (this.options.debugger.jobs.length > 0) {
            if (this.options.debugger.concurrent) {
                await Promise.all(this.options.debugger.jobs.map(
                    job => {
                        const {domain, type, params, context} = job;
                        return this.spawn(job)
                            .catch(e => this.logger.error(
                                'Job ', {domain, type, params, context}, ' encounters an error: ', e
                            ));
                    }
                ));
            } else {
                for (const job of this.options.debugger.jobs) {
                    const {domain, type, params, context} = job;
                    await this.spawn(job)
                        .catch(e => this.logger.error(
                            'Job ', {domain, type, params, context}, ' encounters an error: ', e
                        ));
                }
            }
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
        process.off('SIGTERM', exitHandler);
        process.off('SIGINT', exitHandler);

        if (exit) {
            this.logger.println(this.options.name, ' (debugger) exits.');
            process.exit(0);
        }

    }

    async spawn(jobConfig, taskTypeSpec) {
        jobConfig = {
            params: {}, context: {},
            status: 'DELAYED', trials: [], timeCreated: new Date(),
            ...DEFAULT_TASK_DOMAIN_CONFIG,
            ...jobConfig
        };
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

};