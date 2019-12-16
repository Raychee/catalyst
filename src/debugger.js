const {resolve} = require('path');

const {MongoClient} = require('mongodb');

const {stringify} = require('@raychee/utils');
const {Logger} = require('./logger');
const {TaskLoader, PluginLoader} = require('./loader');
const {TaskDomain, TaskType} = require('./task');
const {DEFAULT_TASK_DOMAIN_CONFIG} = require('./config');


module.exports = class {

    constructor(options) {
        this.options = options || {};
        this.options.name = this.options.name || 'Catalyst';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new Error('db.host must be provided');
        this.options.db.port = this.options.db.port || 27017;
        this.options.db.user = this.options.db.user || '';
        this.options.db.password = this.options.db.password || '';
        this.options.db.collections = this.options.db.collections || {};
        this.options.db.collections.store = this.options.db.collections.store || 'Store';
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

        this.taskLoader = undefined;
        this.pluginLoader = undefined;

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async start() {
        if (this.taskLoader) {
            return;
        }
        process.stdout.write(`${this.options.name} (debugger) is starting up.\n`);

        const {host, port, user, password, service, collections: {store}} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        this.mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, {useNewUrlParser: true, useUnifiedTopology: true}
        );

        process.stdout.write('Connecting to database... ');
        await this.mongodb.connect();
        process.stdout.write('\rConnecting to database... Done.\n');

        const storeCollection = this.mongodb.db(service).collection(store);

        process.stdout.write('Loading plugins... ');
        const {paths, defaults, config} = this.options.plugins;
        this.pluginLoader = new PluginLoader(paths, storeCollection, defaults, config);
        await this.pluginLoader.load();
        process.stdout.write('\rLoading plugins... Done.\n');

        this.taskLoader = new TaskLoader(this.options.tasks.paths, undefined, this.pluginLoader, storeCollection);
        process.stdout.write('Loading task types... ');
        await this.taskLoader.load();
        process.stdout.write('\rLoading task types... Done.\n');

        process.stdout.write(`${this.options.name} (debugger) is started.\n`);

        process.on('SIGTERM', this.stop.bind(this, {exit: true}));
        process.on('SIGINT' , this.stop.bind(this, {exit: true}));

        if (this.options.debugger.jobs.length > 0) {
            if (this.options.debugger.concurrent) {
                const promises = this.options.debugger.jobs.map(
                    job => this.run(job)
                        .catch(e => console.error(stringify('Job ', job, ' returns an error: ', e)))
                );
                await Promise.all(promises);
            } else {
                for (const job of this.options.debugger.jobs) {
                    await this.run(job)
                        .catch(e => console.error(stringify('Job ', job, ' returns an error: ', e)))
                }
            }
            if (this.options.debugger.exitOnComplete) {
                await this.stop({exit: true});
            }
        }
    }

    async run(job, taskTypeSpec) {
        let taskType;
        if (taskTypeSpec) {
            const domain = new TaskDomain('', this.pluginLoader, this.taskLoader.storeCollection);
            await domain.load({});
            taskType = new TaskType(domain, '', undefined, this.pluginLoader, this.taskLoader.storeCollection);
            await taskType.load(taskTypeSpec);
            job = {...DEFAULT_TASK_DOMAIN_CONFIG, job};
        } else {
            const {domain, type} = job;
            taskType = await this.taskLoader.get([domain, type]);
            if (!taskType) {
                throw new Error(`Task type "${domain}.${type}" is not valid.`);
            }
        }
        await taskType.toAgendaJobFn()(job);
    }

    async stop({exit = false} = {}) {
        if (!this.taskLoader) {
            return;
        }
        const mongodb = this.mongodb;
        const pluginLoader = this.pluginLoader;
        this.taskLoader = undefined;
        this.pluginLoader = undefined;
        this.mongodb = undefined;
        process.stdout.write(`${this.options.name} (debugger) is shutting down.\n`);
        process.stdout.write('Stopping and unloading components... ');
        await pluginLoader.unload();
        process.stdout.write('\rStopping and unloading components... Done.\n');
        process.stdout.write('Closing connections to database... ');
        await mongodb.close();
        process.stdout.write('\rClosing connections to database... Done.\n');
        process.stdout.write(`${this.options.name} (debugger) is stopped.\n`);

        if (exit) {
            process.stdout.write(`${this.options.name} (debugger) exits.\n`);
            process.exit(0);
        }
    }

};