const {resolve} = require('path');

const {MongoClient} = require('mongodb');

const Agenda = require('agenda');

const {sleep} = require('@raychee/utils');
const {INDEXES, DEFAULT_JOB_HEARTBEAT, DEFAULT_JOB_HEART_ATTACK} = require('./config');
const {Logger} = require('./logger');
const {TaskLoader, PluginLoader} = require('./loader');
const {JobContextCache} = require('./task');
const Operations = require('./operations');


module.exports = class {

    constructor(options) {
        this.options = options || {};
        this.options.name = this.options.name || 'Catalyst';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new Error('db.host must be provided');
        this.options.db.port = this.options.db.port || 27017;
        this.options.db.user = this.options.db.user || '';
        this.options.db.password = this.options.db.password || '';
        this.options.db.agenda = this.options.db.agenda || 'agenda';
        this.options.db.service = this.options.db.service || 'service';
        this.options.db.collections = this.options.db.collections || {};
        this.options.db.collections.store = this.options.db.collections.store || 'Store';
        this.options.tasks = this.options.tasks || {};
        this.options.tasks.paths = this.options.tasks.paths || [];
        this.options.tasks.heartbeat = this.options.tasks.heartbeat || DEFAULT_JOB_HEARTBEAT;
        this.options.tasks.heartAttack = this.options.tasks.heartAttack || DEFAULT_JOB_HEART_ATTACK;
        this.options.plugins = this.options.plugins || {};
        this.options.plugins.paths = (this.options.plugins.paths || []).map(p =>
            p.replace(/^\/\//, resolve(__dirname, 'plugins') + '/')
        );
        this.options.plugins.defaults = this.options.plugins.defaults || {};
        this.options.plugins.config = this.options.plugins.config || {};
        this.options.daemon = this.options.daemon || {};
        this.options.logging = this.options.logging || {};
        this.options.logging.level = this.options.logging.level || 'INFO';
        this.options.logging.showTimestamp = this.options.logging.showTimestamp || false;

        /** @type {TaskLoader} */
        this.taskLoader = undefined;
        /** @type {PluginLoader} */
        this.pluginLoader = undefined;
        this.mongodb = undefined;

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async start() {
        if (this.taskLoader) {
            return;
        }
        process.stdout.write(`${this.options.name} (daemon) is starting up.\n`);

        const {host, port, user, password, agenda: agenda_, service, collections: {store}} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        this.mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, {useNewUrlParser: true, useUnifiedTopology: true}
        );

        process.stdout.write('Connecting to database... ');
        await this.mongodb.connect();
        process.stdout.write('\rConnecting to database... Done.\n');

        const storeCollection = this.mongodb.db(service).collection(store);

        process.stdout.write('Loading plugins... ');
        const {paths: pluginPaths, defaults, config} = this.options.plugins;
        this.pluginLoader = new PluginLoader(pluginPaths, storeCollection, defaults, config);
        await this.pluginLoader.load();
        process.stdout.write('\rLoading plugins... Done.\n');

        const {paths: taskPaths, ...taskOptions} = this.options.tasks;
        const jobContextCache = new JobContextCache();
        const operations = new Operations(this.mongodb.db(service), undefined, jobContextCache, taskOptions);
        this.taskLoader = new TaskLoader(taskPaths, operations, this.pluginLoader, storeCollection, jobContextCache,
            (name) =>
                new Agenda({sort: {priority: -1, nextRunAt: 1}})
                    .name(name)
                    .mongo(this.mongodb.db(agenda_), name),
            '_task',
        );
        operations.taskLoader = this.taskLoader;
        await this.taskLoader.load({verbose: true});

        process.stdout.write('Ensuring database indexes... ');
        for (const agenda of Object.values(await this.taskLoader.getAllAgendas())) {
            await this.mongodb.db(agenda_).collection(agenda._name).createIndexes(INDEXES.agenda);
        }
        for (const [collectionName, indexes] of Object.entries(INDEXES)) {
            if (collectionName === 'agenda') continue;
            await this.mongodb.db(service).collection(collectionName).createIndexes(indexes);
        }
        process.stdout.write('\rEnsuring database indexes... Done.\n');

        await this.taskLoader.start({verbose: true});

        process.stdout.write(`${this.options.name} (daemon) is started.\n`);

        process.on('SIGTERM', this.stop.bind(this, {exit: true}));
        process.on('SIGINT' , this.stop.bind(this, {exit: true}));
    }

    async stop({exit = false} = {}) {
        if (!this.taskLoader) {
            return;
        }
        const taskLoader = this.taskLoader;
        const pluginLoader = this.pluginLoader;
        const mongodb = this.mongodb;
        this.taskLoader = undefined;
        this.pluginLoader = undefined;
        this.mongodb = undefined;
        process.stdout.write(`${this.options.name} (daemon) is shutting down.\n`);
        await taskLoader.stop({verbose: true});
        process.stdout.write('Unloading plugins... ');
        await pluginLoader.unload();
        process.stdout.write('\rUnloading plugins... Done.\n');
        process.stdout.write('Closing connections to database... ');
        await mongodb.close();
        process.stdout.write('\rClosing connections to database... Done.\n');
        process.stdout.write(`${this.options.name} (daemon) is stopped.\n`);

        if (exit) {
            process.stdout.write(`${this.options.name} (daemon) exits.\n`);
            process.exit(0);
        }
    }
};
