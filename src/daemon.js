const {resolve} = require('path');

const {MongoClient} = require('mongodb');

const Agenda = require('agenda');

const {INDEXES} = require('./config');
const {TaskLoader, PluginLoader} = require('./loader');
const {JobContextCache} = require('./task');
const Operations = require('./operations');
const {sleep} = require('./utils');


module.exports = class {
    constructor(options) {
        this.options = options || {};
        this.options.name = this.options.name || 'Service';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new Error('db.host must be provided');
        this.options.db.port = this.options.db.port || 27017;
        this.options.db.user = this.options.db.user || '';
        this.options.db.password = this.options.db.password || '';
        this.options.db.agenda = this.options.db.agenda || 'agenda';
        this.options.db.service = this.options.db.service || 'service';
        this.options.db.collections = this.options.db.collections || {};
        this.options.db.collections.store = this.options.db.collections.store || 'Store';
        this.options.tasks = this.options.tasks || [];
        this.options.plugins = this.options.plugins || {};
        this.options.plugins.paths = (this.options.plugins.paths || []).map(p => p === 'built-in' ? resolve(__dirname, 'plugins') : p);
        this.options.plugins.defaults = this.options.plugins.defaults || {};
        this.options.plugins.config = this.options.plugins.config || {};
        this.options.daemon = this.options.daemon || {};
        this.options.daemon.waitBeforeStop = this.options.daemon.waitBeforeStop || 0;

        this.taskLoader = undefined;
        this.mongodb = undefined;
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
        const {paths, defaults, config} = this.options.plugins;
        const pluginLoader = new PluginLoader(paths, storeCollection, defaults, config);
        await pluginLoader.load();
        process.stdout.write('\rLoading plugins... Done.\n');

        const jobContextCache = new JobContextCache();
        const operations = new Operations(this.mongodb.db(service), undefined, jobContextCache);
        this.taskLoader = new TaskLoader(this.options.tasks, operations, pluginLoader, storeCollection, jobContextCache,
            (name) => {
                new Agenda({sort: {priority: -1, nextRunAt: 1}})
                    .name(name)
                    .mongo(this.mongodb.db(agenda_), name);
            },
            '_task',
        );
        operations.taskLoader = this.taskLoader;
        process.stdout.write('Loading task types... ');
        await this.taskLoader.load({syncConfigsPeriodically: true});
        process.stdout.write('\rLoading task types... Done.\n');

        process.stdout.write('Ensuring database indexes... ');
        for (const agenda of Object.values(await this.taskLoader.getAllAgendas())) {
            await this.mongodb.db(agenda_).collection(agenda._name).createIndexes(INDEXES.agenda);
        }
        for (const [collectionName, indexes] of Object.entries(INDEXES)) {
            if (collectionName === 'agenda') continue;
            await this.mongodb.db(service).collection(collectionName).createIndexes(indexes);
        }
        process.stdout.write('\rEnsuring database indexes... Done.\n');

        await this.taskLoader.start();

        process.stdout.write(`${this.options.name} (daemon) is started.\n`);

        process.on('SIGTERM', this.stop.bind(this));
        process.on('SIGINT' , this.stop.bind(this));
    }

    async stop() {
        if (!this.taskLoader) {
            return;
        }
        const taskLoader = this.taskLoader;
        const mongodb = this.mongodb;
        this.taskLoader = undefined;
        this.mongodb = undefined;
        process.stdout.write(`${this.options.name} (daemon) is shutting down.\n`);
        await taskLoader.stop();
        if (this.options.daemon.waitBeforeStop > 0) {
            process.stdout.write(`Wait ${this.options.daemon.waitBeforeStop} secs for running jobs to stop. \n`);
            await sleep(this.options.daemon.waitBeforeStop * 1000);
        }
        process.stdout.write('Closing connections to database... ');
        await mongodb.close();
        process.stdout.write('\rClosing connections to database... Done.\n');
        process.stdout.write(`${this.options.name} (daemon) exits.\n`);
    }
};
