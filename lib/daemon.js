const {resolve} = require('path');

const {MongoClient} = require('mongodb');
const {Runnable} = require('@raychee/utils');

const {DEFAULT_HEARTBEAT, DEFAULT_HEART_ATTACK, COLLECTION_NAMES} = require('./config');
const {Logger} = require('./logger');
const {TaskLoader, PluginLoader} = require('./loader');
const Operations = require('./operations');
const Scheduler = require('./scheduler');
const {SystemError} = require('./error');


// TODO: _locked -> , _full -> local
// TODO: remove task & job's id -> just use _id
// TODO: convert job's task / createdFrom / ...  -> ObjectId
// TODO: add db init for configs
// TODO: rename TaskDomainConfig -> Domain, TaskTypeConfig -> Type

module.exports = class extends Runnable {

    constructor(options) {
        super();

        this.options = options || {};
        this.options.name = this.options.name || 'Catalyst';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new SystemError('db.host must be provided');
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
        this.options.daemon = this.options.daemon || {};
        this.options.daemon.heartbeat = this.options.daemon.heartbeat || DEFAULT_HEARTBEAT;
        this.options.daemon.heartAttack = this.options.daemon.heartAttack || DEFAULT_HEART_ATTACK;
        this.options.logging = this.options.logging || {};
        this.options.logging.level = this.options.logging.level || 'INFO';
        this.options.logging.showTimestamp = this.options.logging.showTimestamp || false;

        this.logger = new Logger();

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async run({signal}) {
        this.logger.println(this.options.name, ' (daemon) is starting up.');

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
        const pluginLoader = new PluginLoader(pluginPaths, storeCollection, defaults, config);
        await pluginLoader.load();
        this.logger.reprintln('Plugins are loaded.');

        this.logger.print('Loading task domains and types... ');
        const taskLoader = new TaskLoader(this.options.tasks.paths, pluginLoader);
        await taskLoader.load();
        this.logger.reprintln('Task domains and types are loaded.');

        this.logger.print('Preparing database... ');
        const operations = new Operations(
            new Logger({category: 'Operations'}), mongodb.db(db), taskLoader
        );
        await operations.prepare();
        this.logger.reprintln('Database is prepared.');

        this.logger.print('Starting scheduler...');
        const scheduler = new Scheduler(
            new Logger({category: 'Scheduler'}),
            operations, taskLoader, this.options.daemon
        );
        await scheduler.start();
        this.logger.reprintln('Scheduler is started.');

        this.logger.println(this.options.name, ' (daemon) is started.');

        const {exit} = await signal;

        this.logger.println(this.options.name, ' (daemon) is shutting down.');

        this.logger.print('Stopping scheduler...');
        await scheduler.stop();
        this.logger.reprintln('Scheduler is stopped.');

        this.logger.print('Unloading plugins... ');
        await pluginLoader.unload();
        this.logger.reprintln('Plugins are unloaded.');

        this.logger.print('Closing connections to database... ');
        await mongodb.close();
        this.logger.reprintln('Database connections are closed.');

        process.off('SIGTERM', exitHandler);
        process.off('SIGINT', exitHandler);

        this.logger.println(this.options.name, ' (daemon) is stopped.');

        if (exit) {
            this.logger.println(this.options.name, ' (daemon) exits.');
            process.exit(0);
        }
    }

};