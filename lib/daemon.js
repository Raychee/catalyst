const {resolve} = require('path');

const {MongoClient} = require('mongodb');

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

module.exports = class {

    constructor(options) {
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

        /** @type {MongoClient} */
        this.mongodb = undefined;
        /** @type {PluginLoader} */
        this.pluginLoader = undefined;
        /** @type {Scheduler} */
        this.scheduler = undefined;

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
        logger.println(this.options.name, ' (daemon) is starting up.');

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

        const taskLoader = new TaskLoader(this.options.tasks.paths, this.pluginLoader);
        await taskLoader.load({verbose});

        logger.print('Preparing database... ');
        const operations = new Operations(
            new Logger({category: 'Operations'}), this.mongodb.db(db), taskLoader
        );
        await operations.prepare();
        logger.reprintln('Database is prepared.');

        logger.print('Starting scheduler...');
        this.scheduler = new Scheduler(
            new Logger({category: 'Scheduler'}), operations, taskLoader, this.options.daemon
        );
        this.scheduler.start();
        logger.reprintln('Scheduler is started.');

        logger.println(this.options.name, ' (daemon) is started.');
    }

    async _stop({verbose = true, exit = false} = {}) {
        const logger = new Logger({level: verbose ? 'INFO' : 'ERROR'});
        const pluginLoader = this.pluginLoader;
        const mongodb = this.mongodb;
        const scheduler = this.scheduler;
        this.taskLoader = undefined;
        this.pluginLoader = undefined;
        this.mongodb = undefined;
        this.scheduler = undefined;
        logger.println(this.options.name, ' (daemon) is shutting down.');
        if (scheduler) {
            logger.print('Stopping scheduler...');
            await scheduler.stop();
            logger.reprintln('Scheduler is stopped.');
        }
        if (pluginLoader) {
            logger.print('Unloading plugins... ');
            await pluginLoader.unload();
            logger.reprintln('Plugins are unloaded.');
        }
        logger.print('Closing connections to database... ');
        await mongodb.close();
        logger.reprintln('Database connections are closed.');
        logger.println(this.options.name, ' (daemon) is stopped.');

        if (exit) {
            logger.println(this.options.name, ' (daemon) exits.');
            process.exit(0);
        }
    }
};
