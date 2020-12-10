"use strict";

const {sep} = require('path');

const {MongoClient} = require('mongodb');
const {Runnable} = require('@raychee/utils');

const {DEFAULT_HEARTBEAT, DEFAULT_HEART_ATTACK, COLLECTION_NAMES} = require('./config');
const {Logger} = require('./logger');
const {TaskLoader, PluginLoader} = require('./loader');
const Operations = require('./operations');
const Scheduler = require('./scheduler');
const {SystemError} = require('./error');


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
        this.options.db.options = {
            useNewUrlParser: true, useUnifiedTopology: true,
            ...this.options.db.options
        };
        if (!this.options.db.db) throw new SystemError('db.db must be provided');
        this.options.tasks = this.options.tasks || {};
        this.options.tasks.paths = (this.options.tasks.paths || []).map(p => typeof p === 'string' ? p.replace(/[\\/]/g, sep) : p);
        this.options.tasks.loader = this.options.tasks.loader || (v => v);
        this.options.plugins = this.options.plugins || {};
        this.options.plugins.paths = (this.options.plugins.paths || []).map(p => typeof p === 'string' ? p.replace(/[\\/]/g, sep) : p);
        this.options.plugins.defaults = this.options.plugins.defaults || {};
        this.options.plugins.config = this.options.plugins.config || {};
        this.options.plugins.ttl = this.options.plugins.ttl || -1;
        this.options.daemon = this.options.daemon || {};
        this.options.daemon.heartbeat = this.options.daemon.heartbeat || DEFAULT_HEARTBEAT;
        this.options.daemon.heartAttack = this.options.daemon.heartAttack || DEFAULT_HEART_ATTACK;
        this.options.daemon.exitOnHeartAttack = this.options.daemon.exitOnHeartAttack == null ? true : this.options.daemon.exitOnHeartAttack;
        this.options.logging = this.options.logging || {};
        this.options.logging.level = this.options.logging.level || 'INFO';
        this.options.logging.showTimestamp = this.options.logging.showTimestamp || false;

        this.logger = new Logger();

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async run({signal}) {
        this.logger.println(this.options.name, ' (daemon) is starting up.');

        const exitHandler = this.stop.bind(this, {exit: 0});
        process.once('SIGTERM', exitHandler);
        process.once('SIGINT', exitHandler);

        const {host, port, user, password, db} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        const mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, this.options.db.options
        );

        this.logger.print('Connecting to database... ');
        await mongodb.connect();
        this.logger.reprintln('Database is connected.');

        const storeCollection = mongodb.db(db).collection(COLLECTION_NAMES.Store);

        this.logger.print('Loading plugins... ');
        const {paths: pluginPaths, defaults, config, ttl} = this.options.plugins;
        const pluginLoader = new PluginLoader(pluginPaths, storeCollection, defaults, config, {ttl});
        await pluginLoader.load();
        this.logger.reprintln('Plugins are loaded.');

        this.logger.print('Loading task domains and types... ');
        const taskLoader = new TaskLoader(this.options.tasks.paths, pluginLoader, this.options.tasks.loader);
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
            operations, taskLoader, this.options.daemon,
        );
        await scheduler.start({waitForReady: true});
        this.logger.reprintln('Scheduler is started.');

        this.logger.println(this.options.name, ' (daemon) is started.');

        let terminated = await Promise.race([
            signal,
            scheduler.waitForStop().catch(e => {
                this.logger.println('Scheduler encounters a fatal error and will be shut down: ', e);
            })]);
        if (!terminated) {
            terminated = this.options.daemon.exitOnHeartAttack ? {exit: 1} : {};
        }

        this.logger.println(this.options.name, ' (daemon) is shutting down.');

        this.logger.print('Stopping scheduler...');
        await scheduler.stop({waitForStop: true});
        this.logger.reprintln('Scheduler is stopped.');

        this.logger.print('Unloading plugins... ');
        await pluginLoader.unload();
        this.logger.reprintln('Plugins are unloaded.');

        this.logger.print('Closing connections to database... ');
        await mongodb.close();
        this.logger.reprintln('Database connections are closed.');

        this.logger.println(this.options.name, ' (daemon) is stopped.');
        if (terminated.exit != null) {
            this.logger.println(this.options.name, ' (daemon) exits.');
            process.exit(terminated.exit);
        }
    }

};
