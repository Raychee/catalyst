"use strict";

const debug = require('debug')('catalyst:loader');
const path = require('path');

const {get, setWith} = require('lodash');

const {limit, dedup, stableJSONStringify} = require('@raychee/utils');
const {walk} = require('@raychee/utils/lib/node');
const {TaskDomain, TaskType} = require('./task');
const {StoreLogger} = require('./logger');
const {SystemError} = require('./error');


class TaskLoader {

    /**
     * @param {string[]} loadPaths
     * @param {PluginLoader} [pluginLoader]
     * @param {Function} [loadFn]
     */
    constructor(loadPaths, pluginLoader, loadFn) {
        this.loadPaths = loadPaths;
        this.pluginLoader = pluginLoader;
        this.loadFn = loadFn || (v => v);

        /** @type {Object<string, TaskDomain>} */
        this.loadedTaskDomains = {};
        /** @type {Object<string, TaskType>} */
        this.loadedTaskTypes = {};
    }

    /**
     * @return {TaskDomain[]}
     */
    getAllTaskDomains() {
        return Object.values(this.loadedTaskDomains);
    }

    /**
     * @param {string} domainName
     * @return {TaskDomain}
     */
    getTaskDomain(domainName) {
        return this.loadedTaskDomains[domainName];
    }

    /**
     * @return {TaskType[]}
     */
    getAllTaskTypes() {
        return Object.values(this.loadedTaskTypes);
    }

    /**
     * @param {string} domainName
     * @param {string} typeName
     * @return {TaskType}
     */
    getTaskType(domainName, typeName) {
        return this.loadedTaskTypes[`${domainName}.${typeName}`];
    }

    async load() {
        this.loadedTaskDomains = {};
        this.loadedTaskTypes = {};
        for await (let scanned of this._scan()) {
            const [taskFilePath, domainName, taskFileName] = scanned;
            let domain = this.loadedTaskDomains[domainName];
            if (!domain) {
                domain = new TaskDomain(domainName, this.pluginLoader);
                this.loadedTaskDomains[domainName] = domain;
            }
            if (scanned.length !== 3 || taskFileName !== 'domain') {
                continue;
            }
            const taskDomainSpec = await this.loadFn(require(taskFilePath), {domain: domainName});
            await domain.load(taskDomainSpec);
        }
        for await (let scanned of this._scan()) {
            const [taskFilePath, ...taskTypeFields] = scanned;
            const [domainName, ...taskTypeNameFields] = taskTypeFields;
            if (scanned.length === 3 && taskTypeNameFields[0] === 'domain') {
                continue;
            }
            const domain = this.loadedTaskDomains[domainName];
            const taskTypeFullName = taskTypeFields.join('.');
            if (this.loadedTaskTypes[taskTypeFullName]) {
                throw new SystemError(`conflict task type: ${taskTypeFullName}`);
            }
            const taskTypeName = taskTypeNameFields.join('.');
            const taskTypeSpec = await this.loadFn(require(taskFilePath), {domain: domainName, type: taskTypeName});
            const taskType = new TaskType(domain, taskTypeName, this.pluginLoader);
            await taskType.load(taskTypeSpec);
            this.loadedTaskTypes[taskTypeFullName] = taskType;
        }
    }

    async *_scan() {
        for (const loadPath of this.loadPaths) {
            for await (let taskFilePath of walk(loadPath)) {
                if (!taskFilePath.endsWith('.js')) {
                    continue;
                }
                taskFilePath = taskFilePath.substring(0, taskFilePath.length - 3);
                const i = taskFilePath.lastIndexOf(loadPath) + loadPath.length;
                const taskTypeFields = taskFilePath
                    .substring(i, taskFilePath.length)
                    .split(path.sep)
                    .filter(f => f);
                if (taskTypeFields[taskTypeFields.length - 1].startsWith('_')) {
                    continue;
                }
                taskTypeFields.unshift(taskFilePath);
                yield taskTypeFields;
            }
        }
    }

}


class PluginLoader {

    constructor(loadPaths, storeCollection, defaultPlugins = {}, metaConfigs = {}, options = {}) {
        this.loadPaths = loadPaths;
        this.storeCollection = storeCollection;
        this.loadedPluginModules = {};
        this.loadedPlugins = {};
        this.defaultPlugins = defaultPlugins;
        this.metaConfigs = metaConfigs;
        this.options = options;

        this._get = limit(PluginLoader.prototype._get.bind(this), 1);
    }

    /**
     * @typedef PluginEntryConfig
     * @type {object}
     * @property {boolean} [destroyOnUnload]
     */
    /**
     * @typedef PluginEntry
     * @type {object}
     * @property {*} instance - the plugin instance object
     * @property {*} [bound] - the plugin instance object bounded with a job (from bind())
     * @property {PluginEntryConfig} [config]
     * @property {string} [key]
     * @property {number} [refs]
     * @property {StoreLogger} logger
     * @property {Function} destroy
     * @property {Function} unload
     */
    /**
     * @param pluginOption
     * @param job
     * @return {Promise<PluginEntry>}
     */
    async get(pluginOption, job) {
        const pluginFns = this.loadedPluginModules[pluginOption.type];
        if (!pluginFns) {
            throw new SystemError(`plugin ${pluginOption.type} does not exist.`);
        }
        const {key} = pluginFns;
        let keyValue = undefined;
        if (key) {
            keyValue = key(pluginOption);
            if (keyValue) {
                keyValue = stableJSONStringify(keyValue, {circularValue: '__cycle__'});
            }
        }
        const plugin = keyValue ?
            await this._get(keyValue, pluginOption, pluginFns, job) :
            await this._create(pluginOption, keyValue, pluginFns, job);
        return {
            ...plugin,
            bound: this.bind(plugin.instance, job || plugin.logger),
        };
    }

    bind(instance, job) {
        return new Proxy(instance, {
            get(target, p, receiver) {
                if (typeof p === 'string' && p[0] === '_') {
                    job.crash('_bad_op', 'access to this.<plugin>.', p, ' is invalid in task runners');
                }
                const prop = Reflect.get(target, p, receiver);
                if (typeof prop === 'function') {
                    return prop.bind(target, job);
                } else {
                    return prop;
                }
            },
            ownKeys(target) {
                const ownKeys = Reflect.ownKeys(target);
                return ownKeys.filter(k => typeof k !== 'string' || k[0] !== '_');
            },
            apply(target, thisArg, argArray) {
                return Reflect.apply(target, thisArg, [job, ...argArray]);
            }
        });
    }

    async _get(keyValue, pluginOption, pluginFns, logger) {
        const existing = get(this.loadedPlugins, [pluginOption.type, keyValue]);
        if (existing) {
            existing.refs++;
            debug('plugin get(): %j, existing refs = %s', pluginOption, existing.refs);
            if (existing.ttl) {
                debug('plugin get(): %j, clear existing timeout', pluginOption);
                clearTimeout(existing.ttl);
                delete existing.ttl;
            }
            return existing;
        } else {
            const new_ = await this._create(pluginOption, keyValue, pluginFns, logger);
            setWith(this.loadedPlugins, [pluginOption.type, keyValue], new_, Object);
            return new_;
        }
    }

    /**
     * @return {Promise<PluginEntry>}
     * @private
     */
    async _create(pluginOption, key, {create, destroy, unload, config = {}}, logger) {
        debug('plugin get(): %j, create new', pluginOption);
        const {type} = pluginOption;
        const pluginLogger = new StoreLogger({
            category: 'Plugin', prefixes: [type],
            collection: this.storeCollection, filter: {plugin: type},
        });
        let pluginLoader = this;
        if (logger) {
            pluginLoader = new Proxy(this._this(), {
                get(target, p, receiver) {
                    switch (p) {
                        case 'get':
                            return async (pluginOpts, l) => {
                                return await target.get(pluginOpts, l || logger);
                            };
                        case 'bind':
                            return (plugin, l) => {
                                return target.bind(plugin, l || logger);
                            };
                        case '_this':
                            return () => target;
                        default:
                            return Reflect.get(target, p, receiver);
                    }
                }
            });
        }
        const instance = await create.call(pluginLogger, pluginOption, {pluginLoader});
        const created = {key, instance, logger: pluginLogger, config, refs: 1};
        const destroy_ = dedup(async () => {
            debug('plugin real destroy(): %j', pluginOption);
            try {
                if (destroy) {
                    await destroy.call(pluginLogger, instance);
                }
                if (key) {
                    if (this.loadedPlugins[type]) {
                        debug('plugin real destroy(): %j, delete key = %s', pluginOption, key);
                        delete this.loadedPlugins[type][key];
                    }
                }
            } catch (e) {
                pluginLogger.warn('Plugin error during destroy: ', e);
            }
        });
        created.destroy = async ({force} = {}) => {
            if (created.refs > 0) {
                created.refs--;
                debug('plugin destroy(): %j, refs = %s', pluginOption, created.refs);
                if (created.refs <= 0 || force) {
                    if (key && !force) {
                        if (this.options.ttl >= 0 && isFinite(this.options.ttl) && !created.ttl) {
                            debug(
                                'plugin destroy(): %j, set timeout of %s seconds for real destroy',
                                pluginOption, this.options.ttl
                            );
                            created.ttl = setTimeout(destroy_, this.options.ttl * 1000);
                        }
                    } else {
                        if (created.ttl) {
                            debug('plugin destroy(): %j, clear timeout', pluginOption);
                            clearTimeout(created.ttl);
                            delete created.ttl;
                        }
                        await destroy_();
                    }
                }
            }
        };
        created.unload = async (job) => {
            debug('plugin unload(%s): %j', job.config._id, pluginOption);
            if (unload) {
                await unload.call(pluginLogger, instance, job)
                    .catch(e => pluginLogger.warn('Plugin error during unload: ', e));
            }
        };
        return created;
    }

    async getAll(pluginOpts, job) {
        const plugins = {};
        for (const [name, pluginOption] of Object.entries(pluginOpts)) {
            if (!pluginOption.type) pluginOption.type = name;
            plugins[name] = await this.get(pluginOption, job);
        }
        return plugins;
    };

    async load() {
        this.loadedPluginModules = {};
        const pluginModules = [];
        for (const loadPath of this.loadPaths) {
            if (typeof loadPath === 'string') {
                for await (let filePath of walk(loadPath)) {
                    if (!filePath.endsWith('.js')) continue;
                    filePath = filePath.substring(0, filePath.length - 3);
                    const pluginModule = require(filePath);
                    if (!pluginModule.type) {
                        let pluginType = filePath.split(path.sep).filter(f => f);
                        pluginType = pluginType[pluginType.length - 1];
                        pluginModule.type = pluginType;
                    }
                    pluginModules.push(pluginModule);
                }
            } else {
                pluginModules.push(loadPath);
            }
        }
        for (let pluginModule of pluginModules) {
            if (pluginModule.factory) {
                pluginModule = {...pluginModule, ...await pluginModule.factory(this.metaConfigs[pluginModule.type])};
            }
            if (this.loadedPluginModules[pluginModule.type]) {
                throw new SystemError(`conflict plugin name: ${pluginModule.type}`);
            }
            this.loadedPluginModules[pluginModule.type] = pluginModule;
        }
    }

    async unload() {
        const promises = Object.values(this.loadedPlugins)
            .flatMap(v => Object.values(v).map(({destroy}) => destroy({force: true})));
        if (promises.length > 0) {
            await Promise.all(promises);
        }
    }
    
    _this() {
        return this;
    }

}


module.exports = {
    TaskLoader,
    PluginLoader,
};
