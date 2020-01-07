const path = require('path');

const {get, setWith} = require('lodash');
const stableStringify = require('json-stable-stringify');

const {walk, limit, ensureThunkSync} = require('@raychee/utils');
const {TaskDomain, TaskType} = require('./task');
const {Logger, StoreLogger} = require('./logger');
const {SystemError} = require('./error');


class TaskLoader {

    /**
     * @param {string[]} loadPaths
     * @param {PluginLoader} [pluginLoader]
     */
    constructor(loadPaths, pluginLoader) {
        this.loadPaths = loadPaths;
        this.pluginLoader = pluginLoader;

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
            const taskDomainSpec = require(taskFilePath);
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
            const taskTypeSpec = require(taskFilePath);
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

    constructor(loadPaths, storeCollection, defaultPlugins = {}, metaConfigs = {}) {
        this.loadPaths = loadPaths;
        this.storeCollection = storeCollection;
        this.loadedPluginFns = {};
        this.loadedPlugins = {};
        this.defaultPlugins = defaultPlugins;
        this.metaConfigs = metaConfigs;

        this._get = limit(PluginLoader.prototype._get.bind(this), 1);
    }

    /**
     * @typedef PluginEntryConfig
     * @type {object}
     * @property {boolean} [destroyOnJobDone]
     */
    /**
     * @callback PluginDestroyFn
     */
    /**
     * @typedef PluginEntry
     * @type {object}
     * @property {*} instance - the plugin instance object from create()
     * @property {PluginEntryConfig} config
     * @property {string} [key]
     * @property {StoreLogger} logger
     * @property {PluginDestroyFn} destroy
     */
    /**
     * @param pluginOption
     * @param job
     * @return {Promise<PluginEntry>}
     */
    async get(pluginOption, job) {
        const pluginFns = this.loadedPluginFns[pluginOption.type];
        if (!pluginFns) {
            throw new SystemError(`plugin ${pluginOption.type} does not exist.`);
        }
        const {key} = pluginFns;
        let keyValue = undefined;
        if (key) {
            keyValue = key(pluginOption);
            if (keyValue) {
                keyValue = stableStringify(keyValue);
            }
        }
        const plugin = keyValue ?
            await this._get(keyValue, pluginOption, pluginFns, job) :
            await this._create(pluginOption, pluginFns, job);
        return {
            ...plugin,
            instance: this.create(plugin.instance, job || plugin.logger),
        };
    }

    create(instance, job) {
        return new Proxy(instance, {
            get(target, p, receiver) {
                const prop = Reflect.get(target, p, receiver);
                if (typeof prop === 'function' && !p.startsWith('_')) {
                    return prop.bind(target, job);
                } else {
                    return prop;
                }
            },
            apply(target, thisArg, argArray) {
                return Reflect.apply(target, thisArg, [job, ...argArray]);
            }
        });
    }

    async _get(keyValue, pluginOption, pluginFns, logger) {
        const existing = get(this.loadedPlugins, [pluginOption.type, keyValue]);
        if (existing) {
            return existing;
        } else {
            const new_ = await this._create(pluginOption, pluginFns, logger);
            new_.key = keyValue;
            setWith(this.loadedPlugins, [pluginOption.type, keyValue], new_, Object);
            return new_;
        }
    }

    /**
     * @return {Promise<PluginEntry>}
     * @private
     */
    async _create(pluginOption, {create, destroy, config = {}}, logger) {
        const pluginLogger = new StoreLogger({
            category: 'Plugin', prefixes: [pluginOption.type],
            collection: this.storeCollection, filter: {plugin: pluginOption.type},
        });
        let pluginLoader = this;
        if (logger) {
            pluginLoader = new Proxy(this, {
                get(target, p, receiver) {
                    switch (p) {
                        case 'get':
                            return async (pluginOpts, l) => {
                                return await target.get(pluginOpts, l || logger);
                            };
                        case 'create':
                            return (plugin, l) => {
                                return target.create(plugin, l || logger);
                            };
                        default:
                            return Reflect.get(target, p, receiver);
                    }
                }
            });
        }
        const instance = await create.call(pluginLogger, pluginOption, {pluginLoader});
        return {
            instance, logger: pluginLogger, config,
            destroy: async () => {
                if (!destroy) return;
                try {
                    await destroy.call(pluginLogger, instance);
                } catch (e) {
                    pluginLogger.warn('Plugin unload error: ', e);
                }
            },
        };
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
        this.loadedPluginFns = {};
        for (const loadPath of this.loadPaths) {
            for await (let filePath of walk(loadPath)) {
                if (!filePath.endsWith('.js')) continue;
                filePath = filePath.substring(0, filePath.length - 3);
                let pluginType = filePath.split(path.sep).filter(f => f);
                pluginType = pluginType[pluginType.length - 1];
                if (this.loadedPluginFns[pluginType]) {
                    throw new SystemError(`conflict plugin name: ${pluginType}`);
                }
                this.loadedPluginFns[pluginType] = ensureThunkSync(require(filePath), this.metaConfigs[pluginType]);
            }
        }
    }

    async unload() {
        await Promise.all(
            Object.values(this.loadedPlugins).flatMap(v =>
                Object.values(v).map(async ({destroy}) => destroy())
            )
        );
    }

}


module.exports = {
    TaskLoader,
    PluginLoader,
};