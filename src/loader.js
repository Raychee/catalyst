const path = require('path');

const {get, setWith} = require('lodash');
const stableStringify = require('json-stable-stringify');

const {walk, limit, dedup, sleep, ensureThunkSync} = require('@raychee/utils');
const {TaskDomain, TaskType} = require('./task');
const {StoreLogger} = require('./logger');


class TaskLoader {

    constructor(loadPaths, operations, pluginLoader, storeCollection,
                jobContextCache, newAgenda, taskAgendaName) {
        this.loadPaths = loadPaths;
        this.operations = operations;
        this.pluginLoader = pluginLoader;
        this.storeCollection = storeCollection;
        this.jobContextCache = jobContextCache;
        this.newAgenda = newAgenda;
        this.loaded = false;
        this.agendas = {};
        this.taskAgenda = undefined;
        this.taskAgendaName = taskAgendaName;
        this.loadedTaskDomains = {};
        this.loadedTaskTypes = {};

        this.syncConfigInterval = 60;
        this._syncConfigs = dedup(TaskLoader.prototype._syncConfigs.bind(this), {within: this.syncConfigInterval * 1000});
    }

    async getAllAgendas() {
        if (!this.loaded) await this.load();
        return this.agendas;
    }

    async getAllTaskDomains() {
        if (!this.loaded) await this.load();
        return this.loadedTaskDomains;
    }

    async getAllTaskTypes() {
        if (!this.loaded) await this.load();
        return this.loadedTaskTypes;
    }

    async getAgenda(domainName) {
        return (await this.getAllAgendas())[domainName];
    }

    async getDomain(domainName) {
        return (await this.getAllTaskDomains())[domainName];
    }

    async get(taskTypeFields) {
        return (await this.getAllTaskTypes())[taskTypeFields.join('.')];
    }

    async load({syncConfigsPeriodically = false} = {}) {
        this.loadedTaskDomains = {};
        this.loadedTaskTypes = {};
        if (!this.taskAgenda && this.newAgenda) {
            this.taskAgenda = this.newAgenda(this.taskAgendaName);
            this.taskAgenda
                .maxConcurrency(1000)
                .defaultConcurrency(1000)
                .lockLimit(1000)
                .defaultLockLimit(1000)
                .defaultLockLifetime(10 * 60 * 1000);
            this.taskAgenda.define('_task', async (agendaJob) => {
                const {taskId} = agendaJob.attrs.data;
                const taskConfig = await this.operations.getTask([taskId]);
                if (taskConfig.validBefore && Date.now() >= new Date(taskConfig.validBefore)) {
                    await agendaJob.disable();
                    await agendaJob.save();
                    return;
                }
                const jobConfig = {
                    domain: taskConfig.domain,
                    type: taskConfig.type,
                    params: taskConfig.params,
                    context: taskConfig.context,
                    task: taskConfig.id,
                };
                await this.operations.populateTaskSchedulingProperties(jobConfig, taskConfig);
                await this.operations.scheduleJob(jobConfig);
            });
        }
        for await (let scanned of this._scan()) {
            const [taskFilePath, domainName, taskFileName] = scanned;
            let domain = this.loadedTaskDomains[domainName];
            if (!domain) {
                domain = new TaskDomain(domainName, this.pluginLoader, this.storeCollection);
                this.loadedTaskDomains[domainName] = domain;
                if (this.newAgenda && !this.agendas[domainName]) {
                    this.agendas[domainName] = this.newAgenda(domainName);
                }
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
                throw new Error(`conflict task type: ${taskTypeFullName}`);
            }
            const taskTypeName = taskTypeNameFields.join('.');
            const taskTypeSpec = require(taskFilePath);
            const taskType = new TaskType(
                domain, taskTypeName, this.operations, this.pluginLoader, this.storeCollection, this.jobContextCache
            );
            await taskType.load(taskTypeSpec);
            this.loadedTaskTypes[taskTypeFullName] = taskType;
        }
        this.loaded = true;

        if (this.operations) {
            await this._syncConfigs();
            if (syncConfigsPeriodically) {
                (async () => {
                    while (true) {
                        await sleep(this.syncConfigInterval * 1000);
                        try {
                            await this._syncConfigs();
                        } catch (e) {
                            console.error('Refreshing configs encounters an error: ' + e);
                        }
                    }
                })();
            }
        }
    }

    async _syncConfigs() {
        const dataloaders = {};
        await this.operations.updateTaskDomainConfigs(dataloaders);
        await this.operations.updateTaskTypeConfigs(undefined, dataloaders);
    }

    async start() {
        process.stdout.write('Starting agenda... ');
        await this.taskAgenda.start();
        process.stdout.write('\rStarting agenda... Done.\n');
        process.stdout.write('Re-scheduling interrupted jobs... ');
        const now = new Date();
        for (const agenda of Object.values(this.agendas)) {
            while (true) {
                const agendaJobs = await agenda.jobs({
                    type: 'normal', lastFinishedAt: null, lockedAt: null, nextRunAt: null, disabled: {$ne: true}
                }, {}, 200);
                if (agendaJobs.length > 0) {
                    await Promise.all(agendaJobs.map(async agendaJob => {
                        const {jobId} = agendaJob.attrs.data;
                        if (jobId) {
                            await this.operations.upsert('Job', {id: jobId, status: 'PENDING'}, true, undefined, {id: jobId});
                        }
                        agendaJob.schedule(now);
                        await agendaJob.save();
                    }));
                } else {
                    break;
                }
            }
        }
        while (true) {
            const jobs = await this.operations.query('Job', {status: 'INTERRUPTED'}, {limit: 200});
            if (jobs.length > 0) {
                await Promise.all(jobs.map(({id}) => this.operations.scheduleJob({id, status: 'PENDING'})));
            } else {
                break;
            }
        }
        process.stdout.write('\rRe-scheduling interrupted jobs... Done.\n');
        await this._start();
    }

    async stop() {
        await this.taskAgenda.stop();
        const runningJobIds = [];
        for (const agenda of Object.values(this.agendas)) {
            runningJobIds.push(...agenda._runningJobs.map(j => j.attrs.data.jobId).filter(i => i));
        }
        process.stdout.write('Stopping agenda...');
        await this._stop();
        process.stdout.write('\rStopping agenda... Done.\n');
        process.stdout.write('Marking running jobs as interrupted...');
        await this.operations.mongodb.collection('Job').updateMany(
            {id: {$in: runningJobIds}}, {$set: {status: 'INTERRUPTED'}}
        );
        process.stdout.write('\rMarking running jobs as interrupted... Done.\n');
    }

    async _start() {
        for (const agenda of Object.values(this.agendas)) {
            await agenda.start();
        }
    }

    async _stop() {
        for (const agenda of Object.values(this.agendas)) {
            await agenda.stop();
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
        this.loaded = false;
        this.loadedPluginFns = {};
        this.loadedPlugins = {};
        this.defaultPlugins = defaultPlugins;
        this.metaConfigs = metaConfigs;

        this._get = limit(PluginLoader.prototype._get, 1);
    }

    async get(pluginOption, job) {
        const pluginFns = (await this.getAllPluginFns())[pluginOption.type];
        if (!pluginFns) {
            throw new Error(`plugin ${pluginOption.type} does not exist.`);
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

    async _create(pluginOption, {create, destroy, config}, logger) {
        const pluginLogger = new StoreLogger(this.storeCollection, {plugin: pluginOption.type}, 'Plugin', [pluginOption.type]);
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

    async getAllPluginFns() {
        if (!this.loaded) await this.load();
        return this.loadedPluginFns;
    }

    async getAll(pluginOpts, job) {
        const plugins = {};
        await Promise.all(Object.entries(pluginOpts).map(
            async ([name, pluginOption]) => {
                if (!pluginOption.type) pluginOption.type = name;
                plugins[name] = await this.get(pluginOption, job);
            }
        ));
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
                    throw new Error(`conflict plugin name: ${pluginType}`);
                }
                this.loadedPluginFns[pluginType] = ensureThunkSync(require(filePath), this.metaConfigs[pluginType]);
            }
        }
        this.loaded = true;
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