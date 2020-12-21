"use strict";

const debug = require('debug')('catalyst:scheduler');
const {set, get} = require('lodash');
const {sleep, dedup, Runnable} = require("@raychee/utils");

const {CatchableError, JobEarlyExit, JobInterruption, JobCrash, HeartAttack} = require('./error');
const {Job} = require("./task");
const {Watcher, JobWatcherManager} = require('./watcher');


class Scheduler extends Runnable {

    /**
     * @param {import('./logger').Logger} logger
     * @param {import('./operations').Operations} operations
     * @param {import('./loader').TaskLoader} taskLoader
     * @param {Object} options
     * @param {number} options.heartbeat
     * @param {number} options.heartAttack
     */
    constructor(logger, operations, taskLoader, options) {
        super();

        this.logger = logger;
        this.operations = operations;
        this.taskLoader = taskLoader;
        this.options = options;

        this.id = undefined;
        this.heartbeatAt = undefined;
        this.isRunning = false;

        this.scheduleTasksWatcher = new Watcher(this.operations.tasks, {
            debug: require('debug')('catalyst:scheduler:scheduleTasksWatcher'),
            pipeline: [{
                $match: {
                    'operationType': 'insert',
                    'fullDocument.lockedBy': null,
                    'fullDocument.enabled': true,
                }
            }],
            onEvent: () => this.scheduleTasks(),
        });
        this.scheduleJobsManager = new ScheduleJobManager(this);
        this.scheduleJobsWatcher = new Watcher(this.operations.jobs, {
            debug: require('debug')('catalyst:scheduler:scheduleJobsWatcher'),
            pipeline: [{
                $match: {
                    'operationType': 'insert',
                    'fullDocument.status': 'SUSPENDED',
                }
            }],
            onEvent: () => this.scheduleJobsManager.run(),
        });
        this.dispatchJobsWatcher = new Watcher(this.operations.jobs, {
            debug: require('debug')('catalyst:scheduler:dispatchJobsWatcher'),
            pipeline: [{
                $match: {
                    $or: [
                        {
                            'operationType': 'insert',
                            'fullDocument.status': 'PENDING',
                            'fullDocument.lockedBy': null,
                        },
                        {
                            'operationType': 'update',
                            'updateDescription.updatedFields.status': {
                                $in: ['PENDING', 'SUCCESS', 'FAILED', 'CANCELED']
                            },
                        },
                    ]
                }
            }],
            onEvent: () => this.dispatchJobs(),
        });
        this.spawnJobsWatcher = undefined;
        this.jobWatcher = new JobWatcherManager(
            this.operations.jobs, {pollInterval: this.options.heartbeat}
        ).getWatcher(['SUCCESS', 'FAILED', 'CANCELED']);

        this._executeJob = dedup(Scheduler.prototype._executeJob.bind(this), {key: config => config._id});
        this.scheduleTasks = dedup(Scheduler.prototype.scheduleTasks.bind(this), {queue: 1});
        this.dispatchJobs = dedup(Scheduler.prototype.dispatchJobs.bind(this), {queue: 1, debug});
        this.spawnJobs = dedup(Scheduler.prototype.spawnJobs.bind(this), {queue: 1});
    }

    get isActive() {
        return this.id && this.isRunning;
    }

    async run({signal}) {
        this.isRunning = true;
        await this.activate();
        this.spawnJobsWatcher = new Watcher(this.operations.jobs, {
            debug: require('debug')('catalyst:scheduler:spawnJobsWatcher'),
            pipeline: [{
                $match: {
                    'operationType': 'update',
                    'updateDescription.updatedFields.lockedBy': this.id,
                    'fullDocument.status': 'PENDING',
                }
            }],
            options: {fullDocument: 'updateLookup'},
            onEvent: event => this._executeJob(event.fullDocument),
        });
        signal.then(() => {
            this.isRunning = false;
            this.scheduleTasksWatcher.close();
            this.scheduleJobsWatcher.close();
            this.scheduleJobsManager.stop();
            this.dispatchJobsWatcher.close();
            this.spawnJobsWatcher.close();
            this.jobWatcher.close();
            this.jobWatcher.abortAll(new JobInterruption('_system_shut_down', `scheduler shutting down`));
        });
        this.scheduleTasksWatcher.watch();
        this.scheduleJobsWatcher.watch();
        this.dispatchJobsWatcher.watch();
        this.spawnJobsWatcher.watch();
        while (this.isRunning) {
            try {
                if (this.isRunning) await this.heartbeat();
                if (this.isRunning) await this.clearDeadSchedulers();
                if (this.isRunning && !this.scheduleTasks.state().running) this.scheduleTasks();
                if (this.isRunning && !this.scheduleJobsManager.run.state().running) this.scheduleJobsManager.run();
                if (this.isRunning && !this.dispatchJobs.state().running) this.dispatchJobs();
                if (this.isRunning && !this.spawnJobs.state().running) this.spawnJobs();
            } catch (e) {
                if (e instanceof HeartAttack) {
                    throw e;
                }
                this.logger.warn('Heartbeat failure in scheduler ', this.id, ': ', e);
            }
            await sleep(this.options.heartbeat * 1000);
        }
        debug('scheduler is stopping: wait for task scheduling to stop');
        await this.scheduleTasks.wait();
        debug('scheduler is stopping: wait for job dispatching to stop');
        await this.dispatchJobs.wait();
        debug('scheduler is stopping: wait for job scheduling to stop');
        await this.scheduleJobsManager.run.wait();
        debug('scheduler is stopping: wait for job spawning to stop');
        await this.spawnJobs.wait();
        await this.deactivate();
    }

    async activate() {
        const now = new Date();
        debug('activate at %s', now);
        this.heartbeatAt = now;
        const {insertedId} = await this.operations.schedulers.insertOne({timeStarted: now, heartbeat: now});
        this.id = insertedId;
    }

    async deactivate() {
        const now = new Date();
        debug('deactivate at %s', now);
        const _id = this.id;
        this.id = undefined;
        if (_id) {
            await this.operations.schedulers.updateMany({_id}, {$set: {heartbeat: null, timeStopped: now}});
        }
        await this.clearDeadSchedulers();
    }

    async heartbeat() {
        const now = new Date();
        debug('heartbeat at %s', now);
        if (now - this.heartbeatAt >= this.options.heartAttack * 1000) {
            this.id = undefined;
            const messages = [
                'Scheduler ', this.id, '\'s heartbeat is slower than ',
                this.options.heartAttack,
                ' seconds (last time is ', this.heartbeatAt, ') and will be considered dead.'
            ];
            this.logger.heartAttack(...messages);
            throw new HeartAttack(...messages);
        }
        const {modifiedCount} = await this.operations.schedulers.updateOne(
            {_id: this.id, heartbeat: {$gt: new Date(0)}}, {$set: {heartbeat: now}}
        );
        if (modifiedCount <= 0) {
            this.id = undefined;
            const messages = [
                'Scheduler ', this.id, '\'s heartbeat seems to have been already considered dead.'
            ];
            this.logger.heartAttack(...messages);
            throw new HeartAttack(...messages);
        }
        this.heartbeatAt = now;
    }

    async clearDeadSchedulers() {
        const now = new Date();
        const deadHeartbeat = new Date(now - this.options.heartAttack * 1000);
        debug('clear dead schedulers: %o', {heartbeat: {$lt: deadHeartbeat}});
        await this.operations.schedulers.updateMany(
            {heartbeat: {$lt: deadHeartbeat}}, {$set: {heartbeat: null, timeStopped: now}}
        );
        const activeIds = await this._getActiveSchedulerIds();
        await this.operations.tasks.updateMany(
            {lockedBy: {$nin: [null, ...activeIds]}},
            {$set: {lockedBy: null}},
        );
        await this.operations.updateJobs(
            {lockedBy: {$nin: [null, ...activeIds]}, status: {$in: ['PENDING', 'DELAYED', 'RUNNING']}},
            {lockedBy: null, status: 'PENDING'},
        );
    }

    scheduleTasks() {
        return this._scheduleTasks().catch(
            e => this.logger.warn('Scheduling tasks failed in scheduler ', this.id, ': ', e)
        );
    }

    async _scheduleTasks() {
        const now = new Date();
        const filter = {nextTime: {$lte: now}, lockedBy: null};
        debug('schedule tasks: %o -> %o', filter, {lockedBy: this.id});
        await this.operations.updateTasks(filter, {lockedBy: this.id}, {updateMtime: false});
        for await (const task of this.operations.tasks.find(
            {lockedBy: this.id},
            {
                projection: {
                    nextTime: 1, lastTime: 1, 
                    mode: 1, interval: 1, schedule: 1, timezone: 1, validBefore: 1, validAfter: 1, ctime: 1,
                }
            }
        )) {
            debug('create job from task %s', task._id);
            await this.operations.insertJob({task: task._id, timeScheduled: task.nextTime});
            const {_id, lastTime, nextTime} = task;
            this.operations._computeTaskNextTime(task);
            const {lastTime: newLastTime, nextTime: newNextTime} = task;
            const filter = {_id, lockedBy: this.id, lastTime, nextTime};
            const update = {lastTime: newLastTime, nextTime: newNextTime, lockedBy: null};
            const updated = await this.operations.updateTasks(filter, update, {updateMtime: false});
            if (!(get(updated, 'simple.updated') > 0)) {
                debug(
                    'update task next time failed, maybe it is being changed concurrently by others: ' +
                    'updateTasks(%j, %j) -> %j', filter, update, updated
                );
                await this.operations.updateTasks(
                    {_id, lockedBy: this.id}, {lockedBy: null}, {updateMtime: false}
                );
            }
        }
    }
    
    dispatchJobs() {
        return this._dispatchJobs().catch(
            e => this.logger.warn('Dispatching jobs failed in scheduler ', this.id, ': ', e)
        );
    }

    async _dispatchJobs() {
        const activeIds = await this._getActiveSchedulerIds();
        const {value: core} = await this.operations.schedulers.findOneAndUpdate(
            {_id: 0, lockedBy: {$nin: activeIds.filter(i => i !== this.id)}},
            {$set: {lockedBy: this.id}}, {returnOriginal: false}
        );
        if (!core) {
            debug('dispatch jobs, acquire core lock failed, skipped');
            return false;
        }
        debug('dispatch jobs');
        const runningByType = {}, runningByDomain = {};
        for (const {_id: {domain, type}, count} of await this.operations.jobs.aggregate([
            {$match: {status: {$in: ['PENDING', 'DELAYED', 'RUNNING']}, lockedBy: {$ne: null}}},
            {
                $group: {
                    _id: {domain: '$domain', type: '$type'},
                    count: {$sum: 1},
                }
            }
        ], {allowDiskUse: true}).toArray()) {
            set(runningByType, [domain, type], count);
            runningByDomain[domain] = (runningByDomain[domain] || 0) + count;
        }
        const concurrencyByType = {}, concurrencyByDomain = {};
        for await (const {domain, type, concurrency} of this.operations.types.find()) {
            set(concurrencyByType, [domain, type], concurrency);
        }
        for await (const {domain, maxConcurrency} of this.operations.domains.find()) {
            concurrencyByDomain[domain] = maxConcurrency;
        }
        for (const [domain, domainConcurrency] of Object.entries(concurrencyByDomain)) {
            let availableDomainConcurrency = domainConcurrency - (runningByDomain[domain] || 0);
            if (availableDomainConcurrency <= 0) {
                continue;
            }
            const availableTypeConcurrency = {};
            for (const [type, concurrency] of Object.entries(concurrencyByType[domain])) {
                availableTypeConcurrency[type] = concurrency - get(runningByType, [domain, type], 0);
            }
            let pos = 0;
            while (true) {
                const targetTypes = Object.entries(availableTypeConcurrency)
                    .filter(([, c]) => c > 0).map(([t]) => t);
                if (targetTypes.length <= 0) break;

                let hasLocked = false;
                const cursor = this.operations.jobs.find(
                    {status: 'PENDING', domain, type: {$in: targetTypes}, lockedBy: null},
                    {projection: {domain: 1, type: 1, task: 1}, sort: [['priority', -1], ['timeCreated', 1]]}
                );
                for await (const {_id, domain, type, task} of cursor) {
                    debug(
                        'dispatch job %s.%s %s of task %s to scheduler %s (this scheduler is %s), due to ' +
                        'remaining domainConcurrency = %s, typeConcurrency = %j',
                        domain, type, _id, task, activeIds[pos], this.id,
                        availableDomainConcurrency, availableTypeConcurrency
                    );
                    const updated = await this.operations.updateJobs({_id}, {lockedBy: activeIds[pos]});
                    if (!(updated.simple.updated > 0)) {
                        this.logger.warn(
                            'Locking job ', _id, ' with scheduler id ', activeIds[pos], 
                            ' during job dispatching doesn\'t seem to be successful: ', updated
                        );
                    }
                    hasLocked = true;
                    pos++;
                    if (pos >= activeIds.length) pos = 0;
                    availableDomainConcurrency--;
                    if (availableDomainConcurrency <= 0) break;
                    availableTypeConcurrency[type]--;
                    if (availableTypeConcurrency[type] <= 0) break;
                }
                await cursor.close();
                if (availableDomainConcurrency <= 0) break;
                if (!hasLocked) break;
            }
        }
        await this.operations.schedulers.updateOne(
            {_id: 0, lockedBy: this.id}, {$set: {lockedBy: null}},
        );
        return true;
    }

    spawnJobs() {
        return this._spawnJobs().catch(
            e => this.logger.warn('Spawning jobs failed in scheduler ', this.id, ': ', e)
        );
    }

    async _spawnJobs() {
        debug('spawn jobs: %o', {lockedBy: this.id, status: 'PENDING'});
        for await (const {local, ...job} of this.operations.jobs.find({lockedBy: this.id, status: 'PENDING'})) {
            this._executeJob(job);
        }
    }

    async _executeJob(config) {
        const {_id, domain, type, trials = []} = config;
        debug('execute job %s', _id);
        if (config.status !== 'DELAYED') {
            await this.operations.updateJobs({_id: config._id}, {status: 'DELAYED'});
            config.status = 'DELAYED';
        }
        if (trials.length > 0) {
            const {status, timeStopped} = trials.pop();
            if (['SUCCESS', 'CANCELED'].includes(status)) {
                await this.operations.updateJobs({_id}, {status, timeStopped});
                return;
            } else {
                await this.operations.updateJobs({_id}, {trials});
            }
        }
        let trial = trials.length;
        if (trial > config.retry) {
            debug('skip executing job %s because it has already retried %d times', _id, trial - 1);
            return;
        }
        const taskType = await this.taskLoader.getTaskType(domain, type);
        const job = new Job(config, taskType, this.jobWatcher, this, this.operations);
        try {
            await job._execute();
        } catch (e) {
            if (
                !(e instanceof JobEarlyExit) &&
                !(e instanceof CatchableError) &&
                !(e instanceof JobCrash)
            ) {
                this.logger.error('System crashed during job execution of ', job._id, ': ', e);
            }
        }
    }

    async _getActiveSchedulerIds() {
        const active = await this.operations.schedulers.find({heartbeat: {$gt: new Date(0)}}).toArray();
        return active.map(s => s._id);
    }

}


class ScheduleJobManager {

    constructor(scheduler) {
        this.scheduler = scheduler;
        this.nextRun = undefined;
        this.stopped = false;
        this.run = dedup(ScheduleJobManager.prototype.run.bind(this), {queue: 1, debug});
    }
    
    run() {
        if (this.stopped) return;
        return this._run().catch(
            e => this.scheduler.logger.warn('Scheduling jobs failed in scheduler ', this.scheduler.id, ': ', e)
        );
    }
    
    stop() {
        this.stopped = true;
        this.clearNext();
    }
    
    clearNext() {
        if (this.nextRun) {
            clearTimeout(this.nextRun);
            this.nextRun = undefined;
        }
    }
    
    async _run() {
        this.clearNext();
        const now = new Date();
        const filter = {status: 'SUSPENDED', timePending: {$lte: now}};
        const update = {status: 'PENDING'};
        debug('schedule jobs: %o -> %o', filter, update);
        await this.scheduler.operations.updateJobs(filter, update);
        const nextJob = await this.scheduler.operations.jobs.findOne(
            {status: 'SUSPENDED'}, {sort: [['timePending', 1]], projection: {_id: 0, timePending: 1}}
        );
        if (nextJob) {
            const interval = nextJob.timePending.getTime() - now;
            debug(
                'there are still suspended jobs, next time to schedule jobs is: %s (%s seconds later)',
                nextJob.timePending, interval / 1000,
            );
            this.nextRun = setTimeout(() => { this.nextRun = undefined; this.run(); }, interval);
        }
    }

}




module.exports = Scheduler;
