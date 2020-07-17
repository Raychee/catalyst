const debug = require('debug')('catalyst:scheduler');
const {set, get} = require('lodash');
const {CronTime} = require('cron');
const moment = require('moment');
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
                            'updateDescription.updatedFields.status': {$in: ['SUCCESS', 'FAILED', 'CANCELED']},
                        }
                    ]
                }
            }],
            onEvent: () => this.dispatchJobs(),
        });
        this.scheduleJobsWatcher = undefined;
        this.jobWatcher = new JobWatcherManager(
            this.operations.jobs, {pollInterval: this.options.heartbeat}
        ).getWatcher(['SUCCESS', 'FAILED', 'CANCELED']);

        this._executeJob = dedup(Scheduler.prototype._executeJob.bind(this), {key: config => config._id});
        this.scheduleTasks = dedup(Scheduler.prototype.scheduleTasks.bind(this), {queue: 1});
        this.dispatchJobs = dedup(Scheduler.prototype.dispatchJobs.bind(this), {queue: 1});
        this.scheduleJobs = dedup(Scheduler.prototype.scheduleJobs.bind(this), {queue: 1});
    }

    get isActive() {
        return this.id && this.isRunning;
    }

    async run({signal}) {
        this.isRunning = true;
        await this.activate();
        this.scheduleJobsWatcher = new Watcher(this.operations.jobs, {
            debug: require('debug')('catalyst:scheduler:scheduleJobsWatcher'),
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
            this.dispatchJobsWatcher.close();
            this.scheduleJobsWatcher.close();
            this.jobWatcher.close();
            this.jobWatcher.abortAll(new JobInterruption('_system_shut_down', `scheduler shutting down`));
        });
        this.scheduleTasksWatcher.watch();
        this.dispatchJobsWatcher.watch();
        this.scheduleJobsWatcher.watch();
        while (this.isRunning) {
            try {
                if (this.isRunning) await this.heartbeat();
                if (this.isRunning) await this.clearDeadSchedulers();
                if (this.isRunning && !this.scheduleTasks.state().running) this.scheduleTasks();
                if (this.isRunning && !this.dispatchJobs.state().running) this.dispatchJobs();
                if (this.isRunning && !this.scheduleJobs.state().running) this.scheduleJobs();
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
        await this.scheduleJobs.wait();
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
            {lockedBy: {$nin: [null, ...activeIds]}, status: {$in: ['DELAYED', 'RUNNING', 'PENDING']}},
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
        const filter = {
            nextTime: {$lte: now}, lockedBy: null,
            enabled: true,
            validBefore: {$not: {$lt: now}},
            validAfter: {$not: {$gte: now}},
        };
        debug('schedule tasks: %o -> %o', filter, {lockedBy: this.id});
        await this.operations.updateTasks(filter, {lockedBy: this.id});
        for await (const task of this.operations.tasks.find({lockedBy: this.id})) {
            const now = new Date();
            const isNew = task.nextTime <= 0 && !task.lastTime;
            const isReset = task.nextTime <= 0 && task.lastTime;
            if (isNew) {
                task.nextTime = now;
            }
            if (isReset) {
                task.nextTime = task.lastTime;
                task.lastTime = undefined;
            }
            if (!isReset && (!isNew || task.mode !== 'SCHEDULED')) {
                debug('create job from task %s', task._id);
                await this.operations.insertJob({task: task._id});
            }
            while (task.nextTime && task.nextTime <= now) {
                const {lastTime, nextTime} = this._computeTaskNextTime(task);
                task.nextTime = nextTime;
                task.lastTime = lastTime;
            }
            const {_id, lastTime, nextTime} = task;
            await this.operations.updateTasks({_id}, {lastTime, nextTime, lockedBy: null});
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
                    {sort: [['priority', -1], ['timeCreated', 1]]}
                );
                for await (const {_id, domain, type, task} of cursor) {
                    debug(
                        'dispatch job %s.%s %s of task %s to scheduler %s (this scheduler is %s), due to ' +
                        'remaining domainConcurrency = %s, typeConcurrency = %j',
                        domain, type, _id, task, activeIds[pos], this.id,
                        availableDomainConcurrency, availableTypeConcurrency
                    );
                    await this.operations.updateJobs({_id}, {lockedBy: activeIds[pos]});
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

    scheduleJobs() {
        return this._scheduleJobs().catch(
            e => this.logger.warn('Scheduling jobs failed in scheduler ', this.id, ': ', e)
        );
    }

    async _scheduleJobs() {
        debug('schedule jobs: %o', {lockedBy: this.id, status: 'PENDING'});
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

    _computeTaskNextTime(task) {
        const {nextTime: lastTime, mode, interval, schedule, timezone} = task;
        let nextTime;
        if (mode === 'ONCE') {
            nextTime = null;
        } else if (mode === 'REPEATED') {
            nextTime = new Date(lastTime.getTime() + interval * 1000);
        } else if (mode === 'SCHEDULED') {

            // the following is copied from agenda/job/compute-next-run-at.js

            const dateForTimezone = date => {
                date = moment(date);
                if (timezone) {
                    date.tz(timezone);
                }
                return date;
            };
            const lastTime_ = dateForTimezone(lastTime || new Date());
            const cronTime = new CronTime(schedule);
            let nextDate = cronTime._getNextDateFrom(lastTime_);
            if (nextDate.valueOf() === lastTime_.valueOf()) {
                // Handle cronTime giving back the same date for the next run time
                nextDate = cronTime._getNextDateFrom(dateForTimezone(new Date(lastTime_.valueOf() + 1001)));
            }
            nextTime = nextDate.toDate();
        }
        return {lastTime, nextTime};
    }

}


module.exports = Scheduler;
