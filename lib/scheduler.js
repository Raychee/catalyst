const debug = require('debug')('catalyst:scheduler');
const {set, get} = require('lodash');
const {CronTime} = require('cron');
const moment = require('moment');
const {sleep, dedup} = require("@raychee/utils");

const {CatchableError, HeartAttack} = require('./error');
const {Job} = require("./task");


class Scheduler {

    /**
     * @param {import('./logger').Logger} logger
     * @param {import('./operations').Operations} operations
     * @param {import('./loader').TaskLoader} taskLoader
     * @param {Object} options
     * @param {number} options.heartbeat
     * @param {number} options.heartAttack
     */
    constructor(logger, operations, taskLoader, options) {
        this.logger = logger;
        this.operations = operations;
        this.taskLoader = taskLoader;
        this.options = options;

        this.id = undefined;
        this.heartbeatAt = undefined;
        this.running = undefined;

        this._scheduleTasks = dedup(Scheduler.prototype._scheduleTasks.bind(this), {key: null});
    }

    get isActive() {
        return this.id && this.running;
    }

    start() {
        if (!this.running) {
            this.running = this._start();
        }
    }

    async stop() {
        const running = this.running;
        this.running = undefined;
        if (running) {
            await running;
        }
    }

    async _start() {
        await this._activate();
        while (this.running) {
            try {
                await this._heartbeat();
                await this._clearDeadSchedulers();
                await this._dispatchJobs();
                await this._scheduleTasks();
                await this._scheduleJobs();
            } catch (e) {
                if (e instanceof HeartAttack) {
                    throw e;
                }
                this.logger.warn('Heartbeat failure in scheduler ', this.id, ': ', e);
            }
            await sleep(this.options.heartbeat * 1000);
        }
        await this._stop();
    }

    async _activate() {
        const now = new Date();
        this.heartbeatAt = now;
        const {insertedId} = await this.operations.schedulers.insertOne({timeStarted: now, heartbeat: now});
        this.id = insertedId;
    }

    async _stop() {
        const now = new Date();
        const _id = this.id;
        this.id = undefined;
        await this.operations.schedulers.updateMany({_id}, {$set: {heartbeat: null, timeStopped: now}});
        await this._clearDeadSchedulers();
    }

    async _scheduleTasks(query = {}) {
        const now = new Date();
        const filter = {
            nextTime: {$lte: now}, lockedBy: null,
            enabled: true,
            validBefore: {$not: {$lt: now}},
            validAfter: {$not: {$gte: now}},
            ...query
        };
        debug('lock tasks: %o -> %o', filter, {lockedBy: this.id});
        await this.operations.updateTasks(filter, {lockedBy: this.id});
        for await (const task of this.operations.tasks.find({...query, lockedBy: this.id})) {
            const now = new Date();
            debug('create job from task %s', task._id);
            await this.operations.insertJob({task: task._id});
            while (task.nextTime && task.nextTime <= now) {
                const {lastTime, nextTime} = this._computeTaskNextTime(task);
                task.nextTime = nextTime;
                task.lastTime = lastTime;
            }
            const {_id, lastTime, nextTime} = task;
            await this.operations.updateTasks({_id}, {lastTime, nextTime, lockedBy: null});
        }
    }

    async _scheduleJobs() {
        debug('schedule jobs: %o', {lockedBy: this.id, status: 'PENDING'});
        for await (const {local, ...job} of this.operations.jobs.find({lockedBy: this.id, status: 'PENDING'})) {
            await this.operations.updateJobs({_id: job._id}, {status: 'DELAYED'});
            job.status = 'DELAYED';
            this._executeJob(job);
        }
    }

    async _executeJob(config) {
        const {_id, domain, type, trials = []} = config;
        debug('execute job %s', _id);
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
        const job = new Job(config, taskType, this, this.operations);
        try {
            await job._execute();
        } catch (e) {
            if (!(e instanceof CatchableError)) {
                this.logger.error('System crashed during job execution of ', job._id, ': ', e);
            }
        } finally {
            await this.operations.updateJobs({_id}, {lockedBy: null});
        }
    }

    async _heartbeat() {
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

    async _clearDeadSchedulers() {
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
            {$match: {lockedBy: {$ne: null}}},
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
                for await (const {_id, type} of this.operations.jobs.find(
                    {status: 'PENDING', domain, type: {$in: targetTypes}, lockedBy: null},
                    {sort: [['priority', -1], ['timeCreated', 1]]}
                )) {
                    debug('dispatch job %s to scheduler %s (this scheduler is %s)', _id, activeIds[pos], this.id);
                    await this.operations.updateJobs({_id}, {lockedBy: activeIds[pos]});
                    hasLocked = true;
                    pos++;
                    if (pos >= activeIds.length) pos = 0;
                    availableDomainConcurrency--;
                    if (availableDomainConcurrency <= 0) break;
                    availableTypeConcurrency[type]--;
                    if (availableTypeConcurrency[type] <= 0) break;
                }
                if (availableDomainConcurrency <= 0) break;
                if (!hasLocked) break;
            }
        }
        await this.operations.schedulers.updateOne(
            {_id: 0, lockedBy: this.id}, {$set: {lockedBy: null}},
        );
        return true;
    }

    async _getActiveSchedulerIds() {
        const active = await this.operations.schedulers.find({heartbeat: {$gt: new Date(0)}}).toArray();
        return active.map(s => s._id);
    }

    _computeTaskNextTime(task) {
        const {nextTime: lastTime, mode, interval, schedule, timezone} = task;
        let nextTime = null;
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