const {MongoClient} = require('mongodb');
const {sleep} = require('@raychee/utils');

const {Logger} = require('../lib/logger');
const Operations = require('../lib/operations');
const {TaskDomain, TaskType, Job} = require('../lib/task');
const {JobWatcherManager} = require('../lib/watcher');


describe('Job', () => {

    let connection, db;
    /** @type Operations */
    let operations, jobWatcher;

    beforeAll(async () => {
        connection = await MongoClient.connect(process.env.MONGO_URL, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        db = await connection.db('test_task');
        operations = new Operations(new Logger('Operations'), db, {
            async getTaskType() {
                return {
                    validate() {
                    }
                };
            }
        });
        jobWatcher = new JobWatcherManager(operations.jobs).getWatcher(['SUCCESS', 'FAILED', 'CANCELED']);
    });

    afterAll(async () => {
        jobWatcher.close();
        jobWatcher.abortAll(new Error('test is finished'));
        await connection.close();
        await db.close();
    });

    test('run a successful job trial with 2 retries', async () => {

        const taskDomain = new TaskDomain('domain1');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run({v}, ctx) {
                ctx.i = (ctx.i || 0) + 1;
                if (ctx.i >= 3) {
                    await this.schedule('domain1.type2', {id: this.config._id, vv: v + 1});
                } else {
                    this.fail('test_code', 'fail ', ctx.i, ' times');
                }
            },
            async catch() {
                await this.schedule('domain1.type3');
            }
        });

        await operations.ensureDomain('domain1');
        await operations.ensureType('domain1', 'type1');
        await operations.ensureType('domain1', 'type2');
        await operations.ensureType('domain1', 'type3');
        await operations.updateDomains({domain: 'domain1'}, {retry: 2});
        const taskConfig = await operations.insertTask({
            domain: 'domain1', type: 'type1', params: {v: 1}, mode: 'ONCE'
        });
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        expect(jobConfig.retry).toBe(2);

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);
        let timeStarted = undefined, timeStopped = undefined;
        for (let trial = 0; trial <= job.config.retry; trial++) {
            job._timeout = false;
            job.config.trials.push({});
            await job._executeTrial();
            const {timeStarted: t1, timeStopped: t2, ...t} = job.config.trials[trial];
            const scheduled1 = await operations.jobs.findOne({domain: 'domain1', type: 'type2'});
            const scheduled2 = await operations.jobs.findOne({domain: 'domain1', type: 'type3'});
            if (!timeStarted) timeStarted = job.config.timeStarted;
            if (!timeStopped) timeStopped = job.config.timeStopped;

            expect(timeStarted).toBeDefined();
            expect(job.config.trials.length).toBe(trial + 1);
            expect(t1).toBeDefined();
            expect(t2).toBeDefined();
            expect(t2.getTime()).toBeGreaterThanOrEqual(t1.getTime());
            expect(scheduled2).toBeNull();

            if (trial === 0) {
                expect(t1.getTime()).toBe(timeStarted.getTime());
            } else {
                expect(t1.getTime()).toBeGreaterThan(timeStarted.getTime());
            }

            if (trial < job.config.retry) {
                expect(job.config.status).toBe('RUNNING');
                expect(t).toStrictEqual({
                    status: 'FAILED', fail: {code: 'test_code', message: `fail ${trial + 1} times`},
                    context: {i: trial + 1}, delay: 0,
                });
                expect(timeStopped).toBeUndefined();
                expect(job.config.fail).toStrictEqual({code: 'test_code', message: `fail ${trial + 1} times`});
                expect(scheduled1).toBeNull();
            } else {
                expect(job.config.status).toBe('SUCCESS');
                expect(t).toStrictEqual({status: 'SUCCESS', context: {i: trial + 1}, delay: 0});
                expect(timeStopped).toBeDefined();
                expect(scheduled1).toMatchObject({
                    domain: 'domain1', type: 'type2', params: {id: job.config._id, vv: 2}, retry: 2,
                    task: taskConfig._id, createdBy: jobConfig._id, createdFrom: jobConfig._id,
                });
            }
        }

    });

    test('run a catchable job that finally crashes', async () => {

        const taskDomain = new TaskDomain('domain2');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run({v}, ctx) {
                ctx.i = (ctx.i || 0) + 1;
                if (ctx.i >= 3) {
                    throw new Error('crash');
                } else {
                    throw new Error('');
                }
            },
            async catch(e) {
                await this.schedule('domain2.type2', {e: e.message});
                if (!e.message) {
                    return 'caught';
                }
            }
        });

        await operations.ensureDomain('domain2');
        await operations.ensureType('domain2', 'type1');
        await operations.ensureType('domain2', 'type2');
        await operations.updateDomains({domain: 'domain2'}, {retry: 3});
        const taskConfig = await operations.insertTask({
            domain: 'domain2', type: 'type1', params: {v: 1}, mode: 'ONCE'
        });
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        expect(jobConfig.retry).toBe(3);

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        for (let trial = 0; trial <= 2; trial++) {
            job._timeout = false;
            job.config.trials.push({});
            try {
                await job._executeTrial();
            } catch (e) {
                expect(trial).toBe(2);
                expect(e.name).toBe('CatchableError');
                expect(e.error.message).toBe('crash');
            }
            const {timeStarted: t1, timeStopped: t2, ...t} = job.config.trials[trial];
            const scheduled = await operations.jobs.find({domain: 'domain2', type: 'type2'}).toArray();

            expect(scheduled).toHaveLength(trial + 1);

            if (trial < 2) {
                expect(job.config.status).toBe('RUNNING');
                expect(t).toStrictEqual({
                    status: 'FAILED', fail: {code: '_catch', message: 'caught'},
                    context: {i: trial + 1}, delay: 0,
                });
                expect(job.config.fail).toStrictEqual({code: '_catch', message: 'caught'});
                expect(scheduled[trial]).toMatchObject({domain: 'domain2', type: 'type2', params: {e: ''}, retry: 3});
            } else {
                expect(job.config.status).toBe('FAILED');
                expect(t).toStrictEqual({
                    status: 'FAILED', fail: {code: '_crash', message: 'crash'},
                    context: {i: trial + 1}, delay: 0,
                });
                expect(job.config.fail).toStrictEqual({code: '_crash', message: 'crash'});
                expect(scheduled[trial]).toMatchObject({
                    domain: 'domain2',
                    type: 'type2',
                    params: {e: 'crash'},
                    retry: 3
                });
            }
        }

    });

    test('run a long job that gets canceled in the middle', async () => {

        const taskDomain = new TaskDomain('domain3');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run() {
                await this.delay(2);
            }
        });

        await operations.ensureDomain('domain3');
        await operations.ensureType('domain3', 'type1');
        const taskConfig = await operations.insertTask({domain: 'domain3', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        const execute = job._execute();
        const cancelJob = (async () => {
            await sleep(500);
            await operations.updateJobs({_id: jobConfig._id}, {status: 'CANCELED'});
        })();
        let throwError = true;
        try {
            await Promise.all([execute, cancelJob]);
            throwError = false;
        } catch (e) {
            expect(e.name).toBe('JobCancellation');
            expect(job.config).toMatchObject({
                status: 'CANCELED',
                fail: {code: '_cancel', message: 'Job is canceled due to manual intervention or task being disabled.'},
            });
        }
        expect(throwError).toBeTruthy();
    });

    test('run a long job that gets interrupted in the middle', async () => {
        const taskDomain = new TaskDomain('domain4');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run() {
                await this.delay(2);
            }
        });

        await operations.ensureDomain('domain4');
        await operations.ensureType('domain4', 'type1');
        const taskConfig = await operations.insertTask({domain: 'domain4', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        const execute = job._execute();
        const interruptJob = (async () => {
            await sleep(500);
            await operations.updateJobs({_id: jobConfig._id}, {status: 'PENDING'});
        })();
        let throwError = true;
        try {
            await Promise.all([execute, interruptJob]);
            throwError = false;
        } catch (e) {
            expect(e.name).toBe('JobInterruption');
            expect(job.config.status).toBe('PENDING');
            expect(job.config.fail).toBeUndefined();
            expect(job.config.trials).toHaveLength(1);
            expect(job.config.trials[0]).toMatchObject({status: 'RUNNING', delay: 0});
        }
        expect(throwError).toBeTruthy();
    });

    test('run a delayed job that gets changed delay interval in the middle', async () => {
        const taskDomain = new TaskDomain('domain5');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({});

        await operations.ensureDomain('domain5');
        await operations.ensureType('domain5', 'type1');
        await operations.updateDomains({domain: 'domain5'}, {delay: 2});
        const taskConfig = await operations.insertTask({domain: 'domain5', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        const execute = job._execute();
        const changeDelay = (async () => {
            await sleep(200);
            await operations.updateDomains({domain: 'domain5'}, {delay: 3});
        })();
        await Promise.all([execute, changeDelay]);
        expect(job.config.status).toBe('SUCCESS');
        expect(job.config.fail).toBeUndefined();
        expect(job.config.trials).toHaveLength(1);
        expect(job.config.trials[0]).toMatchObject({status: 'SUCCESS', delay: 3});
    });

    test('run a long job that timeouts', async () => {
        const taskDomain = new TaskDomain('domain6');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run() {
                if (this.config.trials.length <= 1) {
                    await this.delay(2);
                }
            }
        });

        await operations.ensureDomain('domain6');
        await operations.ensureType('domain6', 'type1');
        await operations.updateDomains({domain: 'domain6'}, {retry: 3, timeout: 1});
        const taskConfig = await operations.insertTask({domain: 'domain6', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        await job._execute();
        expect(job.config.status).toBe('SUCCESS');
        expect(job.config.trials).toHaveLength(2);
        expect(job.config.trials[0]).toMatchObject({
            status: 'FAILED', delay: 0,
            fail: {code: '_timeout', message: 'Job execution time exceeds 1 seconds and is terminated.'}
        });
        expect(job.config.trials[1]).toMatchObject({
            status: 'SUCCESS', delay: 0,
        });
    });

    test('run a job that waits for another job that succeeds', async () => {
        const taskDomain = new TaskDomain('domain7');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run() {
                const newJob = await this.schedule('domain7.type2', {v: 1});
                const finishedJob = await this.wait(newJob);
                await this.schedule('domain7.type3', {finished: finishedJob});
            }
        });

        await operations.ensureDomain('domain7');
        await operations.ensureType('domain7', 'type1');
        await operations.ensureType('domain7', 'type2');
        await operations.ensureType('domain7', 'type3');
        const taskConfig = await operations.insertTask({domain: 'domain7', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        const running = job._execute();
        await sleep(1500);
        expect(job.config.status).toBe('RUNNING');
        const scheduled1 = await operations.jobs.find({domain: 'domain7', type: 'type2'}).toArray();
        expect(scheduled1).toHaveLength(1);
        expect(scheduled1[0].params).toStrictEqual({v: 1});
        expect(scheduled1[0].status).toBe('PENDING');
        await operations.updateJobs({_id: scheduled1[0]._id}, {status: 'SUCCESS'});
        await running;
        expect(job.config.status).toBe('SUCCESS');
        const scheduled2 = await operations.jobs.find({domain: 'domain7', type: 'type3'}).toArray();
        expect(scheduled2).toHaveLength(1);
        expect(scheduled2[0].params.finished).toMatchObject({_id: scheduled1[0]._id, params: {v: 1}, status: 'SUCCESS'});
    });

    test('run a job that waits for another job that fails', async () => {
        const taskDomain = new TaskDomain('domain8');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run() {
                const newJob = await this.schedule('domain8.type2', {v: 1});
                const finishedJob = await this.wait(newJob);
                await this.schedule('domain8.type3', {finished: finishedJob});
            }
        });

        await operations.ensureDomain('domain8');
        await operations.ensureType('domain8', 'type1');
        await operations.ensureType('domain8', 'type2');
        await operations.ensureType('domain8', 'type3');
        const taskConfig = await operations.insertTask({domain: 'domain8', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        const running = job._execute();
        await sleep(1500);
        expect(job.config.status).toBe('RUNNING');
        const scheduled1 = await operations.jobs.find({domain: 'domain8', type: 'type2'}).toArray();
        expect(scheduled1).toHaveLength(1);
        expect(scheduled1[0].params).toStrictEqual({v: 1});
        expect(scheduled1[0].status).toBe('PENDING');
        await operations.updateJobs({_id: scheduled1[0]._id}, {status: 'FAILED'});
        await running;
        expect(job.config.status).toBe('FAILED');
        const scheduled2 = await operations.jobs.find({domain: 'domain8', type: 'type3'}).toArray();
        expect(scheduled2).toHaveLength(0);
    });

    test('run a job that checks for history', async () => {
        const taskDomain = new TaskDomain('domain9');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run({}, ctx) {
                const jobs = await this.history({
                    type: 'type1', timeStarted_gt: new Date('2020-01-01T00:00:00Z'),
                    status_in: ['SUCCESS', 'PENDING'],
                });
                ctx.jobs = jobs;
            }
        });

        await operations.ensureDomain('domain9');
        await operations.ensureType('domain9', 'type1');
        await operations.ensureType('domain9', 'type2');
        const taskConfig = await operations.insertTask({domain: 'domain9', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});
        await operations.insertJob({task: taskConfig._id, type: 'type2'});
        await operations.insertJob({task: taskConfig._id, timeStarted: new Date('2019-01-01T00:00:00Z')});
        const j = await operations.insertJob({task: taskConfig._id, timeStarted: new Date('2020-01-02T00:00:00Z'), status: 'SUCCESS'});
        await operations.insertJob({task: taskConfig._id, timeStarted: new Date('2020-01-03T00:00:00Z'), status: 'FAILED'});

        const job = new Job(jobConfig, taskType, jobWatcher, undefined, operations);

        await job._execute();
        expect(job.config.status).toBe('SUCCESS');
        expect(job.config.context.jobs).toHaveLength(1);
        expect(job.config.context.jobs[0]).toStrictEqual(j);
    });

});
