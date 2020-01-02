const {MongoClient} = require('mongodb');
const {sleep} = require('@raychee/utils');

const {Logger} = require('../lib/logger');
const Operations = require('../lib/operations');
const {TaskDomain, TaskType, Job} = require('../lib/task');


describe('Job', () => {

    let connection, db, operations;

    beforeAll(async () => {
        connection = await MongoClient.connect(process.env.MONGO_URL, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        db = await connection.db('test_task');
        operations = new Operations(new Logger('Operations'), db, {
            async get() {
                return {
                    validate() {
                    }
                };
            }
        });
    });

    afterAll(async () => {
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

        await operations.ensureTaskDomainConfig('domain1');
        await operations.ensureTaskTypeConfig('domain1', 'type1');
        await operations.ensureTaskTypeConfig('domain1', 'type2');
        await operations.ensureTaskTypeConfig('domain1', 'type3');
        await operations.updateTaskDomainConfigs({domain: 'domain1'}, {retry: 2});
        const taskConfig = await operations.insertTask({
            domain: 'domain1', type: 'type1', params: {v: 1}, mode: 'ONCE'
        });
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        expect(jobConfig.retry).toBe(2);

        const job = new Job(jobConfig, taskType, undefined, operations);
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
                expect(job.config.status).toBe('FAILED');
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

        await operations.ensureTaskDomainConfig('domain2');
        await operations.ensureTaskTypeConfig('domain2', 'type1');
        await operations.ensureTaskTypeConfig('domain2', 'type2');
        await operations.updateTaskDomainConfigs({domain: 'domain2'}, {retry: 2});
        const taskConfig = await operations.insertTask({
            domain: 'domain2', type: 'type1', params: {v: 1}, mode: 'ONCE'
        });
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        expect(jobConfig.retry).toBe(2);

        const job = new Job(jobConfig, taskType, undefined, operations);

        for (let trial = 0; trial <= job.config.retry; trial++) {
            job._timeout = false;
            job.config.trials.push({});
            try {
                await job._executeTrial();
            } catch (e) {
                expect(trial).toBe(job.config.retry);
                expect(e.message).toBe('crash');
            }
            const {timeStarted: t1, timeStopped: t2, ...t} = job.config.trials[trial];
            const scheduled = await operations.jobs.find({domain: 'domain2', type: 'type2'}).toArray();

            expect(job.config.status).toBe('FAILED');
            expect(scheduled).toHaveLength(trial + 1);

            if (trial < job.config.retry) {
                expect(t).toStrictEqual({
                    status: 'FAILED', fail: {code: '_catch', message: 'caught'},
                    context: {i: trial + 1}, delay: 0,
                });
                expect(job.config.fail).toStrictEqual({code: '_catch', message: 'caught'});
                expect(scheduled[trial]).toMatchObject({domain: 'domain2', type: 'type2', params: {e: ''}, retry: 2});
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
                    retry: 2
                });
            }
        }

    });

    test('run a long job that gets canceled in the middle', async () => {

        const taskDomain = new TaskDomain('domain3');
        const taskType = new TaskType(taskDomain, 'type1');
        await taskType.load({
            async run() {
                await this.delay(1);
            }
        });

        await operations.ensureTaskDomainConfig('domain3');
        await operations.ensureTaskTypeConfig('domain3', 'type1');
        const taskConfig = await operations.insertTask({domain: 'domain3', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, undefined, operations);

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
                await this.delay(1);
            }
        });

        await operations.ensureTaskDomainConfig('domain4');
        await operations.ensureTaskTypeConfig('domain4', 'type1');
        const taskConfig = await operations.insertTask({domain: 'domain4', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, undefined, operations);

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

        await operations.ensureTaskDomainConfig('domain5');
        await operations.ensureTaskTypeConfig('domain5', 'type1');
        await operations.updateTaskDomainConfigs({domain: 'domain5'}, {delay: 1});
        const taskConfig = await operations.insertTask({domain: 'domain5', type: 'type1', mode: 'ONCE'});
        const jobConfig = await operations.insertJob({task: taskConfig._id});

        const job = new Job(jobConfig, taskType, undefined, operations);

        const execute = job._execute();
        const changeDelay = (async () => {
            await sleep(200);
            await operations.updateTaskDomainConfigs({domain: 'domain5'}, {delay: 2});
        })();
        await Promise.all([execute, changeDelay]);
        expect(job.config.status).toBe('SUCCESS');
        expect(job.config.fail).toBeUndefined();
        expect(job.config.trials).toHaveLength(1);
        expect(job.config.trials[0]).toMatchObject({status: 'SUCCESS', delay: 2});
    });

});