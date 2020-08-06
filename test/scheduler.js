const {describe, beforeAll, beforeEach, afterAll, test, expect} = require('@jest/globals');

const {MongoClient} = require('mongodb');
const {sleep} = require('@raychee/utils');

const {Logger} = require('../lib/logger');
const {TaskDomain, TaskType} = require('../lib/task');
const Operations = require('../lib/operations');
const Scheduler = require('../lib/scheduler');


describe('Scheduler', () => {

    let connection, db;
    /** @type Operations */
    let operations;

    beforeAll(async () => {
        connection = await MongoClient.connect(process.env.MONGO_URL, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        db = await connection.db('test_scheduler');
        const taskLoader = {
            getAllTaskDomains() {
                return [];
            },
            getAllTaskTypes() {
                return [];
            },
            async getTaskType() {
                return {
                    validate() {
                    }
                };
            }
        };
        operations = new Operations(new Logger('Operations'), db, taskLoader);
    });

    afterAll(async () => {
        await connection.close();
        await db.close();
    });

    beforeEach(async () => {
        await db.dropDatabase();
        await operations.prepare();
    });

    test('heart beat', async () => {
        const scheduler = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );

        scheduler.isRunning = true;
        await scheduler.activate();
        for (let i = 0; i < 3; i++) {
            try {
                await sleep(100);
                await scheduler.heartbeat();
            } catch (e) {
                expect(e).toBeUndefined();
            }
        }
        await sleep(1100);
        try {
            await scheduler.heartbeat();
        } catch (e) {
            expect(e.name).toBe('HeartAttack');
        }
    });

    test('clear dead schedulers', async () => {
        const {insertedId: s1} = await operations.schedulers.insertOne({heartbeat: new Date(0)});
        const {insertedId: s2} = await operations.schedulers.insertOne({heartbeat: new Date(0)});
        const {insertedId: s3} = await operations.schedulers.insertOne({heartbeat: new Date(0)});

        const scheduler1 = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );
        const scheduler2 = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );
        scheduler1.isRunning = true;
        await scheduler1.activate();
        scheduler2.isRunning = true;
        await scheduler2.activate();

        const {insertedId: t1} = await operations.tasks.insertOne({lockedBy: s1});
        const {insertedId: t2} = await operations.tasks.insertOne({lockedBy: s1});
        const {insertedId: t3} = await operations.tasks.insertOne({lockedBy: s2});
        const {insertedId: t4} = await operations.tasks.insertOne({lockedBy: scheduler1.id});
        const {insertedId: t5} = await operations.tasks.insertOne({lockedBy: scheduler2.id});
        const {insertedId: j1} = await operations.jobs.insertOne({lockedBy: s2, status: 'DELAYED'});
        const {insertedId: j2} = await operations.jobs.insertOne({lockedBy: s3, status: 'RUNNING'});
        const {insertedId: j3} = await operations.jobs.insertOne({lockedBy: s3, status: 'SUCCESS'});
        const {insertedId: j4} = await operations.jobs.insertOne({lockedBy: scheduler1.id, status: 'RUNNING'});
        const {insertedId: j5} = await operations.jobs.insertOne({lockedBy: scheduler2.id, status: 'PENDING'});
        const {insertedId: j6} = await operations.jobs.insertOne({status: 'FAILED'});
        const {insertedId: j7} = await operations.jobs.insertOne({status: 'DELAYED'});

        for (const scheduler of [scheduler1, scheduler2]) {
            await scheduler.clearDeadSchedulers();
            expect(await operations.tasks.findOne({_id: t1})).toStrictEqual({_id: t1, lockedBy: null});
            expect(await operations.tasks.findOne({_id: t2})).toStrictEqual({_id: t2, lockedBy: null});
            expect(await operations.tasks.findOne({_id: t3})).toStrictEqual({_id: t3, lockedBy: null});
            expect(await operations.tasks.findOne({_id: t4})).toStrictEqual({_id: t4, lockedBy: scheduler1.id});
            expect(await operations.tasks.findOne({_id: t5})).toStrictEqual({_id: t5, lockedBy: scheduler2.id});
            expect(await operations.jobs.findOne({_id: j1})).toStrictEqual({_id: j1, lockedBy: null, status: 'PENDING', local: {status: 'PENDING'}});
            expect(await operations.jobs.findOne({_id: j2})).toStrictEqual({_id: j2, lockedBy: null, status: 'PENDING', local: {status: 'PENDING'}});
            expect(await operations.jobs.findOne({_id: j3})).toStrictEqual({_id: j3, lockedBy: s3, status: 'SUCCESS'});
            expect(await operations.jobs.findOne({_id: j4})).toStrictEqual({_id: j4, lockedBy: scheduler1.id, status: 'RUNNING'});
            expect(await operations.jobs.findOne({_id: j5})).toStrictEqual({_id: j5, lockedBy: scheduler2.id, status: 'PENDING'});
            expect(await operations.jobs.findOne({_id: j6})).toStrictEqual({_id: j6, status: 'FAILED'});
            expect(await operations.jobs.findOne({_id: j7})).toStrictEqual({_id: j7, status: 'DELAYED'});
        }

        await scheduler1.deactivate();

        expect(await operations.tasks.findOne({_id: t1})).toStrictEqual({_id: t1, lockedBy: null});
        expect(await operations.tasks.findOne({_id: t2})).toStrictEqual({_id: t2, lockedBy: null});
        expect(await operations.tasks.findOne({_id: t3})).toStrictEqual({_id: t3, lockedBy: null});
        expect(await operations.tasks.findOne({_id: t4})).toStrictEqual({_id: t4, lockedBy: null});
        expect(await operations.tasks.findOne({_id: t5})).toStrictEqual({_id: t5, lockedBy: scheduler2.id});
        expect(await operations.jobs.findOne({_id: j1})).toStrictEqual({_id: j1, lockedBy: null, status: 'PENDING', local: {status: 'PENDING'}});
        expect(await operations.jobs.findOne({_id: j2})).toStrictEqual({_id: j2, lockedBy: null, status: 'PENDING', local: {status: 'PENDING'}});
        expect(await operations.jobs.findOne({_id: j3})).toStrictEqual({_id: j3, lockedBy: s3, status: 'SUCCESS'});
        expect(await operations.jobs.findOne({_id: j4})).toStrictEqual({_id: j4, lockedBy: null, status: 'PENDING', local: {status: 'PENDING'}});
        expect(await operations.jobs.findOne({_id: j5})).toStrictEqual({_id: j5, lockedBy: scheduler2.id, status: 'PENDING'});
        expect(await operations.jobs.findOne({_id: j6})).toStrictEqual({_id: j6, status: 'FAILED'});
        expect(await operations.jobs.findOne({_id: j7})).toStrictEqual({_id: j7, status: 'DELAYED'});

    });

    test('dispatch jobs', async () => {
        await operations.ensureDomain('domainCurrency3');
        await operations.updateDomains({domain: 'domainCurrency3'}, {maxConcurrency: 3});
        await operations.ensureType('domainCurrency3', 'typeCurrency1');
        await operations.updateTypes({domain: 'domainCurrency3', type: 'typeCurrency1'}, {concurrency: 1});
        await operations.ensureType('domainCurrency3', 'typeCurrency4');
        await operations.updateTypes({domain: 'domainCurrency3', type: 'typeCurrency4'}, {concurrency: 4});

        await operations.ensureDomain('domainCurrency100');
        await operations.updateDomains({domain: 'domainCurrency100'}, {maxConcurrency: 100});
        await operations.ensureType('domainCurrency100', 'typeCurrency2');
        await operations.updateTypes({domain: 'domainCurrency100', type: 'typeCurrency2'}, {concurrency: 2});

        const scheduler1 = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );
        const scheduler2 = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );
        scheduler1.isRunning = true;
        await scheduler1.activate();

        const {insertedId: j1} = await operations.jobs.insertOne({status: 'RUNNING', domain: 'domainCurrency3', type: 'typeCurrency4', priority: 0, lockedBy: scheduler1.id});
        const {insertedId: j2} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency4', priority: -1});
        const {insertedId: j3} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency4', priority: 0});
        const {insertedId: j4} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency1', priority: 1});
        const {insertedId: j5} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency1', priority: 2});
        const {insertedId: j6} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency100', type: 'typeCurrency2', priority: 3});
        const {insertedId: j7} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency100', type: 'typeCurrency2', priority: 2});
        const {insertedId: j8} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency100', type: 'typeCurrency2', priority: 1});

        const ret = await Promise.all([scheduler1.dispatchJobs(), scheduler2.dispatchJobs()]);

        expect(ret.filter(r => r)).toHaveLength(1);

        expect((await operations.jobs.findOne({_id: j1})).lockedBy).toEqual(scheduler1.id);
        expect((await operations.jobs.findOne({_id: j2})).lockedBy).toBeUndefined();
        expect((await operations.jobs.findOne({_id: j3})).lockedBy).toEqual(scheduler1.id);
        expect((await operations.jobs.findOne({_id: j4})).lockedBy).toBeUndefined();
        expect((await operations.jobs.findOne({_id: j5})).lockedBy).toEqual(scheduler1.id);
        expect((await operations.jobs.findOne({_id: j6})).lockedBy).toEqual(scheduler1.id);
        expect((await operations.jobs.findOne({_id: j7})).lockedBy).toEqual(scheduler1.id);
        expect((await operations.jobs.findOne({_id: j8})).lockedBy).toBeUndefined();

    });

    test('schedule tasks', async () => {
        await operations.ensureDomain('domain');
        await operations.ensureType('domain', 'type');

        let t1 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'ONCE'
        });
        let t2 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'REPEATED', interval: 10
        });
        let t3 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *'
        });
        let t4 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *',
            validBefore: new Date(Date.now() - 10000)
        });
        let t5 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *',
            validAfter: new Date(Date.now() + 10000)
        });
        let t6 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *',
            validAfter: new Date(Date.now() + 10000), enabled: false
        });
        let t7LastTime = new Date(Date.now() - 30 * 60 * 1000);
        let t7 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *',
            lastTime: t7LastTime,
        });
        let t8LastTime = new Date();
        t8LastTime.setSeconds(0, 0);
        let t8NextTime = new Date(t8LastTime.getTime() + 60 * 1000);
        let t8 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'REPEATED', interval: 60,
            lastTime: new Date(t8LastTime.getTime() - 3 * 60 * 1000),
        });

        const scheduler = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );

        scheduler.isRunning = true;
        await scheduler.activate();
        await scheduler.scheduleTasks();

        const now = new Date();
        const minutes = now.getMinutes();
        let nextTime = new Date(now.getTime());
        nextTime.setMinutes(10, 0, 0);
        if (minutes >= 10) {
            nextTime = new Date(nextTime.getTime() + 60 * 60 * 1000);
        }
        if (nextTime - 60 * 60 * 1000 > t7LastTime) {
            t7LastTime = new Date(nextTime - 60 * 60 * 1000);
        }

        t1 = await operations.tasks.findOne({_id: t1._id});
        expect(t1).toMatchObject({lockedBy: null, nextTime: null});
        expect(Math.abs(t1.lastTime - now)).toBeLessThan(1000);

        t2 = await operations.tasks.findOne({_id: t2._id});
        expect(t2).toMatchObject({lockedBy: null});
        expect(Math.abs(t2.lastTime - now)).toBeLessThan(1000);
        expect(Math.abs(t2.nextTime - 10 * 1000 - now)).toBeLessThan(1000);

        t3 = await operations.tasks.findOne({_id: t3._id});
        expect(t3).toMatchObject({lockedBy: null, nextTime});
        expect(Math.abs(t3.lastTime - now)).toBeLessThan(1000);

        expect(await operations.tasks.findOne({_id: t4._id})).toMatchObject({nextTime: t4.nextTime});
        expect(await operations.tasks.findOne({_id: t5._id})).toMatchObject({nextTime: t5.nextTime});
        expect(await operations.tasks.findOne({_id: t6._id})).toMatchObject({nextTime: t6.nextTime});
        expect(await operations.tasks.findOne({_id: t7._id})).toMatchObject({lastTime: t7LastTime, nextTime});
        expect(await operations.tasks.findOne({_id: t8._id})).toMatchObject({lastTime: t8LastTime, nextTime: t8NextTime});

        expect(await operations.jobs.countDocuments({task: t1._id})).toBe(1);
        expect(await operations.jobs.countDocuments({task: t2._id})).toBe(1);
        expect(await operations.jobs.countDocuments({task: t3._id})).toBe(0);
        expect(await operations.jobs.countDocuments({task: t4._id})).toBe(0);
        expect(await operations.jobs.countDocuments({task: t5._id})).toBe(0);
        expect(await operations.jobs.countDocuments({task: t6._id})).toBe(0);
        expect(await operations.jobs.countDocuments({task: t7._id})).toBe(0);
        expect(await operations.jobs.countDocuments({task: t8._id})).toBe(0);

    });

    test('schedule jobs', async () => {
        await operations.ensureDomain('domain');
        await operations.ensureType('domain', 'type');
        await operations.updateDomains({domain: 'domain'}, {retry: 3});

        const taskDomain = new TaskDomain('domain');
        const taskType = new TaskType(taskDomain, 'type');
        await taskType.load({
            async run() {
                await this.schedule('domain.type', {v: 'scheduled'});
            },
        });
        const taskLoader = {
            async getTaskType() {
                return taskType;
            }
        };
        const scheduler = new Scheduler(
            new Logger('Scheduler'), operations, taskLoader, {heartbeat: 0.1, heartAttack: 1}
        );
        scheduler.isRunning = true;
        await scheduler.activate();
        scheduler.running = Promise.resolve();

        let t1 = await operations.insertTask({domain: 'domain', type: 'type', mode: 'ONCE'});
        let j1 = await operations.insertJob({task: t1._id});
        await operations.updateJobs({_id: j1._id}, {trials: [{status: 'FAILED'}, {status: 'FAILED'}], lockedBy: scheduler.id});
        j1 = await operations.jobs.findOne({_id: j1._id});

        await scheduler._executeJob(j1);

        const j2 = await operations.jobs.findOne({_id: j1._id});
        expect(j2).toMatchObject({status: 'SUCCESS', lockedBy: scheduler.id});
        expect(j2.trials).toHaveLength(2);
        expect(j2.trials[0]).toStrictEqual({status: 'FAILED'});
        expect(j2.trials[1]).toMatchObject({status: 'SUCCESS', delay: 0});

        const j3 = await operations.jobs.findOne({'params.v': 'scheduled'});
        expect(j3).toBeTruthy();

    });

    test('compute next time', async () => {
        const scheduler = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );

        expect(scheduler._computeTaskNextTime({
            nextTime: new Date('2020-01-01T00:10:01.000Z'), mode: 'SCHEDULED', schedule: '* 10 * * * *'
        })).toMatchObject({nextTime: new Date('2020-01-01T00:10:02.000Z')});

        expect(scheduler._computeTaskNextTime({
            nextTime: new Date('2020-01-01T00:10:59.000Z'), mode: 'SCHEDULED', schedule: '* 10 * * * *'
        })).toMatchObject({nextTime: new Date('2020-01-01T01:10:00.000Z')});

        expect(scheduler._computeTaskNextTime({
            nextTime: new Date('2020-01-01T00:10:01.000Z'), mode: 'SCHEDULED', schedule: '0 10 * * * *'
        })).toMatchObject({nextTime: new Date('2020-01-01T01:10:00.000Z')});

    });

});
