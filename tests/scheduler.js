const {MongoClient} = require('mongodb');
const {sleep} = require('@raychee/utils');

const {Logger} = require('../lib/logger');
const Operations = require('../lib/operations');
const Scheduler = require('../lib/scheduler');


describe('Scheduler', () => {

    let connection, db, operations;

    beforeAll(async () => {
        connection = await MongoClient.connect(process.env.MONGO_URL, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        db = await connection.db('test_scheduler');
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

    beforeEach(async () => {
        await db.dropDatabase();
        await operations.prepare();
    });

    test('heart beat', async () => {
        const scheduler = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );

        await scheduler._activate();
        for (let i = 0; i < 3; i++) {
            try {
                await sleep(100);
                await scheduler._heartbeat();
            } catch (e) {
                expect(e).toBeUndefined();
            }
        }
        await sleep(1100);
        try {
            await scheduler._heartbeat();
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
        await scheduler1._activate();
        await scheduler2._activate();

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
            await scheduler._clearDeadSchedulers();
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

        await scheduler1.stop();
        await scheduler2._clearDeadSchedulers();

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
        await operations.ensureTaskDomainConfig('domainCurrency3');
        await operations.updateTaskDomainConfigs({domain: 'domainCurrency3'}, {maxConcurrency: 3});
        await operations.ensureTaskTypeConfig('domainCurrency3', 'typeCurrency1');
        await operations.updateTaskTypeConfigs({domain: 'domainCurrency3', type: 'typeCurrency1'}, {concurrency: 1});
        await operations.ensureTaskTypeConfig('domainCurrency3', 'typeCurrency4');
        await operations.updateTaskTypeConfigs({domain: 'domainCurrency3', type: 'typeCurrency4'}, {concurrency: 4});

        await operations.ensureTaskDomainConfig('domainCurrency100');
        await operations.updateTaskDomainConfigs({domain: 'domainCurrency100'}, {maxConcurrency: 100});
        await operations.ensureTaskTypeConfig('domainCurrency100', 'typeCurrency2');
        await operations.updateTaskTypeConfigs({domain: 'domainCurrency100', type: 'typeCurrency2'}, {concurrency: 2});

        const scheduler1 = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );
        const scheduler2 = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );
        await scheduler1._activate();

        const {insertedId: j1} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency4', priority: 0, lockedBy: scheduler1.id});
        const {insertedId: j2} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency4', priority: -1});
        const {insertedId: j3} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency4', priority: 0});
        const {insertedId: j4} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency1', priority: 1});
        const {insertedId: j5} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency3', type: 'typeCurrency1', priority: 2});
        const {insertedId: j6} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency100', type: 'typeCurrency2', priority: 3});
        const {insertedId: j7} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency100', type: 'typeCurrency2', priority: 2});
        const {insertedId: j8} = await operations.jobs.insertOne({status: 'PENDING', domain: 'domainCurrency100', type: 'typeCurrency2', priority: 1});

        const ret = await Promise.all([scheduler1._dispatchJobs(), scheduler2._dispatchJobs()]);

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
        await operations.ensureTaskDomainConfig('domain');
        await operations.ensureTaskTypeConfig('domain', 'type');

        const t1 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'ONCE'
        });
        const t2 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'REPEATED', interval: 10
        });
        const t3 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *'
        });
        const t4 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *',
            validBefore: new Date(Date.now() - 10000)
        });
        const t5 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *',
            validAfter: new Date(Date.now() + 10000)
        });
        const t6 = await operations.insertTask({
            domain: 'domain', type: 'type', mode: 'SCHEDULED', schedule: '0 10 * * * *',
            validAfter: new Date(Date.now() + 10000), enabled: false
        });

        const scheduler = new Scheduler(
            new Logger('Scheduler'), operations, {}, {heartbeat: 0.1, heartAttack: 1}
        );

        await scheduler._activate();

        const now = t3.nextTime;
        const minutes = now.getMinutes();
        let nextTime = new Date(now.getTime());
        nextTime.setMinutes(10, 0, 0);
        if (minutes >= 10) {
            nextTime = new Date(nextTime.getTime() + 60 * 60 * 1000);
        }
        await scheduler._scheduleTasks();

        expect(await operations.tasks.findOne({_id: t1._id})).toMatchObject({
            lockedBy: null, lastTime: t1.nextTime, nextTime: null
        });
        expect(await operations.tasks.findOne({_id: t2._id})).toMatchObject({
            lockedBy: null, lastTime: t2.nextTime, nextTime: new Date(t2.nextTime.getTime() + 10 * 1000)
        });
        expect(await operations.tasks.findOne({_id: t3._id})).toMatchObject({
            lockedBy: null, lastTime: t3.nextTime, nextTime
        });
        expect(await operations.tasks.findOne({_id: t4._id})).toMatchObject({nextTime: t4.nextTime});
        expect(await operations.tasks.findOne({_id: t5._id})).toMatchObject({nextTime: t5.nextTime});
        expect(await operations.tasks.findOne({_id: t6._id})).toMatchObject({nextTime: t6.nextTime});

        expect(await operations.jobs.countDocuments({task: t1._id})).toBe(1);
        expect(await operations.jobs.countDocuments({task: t2._id})).toBe(1);
        expect(await operations.jobs.countDocuments({task: t3._id})).toBe(1);

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