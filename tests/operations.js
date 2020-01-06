const {MongoClient} = require('mongodb');

const {Logger} = require('../lib/logger');
const Operations = require('../lib/operations');


describe('Operations', () => {

    let connection;
    let db;

    /** @type Operations */
    let operations;

    let domainA, domainB, taskTypeAa, taskTypeAb, taskTypeBx, taskBx, jobBx, jobAb, jobAa;

    beforeAll(async () => {

        const taskLoader = {
            async getTaskType() {
                return {
                    validate() {}
                };
            }
        };
        connection = await MongoClient.connect(process.env.MONGO_URL, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        db = await connection.db('test_operations');
        operations = new Operations(new Logger(), db, taskLoader);
    });

    afterAll(async () => {
        await connection.close();
        await db.close();
    });

    describe('ensure task domains and types', () => {
        test('', async () => {
            const config = {
                concurrency: 1, timeout: 0, delay: 0, delayRandomize: 0,
                retry: 0, retryDelayFactor: 1, priority: 0, dedupWithin: 0, dedupRecent: true,
            };

            for (const [domainName, typeName] of [
                ['A', 'a'],
                ['A', 'b'],
                ['B', 'x'],
            ]) {
                const {ctime: c1, mtime: m1, ...domain} = await operations.ensureDomain(domainName);
                expect(domain).toStrictEqual({domain: domainName, maxConcurrency: 1, ...config});
                const {ctime: c2, mtime: m2, ...type} = await operations.ensureType(domainName, typeName);
                expect(type).toStrictEqual({domain: domainName, type: typeName, ...config});
            }
            let taskType = await operations.types.findOne({domain: 'B', type: 'x'});
            const {_id: i1, ctime: c1, mtime: m1, local: l1, ...t1} = taskType;
            expect(t1).toStrictEqual({domain: 'B', type: 'x', ...config});
            taskTypeBx = taskType;

            taskType = await operations.types.findOne({domain: 'A', type: 'b'});
            const {_id: i2, ctime: c2, mtime: m2, local: l2, ...t2} = taskType;
            expect(t2).toStrictEqual({domain: 'A', type: 'b', ...config});
            taskTypeAb = taskType;

            taskType = await operations.types.findOne({domain: 'A', type: 'a'});
            const {_id: i5, ctime: c5, mtime: m5, local: l5, ...t5} = taskType;
            expect(t5).toStrictEqual({domain: 'A', type: 'a', ...config});
            taskTypeAa = taskType;

            let domain = await operations.domains.findOne({domain: 'A'});
            const {_id: i3, ctime: c3, mtime: m3, local: l3, ...d3} = domain;
            expect(d3).toStrictEqual({domain: 'A', maxConcurrency: 1, ...config});
            domainA = domain;

            domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i4, ctime: c4, mtime: m4, local: l4, ...d4} = domain;
            expect(d4).toStrictEqual({domain: 'B', maxConcurrency: 1, ...config});
            domainB = domain;
        });
    });

    describe('update task domains and types', () => {
        test('', async () => {
            await operations.updateTypes(
                {domain: 'B', type: 'x'}, {subTasks: [{domain: 'A', type: 'b', delay: 30}]}
            );
            let taskType = await operations.types.findOne({domain: 'B', type: 'x'});
            const {_id: i, ctime: c, mtime: m, local: l, ...t} = taskType;
            const {_id: i1, ctime: c1, mtime: m1, local: l1, ...t1} = taskTypeBx;
            expect(t).toStrictEqual({...t1, subTasks: [{domain: 'A', type: 'b', delay: 30}]});
            expect(l1).toBeUndefined();
            expect(l).toStrictEqual({subTasks: [{domain: 'A', type: 'b', delay: 30}]});
            taskTypeBx = taskType;

            await operations.updateTypes(
                {domain: 'A', type: 'b'}, {delay: 5}
            );
            taskType = await operations.types.findOne({domain: 'A', type: 'b'});
            const {_id: i2, ctime: c2, mtime: m2, local: l2, ...t2} = taskType;
            const {_id: i3, ctime: c3, mtime: m3, local: l3, ...t3} = taskTypeAb;
            expect(t2).toStrictEqual({...t3, delay: 5});
            expect(l3).toBeUndefined();
            expect(l2).toStrictEqual({delay: 5});
            taskTypeAb = taskType;

            await operations.updateDomains(
                {domain: 'B'}, {delay: 2}
            );
            let domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i4, ctime: c4, mtime: m4, local: l4, ...t4} = domain;
            const {_id: i5, ctime: c5, mtime: m5, local: l5, ...t5} = domainB;
            expect(t4).toStrictEqual({...t5, delay: 2});
            expect(l5).toBeUndefined();
            expect(l4).toStrictEqual({delay: 2});
            domainB = domain;

            taskType = await operations.types.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskTypeBx;
            expect(t6).toStrictEqual({...t7, delay: 2});
            expect(l6).toStrictEqual(l7);
            taskTypeBx = taskType;
        });
    });

    describe('insert a valid task', () => {
        test('', async () => {
            const now = new Date();
            taskBx = await operations.insertTask({
                domain: 'B', type: 'x', subTasks: [{domain: 'A', type: 'b', retry: 9}],
                retry: 8, validBefore: new Date('2019-01-01'), mode: 'ONCE',
                params: {p1: 123}
            });
            const {ctime: c1, mtime: m1, _id: i1, local: l1, nextTime: n1, ...t1} = taskBx;
            expect(t1).toStrictEqual({
                domain: 'B', type: 'x', subTasks: [{domain: 'A', type: 'b', delay: 30, retry: 9}],
                retry: 8, validBefore: new Date('2019-01-01'), mode: 'ONCE',
                enabled: true, params: {p1: 123}, context: {},
                timeout: 0, delay: 2, delayRandomize: 0, retryDelayFactor: 1,
                priority: 0, dedupWithin: 0, dedupRecent: true,
            });
            expect(i1).toBeTruthy();
            expect(l1).toBeUndefined();
            expect(n1.getTime()).toBeGreaterThanOrEqual(now.getTime());
            taskBx = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c2, mtime: m2, _id: i2, nextTime: n2, ...t2} = taskBx;
            expect(t2).toStrictEqual({
                domain: 'B', type: 'x', subTasks: [{domain: 'A', type: 'b', delay: 30, retry: 9}],
                retry: 8, validBefore: new Date('2019-01-01'), mode: 'ONCE',
                enabled: true, params: {p1: 123}, context: {},
                timeout: 0, delay: 2, delayRandomize: 0, retryDelayFactor: 1,
                priority: 0, dedupWithin: 0, dedupRecent: true,
                local: {
                    domain: 'B', type: 'x',
                    enabled: true, params: {p1: 123}, context: {}, validBefore: new Date('2019-01-01'),
                    subTasks: [{domain: 'A', type: 'b', retry: 9}], mode: 'ONCE', retry: 8,
                    nextTime: n2,
                }
            });
        });
    });

    describe('insert a valid job', () => {
        test('', async () => {
            const now = new Date();
            let job = await operations.insertJob({task: taskBx._id});
            const {_id: i1, timeCreated: t1, local: l1, ...j1} = job;
            expect(j1).toStrictEqual({
                domain: 'B', type: 'x', delay: 2, params: {p1: 123}, context: {}, trials: [], status: 'PENDING',
                task: taskBx._id, retry: 8, timeout: 0, delayRandomize: 0, retryDelayFactor: 1,
                priority: 0, dedupWithin: 0, dedupRecent: true,
            });
            expect(i1).toBeTruthy();
            expect(t1.getTime()).toBeGreaterThanOrEqual(now.getTime());
            expect(l1).toBeUndefined();

            jobBx = await operations.jobs.findOne({_id: i1});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = jobBx;
            expect(j2).toStrictEqual(j1);
            expect(l2).toStrictEqual({
                domain: 'B', type: 'x', context: {}, task: taskBx._id, status: 'PENDING', trials: []
            });
            expect(i2).toEqual(i1);
            expect(t21.getTime()).toBe(t1.getTime());
            expect(t22.getTime()).toBe(t1.getTime());

            job = await operations.insertJob({task: taskBx._id, domain: 'A', type: 'a', params: {}});
            const {_id: i3, timeCreated: t3, local: l3, ...j3} = job;
            expect(j3).toStrictEqual({
                domain: 'A', type: 'a', delay: 0, params: {}, context: {}, trials: [], status: 'PENDING',
                task: taskBx._id, retry: 0, timeout: 0, delayRandomize: 0, retryDelayFactor: 1,
                priority: 0, dedupWithin: 0, dedupRecent: true,
            });
            expect(i3).toBeTruthy();
            expect(t3.getTime()).toBeGreaterThanOrEqual(now.getTime());
            expect(l3).toBeUndefined();

            jobAa = await operations.jobs.findOne({_id: i3});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = jobAa;
            expect(j4).toStrictEqual(j3);
            expect(l4).toStrictEqual({
                domain: 'A', type: 'a', params: {}, context: {}, task: taskBx._id, status: 'PENDING', trials: []
            });
            expect(i4).toEqual(i3);
            expect(t41.getTime()).toBe(t3.getTime());
            expect(t42.getTime()).toBe(t3.getTime());
        });
    });

    describe('insert a valid job with subTask override', () => {
        test('', async () => {
            const now = new Date();

            const job = await operations.insertJob({task: taskBx._id, domain: 'A', type :'b', params: {}});
            const {_id: i1, timeCreated: t1, local: l1, ...j1} = job;
            expect(j1).toStrictEqual({
                domain: 'A', type :'b', delay: 30, params: {}, context: {}, trials: [], status: 'PENDING',
                task: taskBx._id, retry: 9, timeout: 0, delayRandomize: 0, retryDelayFactor: 1,
                priority: 0, dedupWithin: 0, dedupRecent: true,
            });
            expect(i1).toBeTruthy();
            expect(t1.getTime()).toBeGreaterThanOrEqual(now.getTime());
            expect(l1).toBeUndefined();

            jobAb = await operations.jobs.findOne({_id: i1});
            const {_id: i2, timeCreated: t2, local: {timeCreated: t3, ...l2}, ...j2} = jobAb;
            expect(j2).toStrictEqual(j1);
            expect(l2).toStrictEqual({
                domain: 'A', type :'b', params: {}, context: {}, task: taskBx._id, status: 'PENDING', trials: []
            });
            expect(i2).toEqual(i1);
            expect(t2.getTime()).toBe(t1.getTime());
            expect(t3.getTime()).toBe(t1.getTime());
        });
    });

    describe('update a job with non-null values', () => {
        test('', async () => {
            await operations.updateJobs({_id: jobBx._id}, {status: 'RUNNING', delay: 100});

            const job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i, timeCreated: t1, local: {timeCreated: t2, ...l}, ...j} = job;
            const {_id: i1, timeCreated: t11, local: {timeCreated: t12, ...l1}, ...j1} = jobBx;
            expect(j).toStrictEqual({...j1, delay: 100, status: 'RUNNING'});
            expect(l).toStrictEqual({...l1, delay: 100, status: 'RUNNING'});

            jobBx = job;
        });
    });

    describe('update a task with non-null values', () => {
        test('', async () => {
            await operations.updateTasks({_id: taskBx._id}, {delay: 21});
            let task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c1, mtime: m1, _id: i1, nextTime: n1, ...t1} = task;
            const {ctime: c8, mtime: m8, _id: i8, nextTime: n8, ...t8} = taskBx;
            expect(t1).toStrictEqual({...t8, delay: 21, local: {...t8.local, delay: 21}});
            expect(m1.getTime()).toBeGreaterThanOrEqual(m8.getTime());
            expect(c1.getTime()).toBe(c8.getTime());
            expect(i1).toEqual(i8);

            taskBx = task;

            await operations.updateTasks({_id: taskBx._id}, {
                delay: 15, subTasks: [{domain: 'A', type: 'b', dedupWithin: 10}]
            });
            task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c6, mtime: m6, _id: i6, nextTime: n6, ...t6} = task;
            const {ctime: c7, mtime: m7, _id: i7, nextTime: n7, ...t7} = taskBx;
            expect(t6).toStrictEqual({
                ...t7, delay: 15, subTasks: [{domain: 'A', type: 'b', delay: 30, dedupWithin: 10}],
                local: {
                    ...t7.local, delay: 15, subTasks: [{domain: 'A', type: 'b', dedupWithin: 10}],
                }
            });
            expect(m6.getTime()).toBeGreaterThanOrEqual(m7.getTime());
            expect(c6.getTime()).toBe(c7.getTime());
            expect(i6).toEqual(i7);

            taskBx = task;

            let job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual({...j3});
            expect(l2).toStrictEqual(l3);

            jobBx = job;

            job = await operations.jobs.findOne({_id: jobAb._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = jobAb;
            expect(j4).toStrictEqual({...j5, dedupWithin: 10, retry: 0});
            expect(l4).toStrictEqual(l5);

            jobAb = job;
        });
    });

    describe('update a task with null values', () => {
        test('', async () => {
            await operations.updateTasks({_id: taskBx._id}, {
                delay: null, subTasks: [{domain: 'A', type: 'b', dedupWithin: null}]
            });
            const task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c, mtime: m, _id: i, local: l, nextTime: n, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, local: l1, nextTime: n1, ...t1} = taskBx;
            expect(t).toStrictEqual({...t1, delay: 2, subTasks: [{domain: 'A', type: 'b', delay: 30}]});
            expect(l).toStrictEqual({...l1, delay: null, subTasks: null});
            expect(m.getTime()).toBeGreaterThanOrEqual(m1.getTime());
            expect(c.getTime()).toBe(c1.getTime());
            expect(i).toEqual(i1);
            taskBx = task;

            let job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual(j3);
            expect(l2).toStrictEqual(l3);

            jobBx = job;

            job = await operations.jobs.findOne({_id: jobAb._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = jobAb;
            expect(j4).toStrictEqual({...j5, dedupWithin: 0});
            expect(l4).toStrictEqual(l5);

            jobAb = job;
        });
    });

    describe('update a job with null values', () => {
        test('', async () => {
            await operations.updateJobs({_id: jobBx._id}, {delay: null});

            const job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i, timeCreated: t1, local: {timeCreated: t2, ...l}, ...j} = job;
            const {_id: i1, timeCreated: t11, local: {timeCreated: t12, delay: d1, ...l1}, ...j1} = jobBx;
            expect(j).toStrictEqual({...j1, delay: 2});
            expect(d1).toBe(100);
            expect(l).toStrictEqual({...l1, delay: null});

            jobBx = job;
        });
    });

    describe('update a task type with non-null values', () => {
        test('', async () => {
            await operations.updateTypes({domain: 'B', type: 'x'}, {delay: 29});

            const taskType = await operations.types.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskTypeBx;
            expect(t6).toStrictEqual({...t7, delay: 29});
            expect(l6).toStrictEqual({...l7, delay: 29});
            taskTypeBx = taskType;

            const task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = taskBx;
            expect(t).toStrictEqual({...t1, delay: 29});

            taskBx = task;

            const job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual({...j3, delay: 29});
            expect(l2).toStrictEqual(l3);

            jobBx = job;
        });
    });

    describe('update a task type with null values', () => {
        test('', async () => {
            await operations.updateTypes({domain: 'B', type: 'x'}, {delay: null});

            const taskType = await operations.types.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskTypeBx;
            expect(t6).toStrictEqual({...t7, delay: 2});
            expect(l6).toStrictEqual({...l7, delay: null});
            taskTypeBx = taskType;

            const task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = taskBx;
            expect(t).toStrictEqual({...t1, delay: 2});

            taskBx = task;

            const job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual({...j3, delay: 2});
            expect(l2).toStrictEqual(l3);

            jobBx = job;
        });
    });

    describe('update a domain with non-null values', () => {
        test('', async () => {
            await operations.updateDomains({domain: 'B'}, {delay: 31});

            let domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i4, ctime: c4, mtime: m4, local: l4, ...t4} = domain;
            const {_id: i5, ctime: c5, mtime: m5, local: l5, ...t5} = domainB;
            expect(t4).toStrictEqual({...t5, delay: 31});
            expect(l4).toStrictEqual({...l5, delay: 31});
            domainB = domain;

            let taskType = await operations.types.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskTypeBx;
            expect(t6).toStrictEqual({...t7, delay: 31});
            expect(l6).toStrictEqual(l7);
            taskTypeBx = taskType;

            let task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = taskBx;
            expect(t).toStrictEqual({...t1, delay: 31});

            taskBx = task;

            let job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual({...j3, delay: 31});
            expect(l2).toStrictEqual(l3);

            jobBx = job;


            await operations.updateDomains({domain: 'A'}, {delay: 77});

            domain = await operations.domains.findOne({domain: 'A'});
            const {_id: i8, ctime: c8, mtime: m8, local: l8, ...t8} = domain;
            const {_id: i9, ctime: c9, mtime: m9, local: l9, ...t9} = domainA;
            expect(t8).toStrictEqual({...t9, delay: 77});
            expect(l8).toStrictEqual({...l9, delay: 77});
            domainA = domain;

            taskType = await operations.types.findOne({domain: 'A', type: 'a'});
            const {_id: i10, ctime: c10, mtime: m10, local: l10, ...t10} = taskType;
            const {_id: i11, ctime: c11, mtime: m11, local: l11, ...t11} = taskTypeAa;
            expect(t10).toStrictEqual({...t11, delay: 77});
            expect(l10).toStrictEqual(l11);
            taskTypeAa = taskType;

            job = await operations.jobs.findOne({_id: jobAb._id});
            const {_id: i14, timeCreated: t141, local: {timeCreated: t142, ...l14}, ...j14} = job;
            const {_id: i15, timeCreated: t151, local: {timeCreated: t152, ...l15}, ...j15} = jobAb;
            expect(j14).toStrictEqual(j15);
            expect(l14).toStrictEqual(l15);

            jobAb = job;

            job = await operations.jobs.findOne({_id: jobAa._id});
            const {_id: i12, timeCreated: t121, local: {timeCreated: t122, ...l12}, ...j12} = job;
            const {_id: i13, timeCreated: t131, local: {timeCreated: t132, ...l13}, ...j13} = jobAa;
            expect(j12).toStrictEqual({...j13, delay: 77});
            expect(l12).toStrictEqual(l13);

            jobAa = job;
        });
    });

    describe('update a domain with null values', () => {
        test('', async () => {
            await operations.updateDomains({domain: 'B'}, {delay: null});

            let domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i4, ctime: c4, mtime: m4, local: l4, ...t4} = domain;
            const {_id: i5, ctime: c5, mtime: m5, local: l5, ...t5} = domainB;
            expect(t4).toStrictEqual({...t5, delay: 0});
            expect(l4).toStrictEqual({...l5, delay: null});
            domainB = domain;

            let taskType = await operations.types.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskTypeBx;
            expect(t6).toStrictEqual({...t7, delay: 0});
            expect(l6).toStrictEqual(l7);
            taskTypeBx = taskType;

            let task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = taskBx;
            expect(t).toStrictEqual({...t1, delay: 0});

            taskBx = task;

            let job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual({...j3, delay: 0});
            expect(l2).toStrictEqual(l3);

            jobBx = job;


            await operations.updateDomains({domain: 'A'}, {delay: null});

            domain = await operations.domains.findOne({domain: 'A'});
            const {_id: i8, ctime: c8, mtime: m8, local: l8, ...t8} = domain;
            const {_id: i9, ctime: c9, mtime: m9, local: l9, ...t9} = domainA;
            expect(t8).toStrictEqual({...t9, delay: 0});
            expect(l8).toStrictEqual({...l9, delay: null});
            domainA = domain;

            taskType = await operations.types.findOne({domain: 'A', type: 'a'});
            const {_id: i10, ctime: c10, mtime: m10, local: l10, ...t10} = taskType;
            const {_id: i11, ctime: c11, mtime: m11, local: l11, ...t11} = taskTypeAa;
            expect(t10).toStrictEqual({...t11, delay: 0});
            expect(l10).toStrictEqual(l11);
            taskTypeAa = taskType;

            job = await operations.jobs.findOne({_id: jobAb._id});
            const {_id: i14, timeCreated: t141, local: {timeCreated: t142, ...l14}, ...j14} = job;
            const {_id: i15, timeCreated: t151, local: {timeCreated: t152, ...l15}, ...j15} = jobAb;
            expect(j14).toStrictEqual(j15);
            expect(l14).toStrictEqual(l15);

            jobAb = job;

            job = await operations.jobs.findOne({_id: jobAa._id});
            const {_id: i12, timeCreated: t121, local: {timeCreated: t122, ...l12}, ...j12} = job;
            const {_id: i13, timeCreated: t131, local: {timeCreated: t132, ...l13}, ...j13} = jobAa;
            expect(j12).toStrictEqual({...j13, delay: 0});
            expect(l12).toStrictEqual(l13);

            jobAa = job;
        });
    });

    describe('disable a task', () => {
        test('', async () => {
            await operations.updateTasks({_id: taskBx._id}, {enabled: false});
            let task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task;
            const {ctime: c8, mtime: m8, _id: i8, ...t8} = taskBx;
            expect(t1).toStrictEqual({...t8, enabled: false, local: {...t8.local, enabled: false}});
            expect(m1.getTime()).toBeGreaterThanOrEqual(m8.getTime());
            expect(c1.getTime()).toBe(c8.getTime());
            expect(i1).toEqual(i8);

            taskBx = task;

            let job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual({...j3, status: 'CANCELED'});
            expect(l2).toStrictEqual({...l3, status: 'CANCELED'});

            jobBx = job;

            job = await operations.jobs.findOne({_id: jobAb._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = jobAb;
            expect(j4).toStrictEqual({...j5, status: 'CANCELED'});
            expect(l4).toStrictEqual({...l5, status: 'CANCELED'});

            jobAb = job;
        });
    });

    describe('re-enable a task', () => {
        test('', async () => {
            await operations.updateTasks({_id: taskBx._id}, {enabled: true});
            let task = await operations.tasks.findOne({_id: taskBx._id});
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task;
            const {ctime: c8, mtime: m8, _id: i8, ...t8} = taskBx;
            expect(t1).toStrictEqual({...t8, enabled: true, local: {...t8.local, enabled: true}});
            expect(m1.getTime()).toBeGreaterThanOrEqual(m8.getTime());
            expect(c1.getTime()).toBe(c8.getTime());
            expect(i1).toEqual(i8);

            taskBx = task;

            let job = await operations.jobs.findOne({_id: jobBx._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = jobBx;
            expect(j2).toStrictEqual({...j3});
            expect(l2).toStrictEqual(l3);

            jobBx = job;

            job = await operations.jobs.findOne({_id: jobAb._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = jobAb;
            expect(j4).toStrictEqual({...j5});
            expect(l4).toStrictEqual(l5);

            jobAb = job;
        });
    });

    test('insert an invalid task', async () => {
        await expect((async () => {
            await operations.insertTask({
                domain: 'B', type: 'x', subTasks: [{domain: 'A', type: 'b', retry: 9}],
                delay: 8, validBefore: new Date('2019-01-01'), mode: 'REPEATED'
            });
        })()).rejects.toThrow('field "interval" should be an Int >= 0 when "mode" is "REPEATED"')
    });

});