const {MongoClient} = require('mongodb');

const {Logger} = require('../lib/logger');
const Operations = require('../lib/operations');


describe('Operations', () => {

    let connection;
    let db;
    let operations;

    let domain1, taskType1, taskType2, task1, job1, job2;

    beforeAll(async () => {

        const taskLoader = {
            async get() {
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
                retry: 0, retryDelayFactor: 2, priority: 0, dedupWithin: 0, dedupRecent: true,
            };

            for (const [domainName, typeName] of [
                ['A', 'a'],
                ['A', 'b'],
                ['B', 'x'],
            ]) {
                const {ctime: c1, mtime: m1, ...domain} = await operations.ensureTaskDomainConfig(domainName);
                expect(domain).toStrictEqual({domain: domainName, maxConcurrency: 1, ...config});
                const {ctime: c2, mtime: m2, ...type} = await operations.ensureTaskTypeConfig(domainName, typeName);
                expect(type).toStrictEqual({domain: domainName, type: typeName, ...config});
            }
            let taskType = await operations.taskTypes.findOne({domain: 'B', type: 'x'});
            const {_id: i1, ctime: c1, mtime: m1, local: l1, ...t1} = taskType;
            expect(t1).toStrictEqual({domain: 'B', type: 'x', ...config});
            taskType1 = taskType;

            taskType = await operations.taskTypes.findOne({domain: 'A', type: 'b'});
            const {_id: i2, ctime: c2, mtime: m2, local: l2, ...t2} = taskType;
            expect(t2).toStrictEqual({domain: 'A', type: 'b', ...config});
            taskType2 = taskType;

            let domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i3, ctime: c3, mtime: m3, local: l3, ...d3} = domain;
            expect(d3).toStrictEqual({domain: 'B', maxConcurrency: 1, ...config});
            domain1 = domain;
        });
    });

    describe('update task domains and types', () => {
        test('', async () => {
            let modified = await operations.updateTaskTypeConfigs(
                {domain: 'B', type: 'x'}, {subTasks: [{domain: 'A', type: 'b', delay: 30}]}
            );
            expect(modified).toBeGreaterThan(0);
            let taskType = await operations.taskTypes.findOne({domain: 'B', type: 'x'});
            const {_id: i, ctime: c, mtime: m, local: l, ...t} = taskType;
            const {_id: i1, ctime: c1, mtime: m1, local: l1, ...t1} = taskType1;
            expect(t).toStrictEqual({...t1, subTasks: [{domain: 'A', type: 'b', delay: 30}]});
            expect(l1).toBeUndefined();
            expect(l).toStrictEqual({subTasks: [{domain: 'A', type: 'b', delay: 30}]});
            taskType1 = taskType;

            modified = await operations.updateTaskTypeConfigs(
                {domain: 'A', type: 'b'}, {delay: 5}
            );
            expect(modified).toBeGreaterThan(0);
            taskType = await operations.taskTypes.findOne({domain: 'A', type: 'b'});
            const {_id: i2, ctime: c2, mtime: m2, local: l2, ...t2} = taskType;
            const {_id: i3, ctime: c3, mtime: m3, local: l3, ...t3} = taskType2;
            expect(t2).toStrictEqual({...t3, delay: 5});
            expect(l3).toBeUndefined();
            expect(l2).toStrictEqual({delay: 5});
            taskType2 = taskType;

            modified = await operations.updateTaskDomainConfigs(
                {domain: 'B'}, {delay: 2}
            );
            expect(modified).toBeGreaterThan(0);
            let domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i4, ctime: c4, mtime: m4, local: l4, ...t4} = domain;
            const {_id: i5, ctime: c5, mtime: m5, local: l5, ...t5} = domain1;
            expect(t4).toStrictEqual({...t5, delay: 2});
            expect(l5).toBeUndefined();
            expect(l4).toStrictEqual({delay: 2});
            domain1 = domain;

            taskType = await operations.taskTypes.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskType1;
            expect(t6).toStrictEqual({...t7, delay: 2});
            expect(l6).toStrictEqual(l7);
            taskType1 = taskType;
        });
    });

    describe('insert a valid task', () => {
        test('', async () => {
            const now = new Date();
            task1 = await operations.insertTask({
                domain: 'B', type: 'x', subTasks: [{domain: 'A', type: 'b', retry: 9}],
                retry: 8, validBefore: new Date('2019-01-01'), mode: 'ONCE',
                params: {p1: 123}
            });
            const {ctime: c1, mtime: m1, _id: i1, local: l1, nextTime: n1, ...t1} = task1;
            expect(t1).toStrictEqual({
                domain: 'B', type: 'x', subTasks: [{domain: 'A', type: 'b', delay: 30, retry: 9}],
                retry: 8, validBefore: new Date('2019-01-01'), mode: 'ONCE',
                enabled: true, params: {p1: 123}, context: {},
                timeout: 0, delay: 2, delayRandomize: 0, retryDelayFactor: 2,
                priority: 0, dedupWithin: 0, dedupRecent: true,
            });
            expect(i1).toBeTruthy();
            expect(l1).toBeUndefined();
            expect(n1.getTime()).toBeGreaterThanOrEqual(now.getTime());
            task1 = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c2, mtime: m2, _id: i2, nextTime: n2, ...t2} = task1;
            expect(t2).toStrictEqual({
                domain: 'B', type: 'x', subTasks: [{domain: 'A', type: 'b', delay: 30, retry: 9}],
                retry: 8, validBefore: new Date('2019-01-01'), mode: 'ONCE',
                enabled: true, params: {p1: 123}, context: {},
                timeout: 0, delay: 2, delayRandomize: 0, retryDelayFactor: 2,
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
            const job = await operations.insertJob({task: task1._id});
            const {_id: i1, timeCreated: t1, local: l1, ...j1} = job;
            expect(j1).toStrictEqual({
                domain: 'B', type: 'x', delay: 2, params: {p1: 123}, context: {}, trials: [], status: 'PENDING',
                task: task1._id, retry: 8, timeout: 0, delayRandomize: 0, retryDelayFactor: 2,
                priority: 0, dedupWithin: 0, dedupRecent: true,
            });
            expect(i1).toBeTruthy();
            expect(t1.getTime()).toBeGreaterThanOrEqual(now.getTime());
            expect(l1).toBeUndefined();

            job1 = await operations.jobs.findOne({_id: i1});
            const {_id: i2, timeCreated: t2, local: {timeCreated: t3, ...l2}, ...j2} = job1;
            expect(j2).toStrictEqual(j1);
            expect(l2).toStrictEqual({
                domain: 'B', type: 'x', context: {}, task: task1._id, status: 'PENDING', trials: []
            });
            expect(i2).toEqual(i1);
            expect(t2.getTime()).toBe(t1.getTime());
            expect(t3.getTime()).toBe(t1.getTime());
        });
    });

    describe('insert a valid job with subTask override', () => {
        test('', async () => {
            const now = new Date();

            const job = await operations.insertJob({task: task1._id, domain: 'A', type :'b', params: {}});
            const {_id: i1, timeCreated: t1, local: l1, ...j1} = job;
            expect(j1).toStrictEqual({
                domain: 'A', type :'b', delay: 30, params: {}, context: {}, trials: [], status: 'PENDING',
                task: task1._id, retry: 9, timeout: 0, delayRandomize: 0, retryDelayFactor: 2,
                priority: 0, dedupWithin: 0, dedupRecent: true,
            });
            expect(i1).toBeTruthy();
            expect(t1.getTime()).toBeGreaterThanOrEqual(now.getTime());
            expect(l1).toBeUndefined();

            job2 = await operations.jobs.findOne({_id: i1});
            const {_id: i2, timeCreated: t2, local: {timeCreated: t3, ...l2}, ...j2} = job2;
            expect(j2).toStrictEqual(j1);
            expect(l2).toStrictEqual({
                domain: 'A', type :'b', params: {}, context: {}, task: task1._id, status: 'PENDING', trials: []
            });
            expect(i2).toEqual(i1);
            expect(t2.getTime()).toBe(t1.getTime());
            expect(t3.getTime()).toBe(t1.getTime());
        });
    });

    describe('update a job with non-null values', () => {
        test('', async () => {
            let modified = await operations.updateJobs({_id: job1._id}, {status: 'RUNNING', delay: 100});
            expect(modified).toBeGreaterThan(0);

            const job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i, timeCreated: t1, local: {timeCreated: t2, ...l}, ...j} = job;
            const {_id: i1, timeCreated: t11, local: {timeCreated: t12, ...l1}, ...j1} = job1;
            expect(j).toStrictEqual({...j1, delay: 100, status: 'RUNNING'});
            expect(l).toStrictEqual({...l1, delay: 100, status: 'RUNNING'});

            job1 = job;
        });
    });

    describe('update a task with non-null values', () => {
        test('', async () => {
            let modified = await operations.updateTasks({_id: task1._id}, {delay: 21});
            expect(modified).toBeGreaterThan(0);
            let task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c1, mtime: m1, _id: i1, nextTime: n1, ...t1} = task;
            const {ctime: c8, mtime: m8, _id: i8, nextTime: n8, ...t8} = task1;
            expect(t1).toStrictEqual({...t8, delay: 21, local: {...t8.local, delay: 21}});
            expect(m1.getTime()).toBeGreaterThanOrEqual(m8.getTime());
            expect(c1.getTime()).toBe(c8.getTime());
            expect(i1).toEqual(i8);

            task1 = task;

            modified = await operations.updateTasks({_id: task1._id}, {
                delay: 15, subTasks: [{domain: 'A', type: 'b', dedupWithin: 10}]
            });
            expect(modified).toBeGreaterThan(0);
            task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c6, mtime: m6, _id: i6, nextTime: n6, ...t6} = task;
            const {ctime: c7, mtime: m7, _id: i7, nextTime: n7, ...t7} = task1;
            expect(t6).toStrictEqual({
                ...t7, delay: 15, subTasks: [{domain: 'A', type: 'b', delay: 30, dedupWithin: 10}],
                local: {
                    ...t7.local, delay: 15, subTasks: [{domain: 'A', type: 'b', dedupWithin: 10}],
                }
            });
            expect(m6.getTime()).toBeGreaterThanOrEqual(m7.getTime());
            expect(c6.getTime()).toBe(c7.getTime());
            expect(i6).toEqual(i7);

            task1 = task;

            let job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual({...j3});
            expect(l2).toStrictEqual(l3);

            job1 = job;

            job = await operations.jobs.findOne({_id: job2._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = job2;
            expect(j4).toStrictEqual({...j5, dedupWithin: 10, retry: 0});
            expect(l4).toStrictEqual(l5);

            job2 = job;
        });
    });

    describe('update a task with null values', () => {
        test('', async () => {
            let modified = await operations.updateTasks({_id: task1._id}, {
                delay: null, subTasks: [{domain: 'A', type: 'b', dedupWithin: null}]
            });
            expect(modified).toBeGreaterThan(0);
            const task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c, mtime: m, _id: i, local: l, nextTime: n, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, local: l1, nextTime: n1, ...t1} = task1;
            expect(t).toStrictEqual({...t1, delay: 2, subTasks: [{domain: 'A', type: 'b', delay: 30}]});
            expect(l).toStrictEqual({...l1, delay: null, subTasks: null});
            expect(m.getTime()).toBeGreaterThanOrEqual(m1.getTime());
            expect(c.getTime()).toBe(c1.getTime());
            expect(i).toEqual(i1);
            task1 = task;

            let job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual(j3);
            expect(l2).toStrictEqual(l3);

            job1 = job;

            job = await operations.jobs.findOne({_id: job2._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = job2;
            expect(j4).toStrictEqual({...j5, dedupWithin: 0});
            expect(l4).toStrictEqual(l5);

            job2 = job;
        });
    });

    describe('update a job with null values', () => {
        test('', async () => {
            let modified = await operations.updateJobs({_id: job1._id}, {delay: null});
            expect(modified).toBeGreaterThan(0);

            const job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i, timeCreated: t1, local: {timeCreated: t2, ...l}, ...j} = job;
            const {_id: i1, timeCreated: t11, local: {timeCreated: t12, delay: d1, ...l1}, ...j1} = job1;
            expect(j).toStrictEqual({...j1, delay: 2});
            expect(d1).toBe(100);
            expect(l).toStrictEqual({...l1, delay: null});

            job1 = job;
        });
    });

    describe('update a task type with non-null values', () => {
        test('', async () => {
            let modified = await operations.updateTaskTypeConfigs({domain: 'B', type: 'x'}, {delay: 29});
            expect(modified).toBeGreaterThan(0);

            const taskType = await operations.taskTypes.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskType1;
            expect(t6).toStrictEqual({...t7, delay: 29});
            expect(l6).toStrictEqual({...l7, delay: 29});
            taskType1 = taskType;

            const task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task1;
            expect(t).toStrictEqual({...t1, delay: 29});

            task1 = task;

            const job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual({...j3, delay: 29});
            expect(l2).toStrictEqual(l3);

            job1 = job;
        });
    });

    describe('update a task type with null values', () => {
        test('', async () => {
            let modified = await operations.updateTaskTypeConfigs({domain: 'B', type: 'x'}, {delay: null});
            expect(modified).toBeGreaterThan(0);

            const taskType = await operations.taskTypes.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskType1;
            expect(t6).toStrictEqual({...t7, delay: 2});
            expect(l6).toStrictEqual({...l7, delay: null});
            taskType1 = taskType;

            const task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task1;
            expect(t).toStrictEqual({...t1, delay: 2});

            task1 = task;

            const job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual({...j3, delay: 2});
            expect(l2).toStrictEqual(l3);

            job1 = job;
        });
    });

    describe('update a domain with non-null values', () => {
        test('', async () => {
            let modified = await operations.updateTaskDomainConfigs({domain: 'B'}, {delay: 31});
            expect(modified).toBeGreaterThan(0);

            const domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i4, ctime: c4, mtime: m4, local: l4, ...t4} = domain;
            const {_id: i5, ctime: c5, mtime: m5, local: l5, ...t5} = domain1;
            expect(t4).toStrictEqual({...t5, delay: 31});
            expect(l4).toStrictEqual({...l5, delay: 31});
            domain1 = domain;

            const taskType = await operations.taskTypes.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskType1;
            expect(t6).toStrictEqual({...t7, delay: 31});
            expect(l6).toStrictEqual(l7);
            taskType1 = taskType;

            const task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task1;
            expect(t).toStrictEqual({...t1, delay: 31});

            task1 = task;

            const job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual({...j3, delay: 31});
            expect(l2).toStrictEqual(l3);

            job1 = job;
        });
    });

    describe('update a domain with null values', () => {
        test('', async () => {
            let modified = await operations.updateTaskDomainConfigs({domain: 'B'}, {delay: null});
            expect(modified).toBeGreaterThan(0);

            const domain = await operations.domains.findOne({domain: 'B'});
            const {_id: i4, ctime: c4, mtime: m4, local: l4, ...t4} = domain;
            const {_id: i5, ctime: c5, mtime: m5, local: l5, ...t5} = domain1;
            expect(t4).toStrictEqual({...t5, delay: 0});
            expect(l4).toStrictEqual({...l5, delay: null});
            domain1 = domain;

            const taskType = await operations.taskTypes.findOne({domain: 'B', type: 'x'});
            const {_id: i6, ctime: c6, mtime: m6, local: l6, ...t6} = taskType;
            const {_id: i7, ctime: c7, mtime: m7, local: l7, ...t7} = taskType1;
            expect(t6).toStrictEqual({...t7, delay: 0});
            expect(l6).toStrictEqual(l7);
            taskType1 = taskType;

            const task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c, mtime: m, _id: i, ...t} = task;
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task1;
            expect(t).toStrictEqual({...t1, delay: 0});

            task1 = task;

            const job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual({...j3, delay: 0});
            expect(l2).toStrictEqual(l3);

            job1 = job;
        });
    });

    describe('disable a task', () => {
        test('', async () => {
            let modified = await operations.updateTasks({_id: task1._id}, {enabled: false});
            expect(modified).toBeGreaterThan(0);
            let task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task;
            const {ctime: c8, mtime: m8, _id: i8, ...t8} = task1;
            expect(t1).toStrictEqual({...t8, enabled: false, local: {...t8.local, enabled: false}});
            expect(m1.getTime()).toBeGreaterThanOrEqual(m8.getTime());
            expect(c1.getTime()).toBe(c8.getTime());
            expect(i1).toEqual(i8);

            task1 = task;

            let job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual({...j3, status: 'CANCELED'});
            expect(l2).toStrictEqual({...l3, status: 'CANCELED'});

            job1 = job;

            job = await operations.jobs.findOne({_id: job2._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = job2;
            expect(j4).toStrictEqual({...j5, status: 'CANCELED'});
            expect(l4).toStrictEqual({...l5, status: 'CANCELED'});

            job2 = job;
        });
    });

    describe('re-enable a task', () => {
        test('', async () => {
            let modified = await operations.updateTasks({_id: task1._id}, {enabled: true});
            expect(modified).toBeGreaterThan(0);
            let task = await operations.tasks.findOne({_id: task1._id});
            const {ctime: c1, mtime: m1, _id: i1, ...t1} = task;
            const {ctime: c8, mtime: m8, _id: i8, ...t8} = task1;
            expect(t1).toStrictEqual({...t8, enabled: true, local: {...t8.local, enabled: true}});
            expect(m1.getTime()).toBeGreaterThanOrEqual(m8.getTime());
            expect(c1.getTime()).toBe(c8.getTime());
            expect(i1).toEqual(i8);

            task1 = task;

            let job = await operations.jobs.findOne({_id: job1._id});
            const {_id: i2, timeCreated: t21, local: {timeCreated: t22, ...l2}, ...j2} = job;
            const {_id: i3, timeCreated: t31, local: {timeCreated: t32, ...l3}, ...j3} = job1;
            expect(j2).toStrictEqual({...j3});
            expect(l2).toStrictEqual(l3);

            job1 = job;

            job = await operations.jobs.findOne({_id: job2._id});
            const {_id: i4, timeCreated: t41, local: {timeCreated: t42, ...l4}, ...j4} = job;
            const {_id: i5, timeCreated: t51, local: {timeCreated: t52, ...l5}, ...j5} = job2;
            expect(j4).toStrictEqual({...j5});
            expect(l4).toStrictEqual(l5);

            job2 = job;
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