"use strict";

const debug = require('debug')('catalyst:resolvers');
const {isEmpty, isPlainObject} = require('lodash');
const {ObjectID} = require('mongodb');
const {UserInputError} = require('apollo-server-koa');
const {AugmentedArgResolver, GraphQLDateTime, GraphQLJSON, GraphQLJSONObject} = require('graphql-augment');
const {shrink} = require('@raychee/utils');

const {OperationError} = require('../error');
const {OPERATOR_TO_MONGO_OPERATOR} = require('../config');


async function withOperationError(fn) {
    try {
        return await fn();
    } catch (e) {
        if (e instanceof OperationError) {
            throw new UserInputError(e.message);
        } else {
            throw e;
        }
    }
}

function validateJobWaitOptions(options) {
    if (!(options.until && options.until.length > 0)) {
        throw new UserInputError(`wait.until must be specified with at least one job's status`);
    }
    if (!(options.timeout >= 0 && options.timeout <= 3600)) {
        throw new UserInputError(`wait.timeout must be >= 0 and <= 3600`);
    }
    if (options.timeout > 0 && !(options.pollInterval > 0 && options.pollInterval <= options.timeout)) {
        throw new UserInputError(`wait.pollInterval must be > 0 and <= wait.timeout (which is ${options.timeout})`);
    }
}

async function waitForJob(context, job, waitOptions) {
    if (waitOptions) {
        if (waitOptions.timeout > 0) {
            const jobWatcher = context.jobWatcherManager.getWatcher(waitOptions.until);
            let jobId = job._id;
            while (true) {
                if (!waitOptions.until.includes(job.status)) {
                    job = await jobWatcher.watch(jobId, {timeout: waitOptions.timeout});
                }
                if (job.duplicateOf && waitOptions.followDuplicate) {
                    jobId = job.duplicateOf;
                    job = {};
                } else {
                    break;
                }
            }
        } else if (waitOptions.followDuplicate) {
            while (job.duplicateOf) {
                job = await context.operations.jobs.findOne({_id: job.duplicateOf});
            }
        }
    }
    return job;
}


module.exports = {

    makeResolvers({auth = () => {}} = {}) {
        
        function makeQuery([query, ...queries]) {
            const query_ = {};
            const and = [];
            if (!isEmpty(query)) and.push(query);
            if (queries.length > 0) and.push({$or: queries});
            if (and.length > 0) query_.$and = and;
            return query_;
        }

        const queryResolver = new AugmentedArgResolver({
            auth,
            init: () => ({query: {}, others: {}}),
            filter({query}, field, op, value) {
                if (isPlainObject(value)) {
                    for (const [p, v] of Object.entries(value)) {
                        query[`${field}.${p}`] = v;
                    }
                } else {
                    if (field === 'id') {
                        field = '_id';
                        if (typeof value === 'string') {
                            value = new ObjectID(value);
                        } else if (Array.isArray(value)) {
                            value = value.map(v => ObjectID(v));
                        }
                    }
                    const operator = OPERATOR_TO_MONGO_OPERATOR[op];
                    let condition;
                    if (operator) {
                        condition = {[operator]: value};
                    } else if (op === 'regex') {
                        condition = new RegExp(value);
                    } else if (op === 'not_regex') {
                        condition = {$not: new RegExp(value)};
                    }
                    if (condition) {
                        query[field] = condition;
                    } else {
                        debug('warn: no condition found for %s %s %s', field, op, value);
                    }
                }
            },
            others({others}, arg, value) {
                others[arg] = value;
            },
            paginate(ctx, {page, limit}) {
                ctx.skip = (page - 1) * limit;
                ctx.limit = limit;
            },
            sort(ctx, sort) {
                ctx.sort = sort.map(s => [s.by, s.desc ? -1 : 1]);
            },
            nested({query}, field, resolved) {
                if (Array.isArray(resolved)) {
                    query[field] = {$in: resolved};
                } else if (typeof resolved === 'object' && resolved) {
                    for (const [p, v] of Object.entries(resolved)) {
                        query[`${field}.${p}`] = v;
                    }
                } else {
                    query[field] = resolved;
                }
            },
            async resolve({query}, {type, env: {context: {operations}}}) {
                if (type.name === 'TaskSchedulingConfig') {
                    return query;
                } else {
                    debug('%s.find(%j)', type.name, query);
                    const results = await operations.collections[type.name].find(query).toArray();
                    return results.map(r => r._id);
                }
            },
            async results(ctxs, {type, env: {context}}) {
                const waitOptions = ctxs[0].others.wait;
                if (waitOptions) {
                    validateJobWaitOptions(waitOptions);
                }
                const query = makeQuery(ctxs.map(c => c.query));
                const [{sort, limit, skip}] = ctxs;
                const options = shrink({sort, limit, skip});
                debug('%s.find(%j, %j)', type.name, query, options);
                let results = await context.operations.collections[type.name].find(query, options).toArray();
                if (waitOptions) {
                    results = await Promise.all(results.map(job => waitForJob(context, job, waitOptions)));
                }
                return results.map(({_id, ...r}) => ({id: _id, ...r}));
            },
            async count(ctxs, {type, env: {context: {operations}}}) {
                const query = makeQuery(ctxs.map(c => c.query));
                debug('%s.countDocuments(%j)', type.name, query);
                return operations.collections[type.name].countDocuments(query);
            }
        });
        
        const updateFns = {
            Domain: async (context, query, input) => {
                const {domain, ...update} = input;
                if (domain) query.domain = domain;
                if (!query.domain) throw new UserInputError('domain must be specified');
                await withOperationError(() => context.operations.updateDomains(query, update));
                return await context.operations.domains.findOne(query);
            },
            Type: async (context, query, input) => {
                const {domain, type, ...update} = input;
                if (domain) query.domain = domain;
                if (type) query.type = type;
                if (!query.domain) throw new UserInputError('domain must be specified');
                if (!query.type) throw new UserInputError('type must be specified');
                await withOperationError(() => context.operations.updateTypes(query, update));
                return await context.operations.types.findOne(query);
            },
            Task: async (context, query, input) => {
                let {id, ...update} = input;
                let task;
                if (id) {
                    query._id = new ObjectID(id);
                    await withOperationError(() => context.operations.updateTasks(query, update));
                    task = await context.operations.tasks.findOne(query);
                } else {
                    task = await withOperationError(() => context.operations.insertTask(update));
                }
                if (task) {
                    const {_id, ...rest} = task;
                    return {id: _id, ...rest};
                } else {
                    return null;
                }
            },
            Job: async (context, query, input, others) => {
                const waitOptions = others.wait;
                if (waitOptions) {
                    validateJobWaitOptions(waitOptions);
                }
                let {id, ...update} = input;
                let job;
                query._id = new ObjectID(id);
                await withOperationError(() => context.operations.updateJobs(query, update));
                job = await context.operations.jobs.findOne(query);
                if (job) {
                    job = await waitForJob(context, job, waitOptions);
                    let {_id, ...rest} = job;
                    return {id: _id, ...rest}
                } else {
                    return null;
                }
            }
        };
        
        

        const mutationResolver = new AugmentedArgResolver({
            auth,
            init: () => ({query: {}, update: {}, others: {}}),
            input({update}, field, value) {
                update[field] = value;
            },
            nested({update}, field, resolved) {
                if (Array.isArray(resolved)) {
                    update[field] = resolved.map(r => r.update || r.id);
                } else {
                    update[field] = resolved.update || resolved.id;
                }
            },
            others({others}, arg, value) {
                others[arg] = value;
            },
            async resolve({query, update}, {type, env: {context}}) {
                if (type.name === 'TaskSchedulingConfig') {
                    return {update};
                } else {
                    const updated = await updateFns[type.name](context, query, update);
                    return {id: updated._id};
                }
            },
            async results([{query, update, others}], {type, env: {context}}) {
                return await updateFns[type.name](context, query, update, others);
            }
        });


        return {

            JSON: GraphQLJSON,
            JSONObject: GraphQLJSONObject,
            DateTime: GraphQLDateTime,

            Query: {
                async Domain(parent, args, context, info) {
                    return await queryResolver.resolve(parent, args, context, info);
                },

                async Type(parent, args, context, info) {
                    return await queryResolver.resolve(parent, args, context, info);
                },

                async Task(parent, args, context, info) {
                    return await queryResolver.resolve(parent, args, context, info);
                },

                async Job(parent, args, context, info) {
                    return await queryResolver.resolve(parent, args, context, info);
                },
            },

            Job: {
                async task(parent, args, context, info) {
                    if (!parent[info.fieldName]) return null;
                    const {_id, ...task} = await context.operations.tasks.findOne({_id: parent[info.fieldName]}) || {};
                    return {id: _id, ...task};
                },
                async createdBy(parent, args, context, info) {
                    if (!parent[info.fieldName]) return null;
                    const {_id, ...job} = await context.operations.jobs.findOne({_id: parent[info.fieldName]}) || {};
                    return {id: _id, ...job};
                },
                async createdFrom(parent, args, context, info) {
                    if (!parent[info.fieldName]) return null;
                    const {_id, ...job} = await context.operations.jobs.findOne({_id: parent[info.fieldName]}) || {};
                    return {id: _id, ...job};
                },
                async duplicateOf(parent, args, context, info) {
                    if (!parent[info.fieldName]) return null;
                    const {_id, ...job} = await context.operations.jobs.findOne({_id: parent[info.fieldName]}) || {};
                    return {id: _id, ...job};
                },
            },

            Mutation: {
                async UpdateDomain(parent, args, context, info) {
                    return await mutationResolver.resolve(parent, args, context, info);
                },

                async UpdateType(parent, args, context, info) {
                    return await mutationResolver.resolve(parent, args, context, info);
                },

                async UpdateTask(parent, args, context, info) {
                    return await mutationResolver.resolve(parent, args, context, info);
                },

                async UpdateJob(parent, args, context, info) {
                    return await mutationResolver.resolve(parent, args, context, info);
                }

            }
        };

    }
};
