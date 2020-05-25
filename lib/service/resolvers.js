const debug = require('debug')('catalyst:resolvers');
const {isEmpty, isPlainObject} = require('lodash');
const {ObjectID} = require('mongodb');
const {GraphQLJSON, GraphQLJSONObject} = require('graphql-augment');
const {AugmentedArgResolver, GraphQLDateTime, UserInputError} = require('graphql-augment');

const {OperationError} = require('../error');


const OPERATOR_TO_MONGO_OPERATOR = {
    is: '$eq',
    not: '$ne',
    in: '$in',
    not_in: '$nin',
    gt: '$gt',
    gte: '$gte',
    lt: '$lt',
    lte: '$lte',
};


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


module.exports = function ({auth = () => {}} = {}) {

    async function authorize(ctx, payload, _, options) {
        return await auth(ctx, payload, options);
    }

    function makeQuery([query, ...queries]) {
        const query_ = {};
        const and = [];
        if (!isEmpty(query)) and.push(query);
        if (queries.length > 0) and.push({$or: queries});
        if (and.length > 0) query_.$and = and;
        return query_;
    }

    const queryResolver = new AugmentedArgResolver({
        auth: authorize,
        init: () => ({query: {}}),
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
        nested({query}, field, resolved) {
            if (Array.isArray(resolved)) {
                query[field] = {$in: resolved};
            } else if (typeof resolved === 'object' && resolved) {
                for (const [p, v] of Object.entries(resolved)) {
                    query[`${field}.${p}`] = v;
                }
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
        async return(ctxs, {sort, limit, offset}, {type, env: {context: {operations}}}) {
            const options = {};
            if (sort) {
                sort = sort.map(s => [s.by, s.desc ? -1 : 1]);
                options.sort = sort;
            }
            if (limit > 0) options.limit = limit;
            if (offset > 0) options.skip = offset;
            const query = makeQuery(ctxs.map(c => c.query));
            debug('%s.find(%j, %j)', type.name, query, options);
            const results = await operations.collections[type.name].find(query, options).toArray();
            return results.map(({_id, ...r}) => ({id: _id, ...r}));
        },
        async count(ctxs, _, {type, env: {context: {operations}}}) {
            const query = makeQuery(ctxs.map(c => c.query));
            debug('%s.countDocuments(%j)', type.name, query);
            return operations.collections[type.name].countDocuments(query);
        }
    });


    const updateFns = {
        Domain: async (operations, query, input) => {
            const {domain, ...update} = input;
            if (domain) query.domain = domain;
            if (!query.domain) throw new UserInputError('domain must be specified');
            await withOperationError(() => operations.updateDomains(query, update));
            return await operations.domains.findOne(query);
        },
        Type: async (operations, query, input) => {
            const {domain, type, ...update} = input;
            if (domain) query.domain = domain;
            if (type) query.type = type;
            if (!query.domain) throw new UserInputError('domain must be specified');
            if (!query.type) throw new UserInputError('type must be specified');
            await withOperationError(() => operations.updateTypes(query, update));
            return await operations.types.findOne(query);
        },
        Task: async (operations, query, input) => {
            let {id, ...update} = input;
            let task;
            if (id) {
                query._id = new ObjectID(id);
                await withOperationError(() => operations.updateTasks(query, update));
                task = await operations.tasks.findOne(query);
            } else {
                task = await withOperationError(() => operations.insertTask(update));
            }
            if (task) {
                const {_id, ...rest} = task;
                return {id: _id, ...rest};
            } else {
                return null;
            }
        },
        Job: async (operations, query, input, others) => {
            let {id, ...update} = input;
            const waitForStop = others.waitForStop;
            let job;
            if (id) {
                query._id = new ObjectID(id);
                await withOperationError(() => operations.updateJobs(query, update));
                job = await operations.jobs.findOne(query);
            } else {
                job = await withOperationError(() => operations.insertJob(update));
            }
            if (job) {
                if (job.duplicateOf && waitForStop.dedup) job = await operations.jobs.findOne({_id: job.duplicateOf});
                if (waitForStop && !['SUCCESS', 'FAILED', 'CANCELED'].includes(job.stats)) {
                    job = await watchJobForStop(operations, job._id, waitForStop);
                }
                let {_id, ...rest} = job;
                return {id: _id, ...rest}
            } else {
                return null;
            }
        }
    };

    async function  watchJobForStop(operations, jobId, options) {
        const watch = operations.jobs.watch(
            [{
                $match: {
                    'operationType': {$in: ['update', 'insert']},
                    'documentKey._id': jobId,
                    $or: [
                        {'updateDescription.updatedFields.status': {$in: ['SUCCESS', 'FAILED', 'CANCELED']}},
                        {'fullDocument.status': {$in: ['SUCCESS', 'FAILED', 'CANCELED']}},
                    ],
                }
            }],
            {fullDocument: 'updateLookup'}
        );
        let job;
        let error;
        try {
            job = await operations.jobs.findOne({_id: jobId});
        } catch (e) {
            error = e;
        }
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                    watch.close();
                    if (error) {
                        reject(error)
                    } else {
                        resolve(job);
                    }
                }, options.timeout * 1000
            );
            watch
                .on('change', (event) => {
                    watch.close();
                    resolve(event.fullDocument);
                })
                .on('error', (error) => {
                    debug('encounters an error watching job %s to stop: %s', jobId, error);
                    watch.close();
                    if (error.name === 'MongoError' && error.code === 40573) {
                        resolve(pollJobForStop(operations, jobId, options));
                    } else if (error.name === 'MongoNetworkError' && error.code === 11600) {
                        watchJobForStop(operations, jobId);
                    } else {
                        reject(error);
                    }
                });
        })
    }

    async function pollJobForStop(operations, jobId, options) {
        let job;
        const endTime = Date.now() + options.timeout * 1000;
        while (Date.now() - endTime > 0) {
            job = await operations.jobs.findOne({_id: jobId});
            if (['SUCCESS', 'FAILED', 'CANCELED'].includes(job.stats)) {
                break
            }
        }
        if (!['SUCCESS', 'FAILED', 'CANCELED'].includes(job.stats)) debug('watching job %s  timeout.', jobId);
        return job;
    }

    const mutationResolver = new AugmentedArgResolver({
        auth: authorize,
        init: () => ({query: {}, update: {}, others: {}}),
        input({update}, field, value) {
            update[field] = value;
        },
        nested({update}, field, resolved) {
            if (Array.isArray(resolved)) {
                update[field] = resolved.map(r => r.update);
            } else {
                update[field] = resolved.update;
            }
        },
        others({others}, arg, value) {
            others[arg] = value;
        },
        async resolve({query, update}, {type, env: {context: {operations}}}) {
            if (type.name === 'TaskSchedulingConfig') {
                return update;
            } else {
                const updated = await updateFns[type.name](query, update, operations);
                return updated._id;
            }
        },
        async return([{query, update, others}], _, {type, env: {context: {operations}}}) {
            return await updateFns[type.name](operations, query, update, others);
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

};
