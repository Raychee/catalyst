const {ObjectID} = require('mongodb');
const {GraphQLJSON, GraphQLJSONObject} = require('graphql-type-json');
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

    const queryResolver = new AugmentedArgResolver({
        auth,
        init: () => ({}),
        filter(query, field, op, value) {
            if (typeof value === 'object' && !Array.isArray(value)) {
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
                }
            }
        },
        nested(query, field, resolved) {
            if (Array.isArray(resolved)) {
                query[field] = {$in: resolved};
            } else if (typeof resolved === 'object' && resolved) {
                for (const [p, v] of Object.entries(resolved)) {
                    query[`${field}.${p}`] = v;
                }
            }
        },
        async resolve(query, {type, env: {context: {operations}}}) {
            if (type.name === 'TaskSchedulingConfig') {
                return query;
            } else {
                const results = await operations.collections[type.name].find(query).toArray();
                return results.map(r => r._id);
            }
        },
        async return(queries, {sort, limit, offset}, {type, env: {context: {operations}}}) {
            const options = {};
            if (sort) {
                sort = sort.map(s => [s.by, s.desc ? -1 : 1]);
                options.sort = sort;
            }
            if (limit > 0) options.limit = limit;
            if (offset > 0) options.skip = offset;
            let results;
            if (queries.length === 0) {
                results = [];
            } else {
                results = await operations.collections[type.name].find({$or: queries}, options).toArray();
            }
            return results.map(({_id, ...r}) => ({id: _id, ...r}));
        },
        async count(queries, {limit}, {type, env: {context: {operations}}}) {
            const options = {};
            if (limit > 0) options.limit = limit;
            if (queries.length === 0) {
                return 0;
            } else {
                return operations.collections[type.name].countDocuments({$or: queries}, options);
            }
        }
    });


    const updateFns = {
        Domain: async (input, operations) => {
            const {domain, ...update} = input;
            if (!domain) throw new UserInputError('domain must be specified');
            await withOperationError(() => operations.updateDomains({domain}, update));
            return await operations.domains.findOne({domain});
        },
        Type: async (input, operations) => {
            const {domain, type, ...update} = input;
            if (!domain) throw new UserInputError('domain must be specified');
            if (!type) throw new UserInputError('type must be specified');
            await withOperationError(() => operations.updateTypes({domain, type}, update));
            return await operations.types.findOne({domain, type});
        },
        Task: async (input, operations) => {
            let {id, ...update} = input;
            let task;
            if (id) {
                id = new ObjectID(id);
                await withOperationError(() => operations.updateTasks({_id: id}, update));
                task = await operations.tasks.findOne({_id: id});
            } else {
                task = await withOperationError(() => operations.insertTask(update));
            }
            const {_id, ...rest} = task;
            return {id: _id, ...rest};
        },
        Job: async (input, operations) => {
            let {id, ...update} = input;
            if (!id) throw new UserInputError('Job id must be specified');
            id = new ObjectID(id);
            await withOperationError(() => operations.updateJobs({_id: id}, update));
            const {_id, ...rest} = await operations.jobs.findOne({_id: id});
            return {id: _id, ...rest};
        }
    };

    const mutationResolver = new AugmentedArgResolver({
        auth,
        init: () => ({}),
        input(update, field, value) {
            update[field] = value;
        },
        nested(update, field, resolved) {
            update[field] = resolved;
        },
        async resolve(update, {type, env: {context: {operations}}}) {
            if (type.name === 'TaskSchedulingConfig') {
                return update;
            } else {
                const updated = await updateFns[type.name](update, operations);
                return updated._id;
            }
        },
        async return([update], _, {type, env: {context: {operations}}}) {
            return await updateFns[type.name](update, operations);
        }
    });


    return {

        JSON: GraphQLJSON,
        JSONObject: GraphQLJSONObject,
        DateTime: GraphQLDateTime,

        Query: {
            async Domain(parent, args, context, info) {
                return await queryResolver.resolve(parent, args, info, context);
            },

            async Type(parent, args, context, info) {
                return await queryResolver.resolve(parent, args, info, context);
            },

            async Task(parent, args, context, info) {
                return await queryResolver.resolve(parent, args, info, context);
            },

            async Job(parent, args, context, info) {
                return await queryResolver.resolve(parent, args, info, context);
            },
        },

        Job: {
            async task(parent, args, context, info) {
                const {_id, ...task} = await context.operations.tasks.findOne({_id: parent[info.fieldName]}) || {};
                return {id: _id, ...task};
            },
            async createdBy(parent, args, context, info) {
                const {_id, ...job} = await context.operations.jobs.findOne({_id: parent[info.fieldName]}) || {};
                return {id: _id, ...job};
            },
            async createdFrom(parent, args, context, info) {
                const {_id, ...job} = await context.operations.jobs.findOne({_id: parent[info.fieldName]}) || {};
                return {id: _id, ...job};
            },
        },

        Mutation: {
            async UpdateDomain(parent, args, context, info) {
                return await mutationResolver.resolve(parent, args, info, context);
            },

            async UpdateType(parent, args, context, info) {
                return await mutationResolver.resolve(parent, args, info, context);
            },

            async UpdateTask(parent, args, context, info) {
                return await mutationResolver.resolve(parent, args, info, context);
            },

            async UpdateJob(parent, args, context, info) {
                return await mutationResolver.resolve(parent, args, info, context);
            }

        }
    };

};