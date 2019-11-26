const {GraphQLJSON, GraphQLJSONObject} = require('graphql-type-json');
const {AugmentedArgResolver, GraphQLDateTime, UserInputError} = require('graphql-augment');

const {TYPES} = require('../config');


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


function authorize(payload, auth, type, field, mode, args) {
    const {id, scopes = []} = payload || {};
    if (!Array.isArray(auth)) {
        auth = [auth];
    }
    let message;
    if (!auth.some(a => scopes.indexOf(a) >= 0)) {
        message = `user "${id}" does not have the required scope "${auth.join('", "')}"`
    }

    return message;
}

function queryArgResolverGenerator(context) {

    function getPrefix(type) {
        return type.name === 'Job' ? '' : '_full.';
    }

    return {
        auth: authorize,
        init: () => ({}),
        filter(query, field, op, value, type) {
            if (typeof value === 'object' && !Array.isArray(value)) {
                for (const [p, v] of Object.entries(value)) {
                    query[`${getPrefix(type)}${field}.${p}`] = v;
                }
            } else {
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
                    query[`${getPrefix(type)}${field}`] = condition;
                }
            }
        },
        nested(query, field, resolved, type) {
            if (Array.isArray(resolved)) {
                query[`${getPrefix(type)}${field}`] = {$in: resolved};
            } else if (typeof resolved === 'object' && resolved) {
                for (const [p, v] of Object.entries(resolved)) {
                    query[`${getPrefix(type)}${field}.${p}`] = v;
                }
            }
        },
        async resolve(query, type) {
            if (type.name === 'TaskSchedulingConfig') {
                return query;
            } else {
                const key = TYPES[type.name].key;
                if (key.length === 1) {
                    const results = await context.operations.query(type.name, query, undefined, context.dataloaders);
                    return results.map(r => r[key[0]]);
                }
            }
        },
        async return(queries, options, type) {
            let results;
            if (queries.length === 0) {
                results = [];
            } else if (queries.length === 1) {
                results = await context.operations.query(type.name, queries[0], options, context.dataloaders);
            } else {
                results = await context.operations.query(type.name, {$or: queries}, options, context.dataloaders);
            }
            return results.map(r => r._full || r);
        },
        async count(queries, options, type) {
            if (queries.length === 0) {
                return 0;
            } else if (queries.length === 1) {
                return await context.operations.query(type.name, queries[0], options, context.dataloaders, true);
            } else {
                return await context.operations.query(type.name, {$or: queries}, options, context.dataloaders, true);
            }
        }
    };
}


function mutationArgResolverGenerator(context) {

    const resolveFns = {
        TaskDomainConfig: async (updates) =>
            await context.operations.updateTaskDomainConfig(undefined, updates, context.dataloaders),
        TaskTypeConfig: async (updates) =>
            await context.operations.updateTaskTypeConfig(undefined, undefined, updates, context.dataloaders),
        Task: async (updates) =>
            await context.operations.scheduleTask(updates, context.dataloaders),
        Job: async (updates) => {
            if (!updates.id) throw new UserInputError('Job id must be specified');
            return await context.operations.upsert('Job', updates, true, context.dataloaders);
        }
    };

    return {
        auth: authorize,
        init: () => ({}),
        input(updates, field, value) {
            updates[field] = value;
        },
        nested(updates, field, resolved, type) {
            const nestedTypeName = getNamedType(type.getFields()[field].type).name;
            if (nestedTypeName === 'TaskSchedulingConfig') {
                updates[field] = resolved;
            } else if (resolved) {
                updates[field] = resolved;
            }
        },
        async resolve([updates], type) {
            if (type.name === 'TaskSchedulingConfig') {
                return updates;
            } else {
                const key = TYPES[type.name].key;
                const resolve = resolveFns[type.name];
                if (key.length === 1) {
                    return (await resolve(updates))[key[0]];
                }
            }
        },
        async return([updates], _, type) {
            const resolve = resolveFns[type.name];
            return await resolve(updates);
        }
    };
}


const resolvers = {

    JSON: GraphQLJSON,
    JSONObject: GraphQLJSONObject,
    DateTime: GraphQLDateTime,

    Query: {
        async TaskDomainConfig(parent, args, context, info) {
            return await (new AugmentedArgResolver(queryArgResolverGenerator(context))).resolve(args, info, context);
        },

        async TaskTypeConfig(parent, args, context, info) {
            return await (new AugmentedArgResolver(queryArgResolverGenerator(context))).resolve(args, info, context);
        },

        async Task(parent, args, context, info) {
            return await (new AugmentedArgResolver(queryArgResolverGenerator(context))).resolve(args, info, context);
        },

        async Job(parent, args, context, info) {
            return await (new AugmentedArgResolver(queryArgResolverGenerator(context))).resolve(args, info, context);
        },
    },

    Task: {
        async nextRun(parent, args, context, info) {
            if (parent.id) {
                const [agendaJob] = await context.operations.taskLoader.taskAgenda._collection.find({'data.taskId': parent.id}).toArray();
                return agendaJob && agendaJob.nextRunAt;
            } else {
                return undefined;
            }
        },
        async lastRun(parent, args, context, info) {
            if (parent.id) {
                const [agendaJob] = await context.operations.taskLoader.taskAgenda._collection.find({'data.taskId': parent.id}).toArray();
                return agendaJob && agendaJob.lastRunAt;
            } else {
                return undefined;
            }
        }
    },

    Job: {
        async task(parent, args, context, info) {
            return await context.operations.query('Task', [parent[info.fieldName]], undefined, context.dataloaders);
        },
        async createdBy(parent, args, context, info) {
            return await context.operations.query('Job', [parent[info.fieldName]], undefined, context.dataloaders);
        },
        async createdFrom(parent, args, context, info) {
            return await context.operations.query('Job', [parent[info.fieldName]], undefined, context.dataloaders);
        },
    },

    Mutation: {
        async UpdateTaskDomainConfig(parent, args, context, info) {
            return await (new AugmentedArgResolver(mutationArgResolverGenerator(context))).resolve(args, info, context);
        },

        async UpdateTaskTypeConfig(parent, args, context, info) {
            return await (new AugmentedArgResolver(mutationArgResolverGenerator(context))).resolve(args, info, context);
        },

        async UpdateTask(parent, args, context, info) {
            return await (new AugmentedArgResolver(mutationArgResolverGenerator(context))).resolve(args, info, context);
        },

        async UpdateJob(parent, args, context, info) {
            return await (new AugmentedArgResolver(mutationArgResolverGenerator(context))).resolve(args, info, context);
        }

    }
};


module.exports = resolvers;