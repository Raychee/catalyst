const {GraphQLJSON, GraphQLJSONObject} = require('graphql-type-json');
const {AugmentedArgResolver, GraphQLDateTime, UserInputError, getNamedType} = require('graphql-augment');
const {BatchLoader, shrink, sleep} = require('@raychee/utils');

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


function isQueryBy(query) {
    if (Array.isArray(query)) {
        if (query.length <= 0 || Array.isArray(query[0])) {
            return 'keys';
        } else {
            return 'key';
        }
    } else if (typeof query === 'object') {
        return 'query';
    }
}

function getKeyValues(typeName, doc) {
    if (!doc) doc = {};
    return TYPES[typeName].key.map(k => doc[k]);
}

function setKeyValues(typeName, doc, values) {
    let i = 0;
    for (const k of TYPES[typeName].key) {
        doc[k] = values[i++];
    }
}

function makeQuery(typeName, values) {
    const query = {};
    let i = 0;
    for (const k of TYPES[typeName].key) {
        query[k] = values[i++];
    }
    return query;
}


// for those whose key is undefined (e.g. Config which is globally unique),
// just assign "q" with a random scalar value, like 1. and you will get the one object.
async function query(mongodb, typeName, q, options, dataloaders, count) {
    let dataloader = get(dataloaders, typeName);
    if (!dataloader) {
        dataloader = _newDataLoader(typeName);
        set(dataloaders, typeName, dataloader);
    }
    const key = TYPES[typeName].key;
    if (isQueryBy(q) === 'query') {
        if (key && key.length === 1 && Object.keys(q).length === 1) {
            const [subQ] = getKeyValues(typeName, q);
            if (typeof subQ === 'object') {
                const {$eq} = subQ;
                if ($eq) {
                    q = [[$eq]];
                } else {
                    const {$in} = subQ;
                    if (Array.isArray($in)) {
                        q = [$in.map(k => [k])];
                    }
                }
            } else if (Array.isArray(subQ)) {
                q = [subQ.map(k => [k])];
            } else if (subQ) {
                q = [[subQ]];
            }
        }
    }
    switch (isQueryBy(q)) {
        case 'key':
            return await dataloader.load(q);
        case 'keys':
            return await dataloader.loadMany(q);
        case 'query':
            let cursor = mongodb.collection(typeName).find(q);
            const {sort, limit, offset} = options || {};
            if (sort) {
                const sortArg = {};
                for (const s of sort) {
                    sortArg[s.by] = s.desc ? -1 : 1;
                }
                cursor = cursor.sort(sortArg);
            }
            if (limit > 0) cursor.limit(limit);
            if (offset > 0) cursor.skip(offset);
            if (count) {
                return await cursor.count();
            } else {
                const results = await cursor.toArray();
                for (const result of results) {
                    if (key) {
                        dataloader.prime(getKeyValues(typeName, result), result);
                    }
                    shrink(result);
                }
                return results;
            }
    }

    function _newDataLoader(typeName) {
        const key = TYPES[typeName].key;
        if (key) {
            return new BatchLoader(async ids => {
                const results = await mongodb.collection(typeName).find({
                    $or: ids.map(i => makeQuery(typeName, i))
                }).toArray();
                const resultObject = {};
                for (const result of results) {
                    shrink(result);
                    resultObject[getKeyValues(typeName, result).join('.')] = result;
                }
                return ids.map(i => resultObject[i.join('.')]);
            });
        } else {
            return new BatchLoader(async ids => {
                const result = await mongodb.collection(typeName).findOne();
                shrink(result);
                return ids.map(() => result);
            });
        }
    }
}

// doc must be a valid object subject to the typeName.
// the primary key of the object is optional;
// if missing, this function will make an insert; otherwise it will try upsert.
// returns the upserted doc.
async function upsert(mongodb, typeName, doc, rawChange, dataloaders, filter, waitForFilter) {
    const {key} = TYPES[typeName];
    if (key) {
        const keyValues = getKeyValues(typeName, doc);
        if (keyValues.every(v => v)) {
            const upsert = !filter;
            const query = {...filter, ...makeQuery(typeName, keyValues)};
            doc = await _update(typeName, doc, query, rawChange, upsert, waitForFilter);
        } else {
            doc = await _insert(typeName, doc, rawChange);
        }
    } else {
        doc = await _update(typeName, doc, filter || {}, rawChange, !filter, waitForFilter);
    }
    let dataloader = get(dataloaders, typeName);
    if (dataloader) {
        const k = key ? getKeyValues(typeName, doc) : 1;
        dataloader.prime(k, doc);
    }
    shrink(doc);
    return doc;

    async function _update(typeName, doc, query, rawChange, upsert, waitForResult) {
        const update = {$set: doc};
        if (!rawChange) {
            const {mtime, ctime} = TYPES[typeName];
            const now = new Date();
            if (mtime) {
                doc[mtime] = now;
            }
            if (ctime) {
                update.$setOnInsert = {[ctime]: now};
            }
        }
        while (true) {
            const result = await mongodb.collection(typeName).findOneAndUpdate(
                query, update, {upsert, returnOriginal: false}
            );
            if (!upsert && waitForResult && !result.value) {
                await sleep(100);
            } else {
                return result.value;
            }
        }
    }

    async function _insert(typeName, doc, rawChange) {
        const {key, mtime, ctime} = TYPES[typeName];
        if (key && key.length > 1) {
            throw new Error('internal error: insert is not allowed for type that has keys.length > 1');
        }
        if (!rawChange) {
            const now = new Date();
            if (mtime) {
                doc[mtime] = now;
            }
            if (ctime) {
                doc[ctime] = now;
            }
        }
        const result = await mongodb.collection(typeName).insertOne(doc);
        if (key) {
            const set = {};
            setKeyValues(typeName, set, [result.insertedId.toHexString()]);
            await mongodb.collection(typeName).updateOne({_id: result.insertedId}, {$set: set});
            Object.assign(doc, set);
        }
        return doc;
    }
}



/**
 * @param context {{mongodb: Db, operations: Operations}}
 */
function queryArgResolverGenerator(context) {

    function getPrefix(type) {
        return type.name === 'Job' ? '' : 'full.';
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
                    const results = await query(context.mongodb, type.name, query, undefined, context.dataloaders);
                    return results.map(r => r[key[0]]);
                }
            }
        },
        async return(queries, options, type) {
            let results;
            if (queries.length === 0) {
                results = [];
            } else if (queries.length === 1) {
                results = await query(context.mongodb, type.name, queries[0], options, context.dataloaders);
            } else {
                results = await query(context.mongodb, type.name, {$or: queries}, options, context.dataloaders);
            }
            return results.map(r => r.full || r);
        },
        async count(queries, options, type) {
            if (queries.length === 0) {
                return 0;
            } else if (queries.length === 1) {
                return await query(context.mongodb, type.name, queries[0], options, context.dataloaders, true);
            } else {
                return await query(context.mongodb, type.name, {$or: queries}, options, context.dataloaders, true);
            }
        }
    };
}



/**
 * @param context {{mongodb: Db, operations: Operations, scheduler: Scheduler}}
 */
function mutationArgResolverGenerator(context) {

    // TODO: use schedulers
    const resolveFns = {
        TaskDomainConfig: async (updates) =>
            await context.operations.updateTaskDomainConfig(undefined, updates, context.dataloaders),
        TaskTypeConfig: async (updates) =>
            await context.operations.updateTaskTypeConfig(undefined, undefined, updates, context.dataloaders),
        Task: async (updates) => {
            const task = await context.operations.updateTask(updates);
            if (task.ctime.getTime() === task.mtime.getTime()) {
                await context.scheduler._scheduleTasks({_id: task.id});
            }
        },
        Job: async (updates) => {
            if (!updates.id) throw new UserInputError('Job id must be specified');
            return await upsert(context.mongodb, 'Job', updates, true, context.dataloaders);
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

    Job: {
        async task(parent, args, context, info) {
            return await query(context.mongodb, 'Task', [parent[info.fieldName]], undefined, context.dataloaders);
        },
        async createdBy(parent, args, context, info) {
            return await query(context.mongodb, 'Job', [parent[info.fieldName]], undefined, context.dataloaders);
        },
        async createdFrom(parent, args, context, info) {
            return await query(context.mongodb, 'Job', [parent[info.fieldName]], undefined, context.dataloaders);
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