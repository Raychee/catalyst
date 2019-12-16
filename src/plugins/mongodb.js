const {MongoClient} = require('mongodb');
const {setWith, get} = require('lodash');

const {isThenable} = require('@raychee/utils');


module.exports = {

    key(
        {
            host, port, user, password, db, collection,
            connectionOptions = {useNewUrlParser: true, useUnifiedTopology: true},
            queryOptions = {},
            aggregationOptions = {allowDiskUse: true},
            otherOptions = {},
        }
    ) {
        return {host, port, user, password, db, collection, connectionOptions, queryOptions, aggregationOptions, otherOptions};
    },

    async create(
        {
            host, port, user, password, db, collection,
            connectionOptions = {useNewUrlParser: true, useUnifiedTopology: true},
            queryOptions = {},
            aggregationOptions = {allowDiskUse: true},
            otherOptions = {},
        },
        {pluginLoader}
    ) {
        const options = {host, port, user, password, connectionOptions, queryOptions, aggregationOptions, otherOptions};
        if (db || collection) {
            const plugin = await pluginLoader.get({type: 'mongodb', ...options});
            return plugin.use(db, collection);
        } else {
            return new MongoDB(this, options, pluginLoader);
        }
    },

    async destroy(mongodb) {
        await mongodb._close();
    }

};


class MongoDB {

    constructor(logger, options, pluginLoader, _state = {}) {
        this.logger = logger;
        this.options = options;
        this.pluginLoader = pluginLoader;

        this._state = _state;
    }

    use(logger, db, collection) {
        logger = logger || this.logger;
        let options = this.options;
        if (db && collection) {
            options = {...this.options, db, collection};
        } else if (db) {
            if (this.options.db) {
                options = {...this.options, collection: db};
            } else {
                options = {...this.options, db};
            }
        } else if (collection) {
            logger.crash('internal', 'cannot call this.use(collection) without db');
        } else {
            return this;
        }
        return this.pluginLoader.create(new MongoDB(this.logger, options, this.pluginLoader, this._state), logger);
    }

    aggregate(logger, pipeline, options) {
        logger = logger || this.logger;
        options = {...this.options.aggregationOptions, ...options};
        return this._handleCursor(
            logger, this._connect(logger).then(coll => coll.aggregate(pipeline, options))
        );
    }

    find(logger, query, options) {
        logger = logger || this.logger;
        options = {...this.options.queryOptions, ...options};
        return this._handleCursor(
            logger, this._connect(logger).then(coll => coll.find(query, options))
        );
    }

    findOne(logger, query, options) {
        logger = logger || this.logger;
        options = {...this.options.queryOptions, ...options};
        return this._handlePromise(
            logger, this._connect(logger).then(coll => coll.findOne(query, options))
        );
    }

    countDocuments(logger, query, options) {
        logger = logger || this.logger;
        options = {...this.options.queryOptions, ...options};
        return this._handlePromise(
            logger, this._connect(logger).then(coll => coll.countDocuments(query, options))
        );
    }

    updateMany(logger, filter, update, options) {
        logger = logger || this.logger;
        return this._handlePromise(
            logger, this._connect(logger).then(coll => coll.updateMany(filter, update, options))
        );
    }

    deleteMany(logger, filter, options) {
        logger = logger || this.logger;
        return this._handlePromise(
            logger, this._connect(logger).then(coll => coll.deleteMany(filter, options))
        );
    }

    drop(logger, options) {
        logger = logger || this.logger;
        return this._handlePromise(
            logger, this._connect(logger).then(coll => coll.drop(options).catch(e => {
                if (e.message.match(/ns not found/)) {
                    return null;
                } else {
                    throw e;
                }
            }))
        );
    }

    createIndexes(logger, indexSpecs, options) {
        logger = logger || this.logger;
        return this._handlePromise(
            logger, this._connect(logger).then(coll => coll.createIndexes(indexSpecs, options))
        );
    }

    async _connect(logger) {
        logger = logger || this.logger;
        const {host, port, user, password, db, collection, connectionOptions} = this.options;
        if (!db || !collection) {
            logger.crash('internal', 'this._db or this._collection is undefined');
        }
        if (!this._state.client) {
            const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
            const uri = `mongodb://${auth}${host}${port ? `:${port}` : ''}`;
            const client = MongoClient(uri, connectionOptions);
            await client.connect();
            this._state.client = client;
        }
        let coll = get(this._state, ['pool', db, collection]);
        if (!coll) {
            coll = this._state.client.db(db).collection(collection);
            setWith(this._state, ['pool', db, collection], coll, Object);
        }
        return coll;
    }

    async _close() {
        if (this._state.client) {
            await this._state.client.close();
            this._state = {};
        }
    }

    _handlePromise(logger, promise) {
        logger = logger || this.logger;
        return promise.catch(error => {
            switch (error.name) {
                case 'MongoNetworkError':
                case 'MongoTimeoutError':
                case 'MongoWriteConcernError':
                    logger.fail(error.name, error);
                    break;
                default:
                    throw error;
            }
        });
    }

    _handleCursor(logger, cursor) {
        logger = logger || this.logger;
        const ops = [];
        return new Proxy(cursor, {
            get: (target, p, receiver) => {
                switch (p) {
                    case Symbol.asyncIterator:
                        let options = this.options;
                        return async function* () {
                            let count = 0;
                            while (await receiver.hasNext()) {
                                count++;
                                if (count % options.otherOptions.showProgressEvery === 0) {
                                    process.stdout.write('.');
                                }
                                yield await receiver.next();
                            }
                            if (options.otherOptions.showProgressEvery) {
                                process.stdout.write('✓\n');
                            }
                        };
                    case 'then':
                        return (onFulfilled, onRejected) => {
                            return receiver.next().then(onFulfilled, onRejected);
                        };
                    case 'count':
                    case 'explain':
                    case 'forEach':
                    case 'hasNext':
                    case 'next':
                    case 'toArray':
                        return (...args) => {
                            return this._handlePromise(logger, (async () => {
                                if (isThenable(target)) {
                                    target = await target;
                                }
                                for (const {method, args} of ops) {
                                    target = target[method](...args);
                                }
                                return await target[p](...args);
                            })());
                        };
                    default:
                        return (...args) => {
                            ops.push({method: p, args});
                            return receiver;
                        };
                }
            }
        });
    }

}