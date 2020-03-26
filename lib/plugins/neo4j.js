const {isPlainObject} = require('lodash');
const {driver: neo4jDriver, auth, isInt, types, temporal} = require('neo4j-driver');

const {replaceAll} = require('@raychee/utils');


module.exports = {
    key({
            host, port, user, password, connectionOptions,
            otherOptions = {debug: false, showProgressEvery: undefined}
        } = {}) {
        return {host, port, user, password, connectionOptions, otherOptions};
    },
    create(
        {
            host, port, user, password, connectionOptions,
            otherOptions = {debug: false, showProgressEvery: undefined}
        } = {}
    ) {
        const driver = neo4jDriver(
            `bolt://${host}:${port}`,
            user && password ? auth.basic(user, password) : undefined,
            connectionOptions
        );
        return new Neo4J(this, driver, otherOptions);
    },
    async destroy(neo4j) {
        await neo4j.driver.close();
    }
};


class Neo4J {

    constructor(logger, driver, options) {
        this.logger = logger;
        this.driver = driver;
        this.options = options;
    }

    run(logger, cypher, params) {
        logger = logger || this.logger;
        const session = this.driver.session();

        const transformRecord = (value) => {
            if (this.isTemporal(logger, value)) {
                return new Date(value.toString());
            } else if (value instanceof types.Node) {
                const node = {
                    _id: value.identity,
                    _label: value.labels[0],
                };
                for (const [p, v] of Object.entries(value.properties)) {
                    node[p] = transformRecord(v);
                }
                return node;
            } else if (value instanceof types.Relationship) {
                const rel = {
                    _id: value.identity,
                    _label: value.type,
                    _src: value.start,
                    _dst: value.end,
                };
                for (const [p, v] of Object.entries(value.properties)) {
                    rel[p] = transformRecord(v);
                }
                return rel;
            } else if (value instanceof types.Record) {
                const record = {};
                value.forEach((v, p) => {
                    return record[p] = transformRecord(v);
                });
                return record;
            } else if (isInt(value)) {
                if (value.inSafeRange()) {
                    return value.toInt();
                } else {
                    return value.toNumber();
                }
            } else if (Array.isArray(value)) {
                return value.map(transformRecord);
            } else {
                return value;
            }
        };

        if (this.options.debug) {
            logger.debug(cypher);
            logger.debug(params);
        }
        const result = session.run(cypher, this.ensureCypherValue(logger, params));
        return {

            then: (onResolve, onReject) => new Promise((resolve, reject) => {
                result
                    .then(
                        ({records, summary}) =>
                            resolve({
                                records: records.map(transformRecord),
                                summary,
                            }),
                        error => reject(error)
                    )
                    .finally(() => session.close());
            }).then(onResolve, onReject),

            [Symbol.asyncIterator]: () => {
                const results = [];
                let isCompleted = false, error = undefined, count = 0;
                result.subscribe({
                    onNext: record => {
                        count++;
                        if (count % this.options.showProgressEvery === 0) {
                            process.stdout.write('.');
                        }
                        const doc = transformRecord(record);
                        const index = results.findIndex(r => r.resolve);
                        if (index >= 0) {
                            const [result] = results.splice(index, 1);
                            result.resolve({value: doc, done: false});
                        } else {
                            const promise = Promise.resolve({value: doc, done: false});
                            results.push({promise});
                        }
                    },
                    onCompleted: () => {
                        session.close();
                        if (this.options.showProgressEvery) {
                            process.stdout.write('✓\n');
                        }
                        isCompleted = true;
                        for (let index = results.length - 1; index >= 0; index--) {
                            const result = results[index];
                            if (!result.resolve) continue;
                            result.resolve({done: true});
                            results.splice(index, 1);
                        }
                    },
                    onError: err => {
                        session.close();
                        if (this.options.showProgressEvery === 0) {
                            process.stdout.write('✗\n');
                        }
                        error = err;
                        for (let index = results.length - 1; index >= 0; index--) {
                            const result = results[index];
                            if (!result.reject) continue;
                            result.reject(err);
                            results.splice(index, 1);
                        }
                    }
                });
                return {
                    next: () => {
                        const index = results.findIndex(r => r.promise);
                        if (index >= 0) {
                            const [result] = results.splice(index, 1);
                            return result.promise;
                        } else if (isCompleted) {
                            return Promise.resolve({done: true});
                        } else if (error) {
                            return Promise.reject(error);
                        } else {
                            return new Promise((resolve, reject) => {
                                results.push({resolve, reject});
                            });
                        }
                    }
                };
            },

        };
    }

    isTemporal(logger, value) {
        return temporal.isDate(value) || temporal.isDateTime(value) || temporal.isLocalDateTime(value) ||
            temporal.isLocalTime(value) || temporal.isTime(value);
    }

    makeName(logger, hint, params, value) {
        logger = logger || this.logger;
        if (value !== undefined) {
            for (const [n, v] of Object.entries(params)) {
                if (
                    v === value ||
                    this.isTemporal(logger, v) && this.isTemporal(logger, value) &&
                    v.toString() === value.toString()
                ) {
                    return n;
                }
            }
        }
        hint = hint.replace(/[.-]/g, '_').toLowerCase();
        let name = hint;
        let i = 2;
        while (name in params) {
            name = `${hint}_${i++}`;
        }
        params[name] = value;
        return name;
    }

    ensureCypherValue(logger, value) {
        if (value instanceof Date) {
            return types.DateTime.fromStandardDate(value);
        } else if (Array.isArray(value)) {
            return value.map(v => this.ensureCypherValue(logger, v));
        } else if (isPlainObject(value)) {
            const ret = {};
            for (const [p, v] of Object.entries(value)) {
                ret[p] = this.ensureCypherValue(logger, v);
            }
            return ret;
        } else {
            return value;
        }
    }

    makeCypherParam(logger, hint, params, value) {
        logger = logger || this.logger;
        return `$${this.makeName(logger, hint, params, this.ensureCypherValue(logger, value))}`;
    }

    makeCypher(logger, snippets) {
        logger = logger || this.logger;
        const cyphers = [], namespace = {}, params = {};
        for (let snippet of snippets) {
            let cypher = undefined, param = undefined;
            if (typeof snippet === 'string') {
                cypher = snippet;
            } else {
                cypher = snippet.cypher;
                param = snippet.params;
            }
            const names = {};
            for (const name of new Set(cypher.match(/\$\$\w+/g))) {
                names[name] = this.makeName(logger, name.slice(2), namespace);
            }
            const varNames = {};
            if (param) {
                for (const [varName, value] of Object.entries(param)) {
                    varNames[`$${varName}`] = this.makeCypherParam(logger, varName, params, value);
                }
            }
            cyphers.push(replaceAll(cypher, {...names, ...varNames}));
        }
        return [cyphers.join('\n'), params];
    };
}
