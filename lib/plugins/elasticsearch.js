const {Client} = require('@elastic/elasticsearch');


module.exports = {
    key(options) {
        return options;
    },
    async create(options) {
        return new ElasticSearch(this, options);
    },
    async destroy(elasticSearch) {
        await elasticSearch._close();
    }
};


class ElasticSearch {

    constructor(logger, {clientOptions, bulkOptions}) {
        this.logger = logger;
        this.clientOptions = clientOptions || {};
        this.bulkOptions = {
            retryIntervalForTooManyRequests: 1,
            maxRetriesForTooManyRequests: 0,
            ...bulkOptions,
        };
        this.client = undefined;
    }

    async bulk(logger, params, options) {
        logger = logger || this.logger;
        await this._connect();
        let resp;
        for (let trial = 0; trial <= this.bulkOptions.maxRetriesForTooManyRequests; trial++) {
            resp = await this._handlePromise(logger, this.client.bulk(params, options));
            if (resp.body.errors) {
                const errorItems = resp.body.items
                    .map((item, pos) => ({pos, ...Object.values(item)[0]}))
                    .filter(({error}) => error);
                if (errorItems.some(item => item.status !== 429)) {
                    logger.fail('_es_bulk_error', errorItems);
                }
                if (trial < this.bulkOptions.maxRetriesForTooManyRequests) {
                    logger.info(
                        'Bulk operation has failed because of too many requests. Will retry (',
                        trial + 1, '/', this.bulkOptions.maxRetriesForTooManyRequests,
                        ') in ', this.bulkOptions.retryIntervalForTooManyRequests, ' seconds.',
                    );
                } else {
                    logger.fail('_es_bulk_error', errorItems);
                }
            } else {
                return resp;
            }
        }
    }

    async _connect() {
        if (!this.client) {
            this.client = new Client(this.clientOptions);
        }
    }

    async _close() {
        if (this.client) {
            const client = this.client;
            this.client = undefined;
            await client.close();
        }
    }

    _handlePromise(logger, promise) {
        logger = logger || this.logger;
        return promise.catch(error => {
            logger.fail('_es_error', error);
        });
    }

}
