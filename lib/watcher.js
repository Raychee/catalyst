const {ObjectID} = require('mongodb');
const {isPlainObject} = require('lodash');
const {sleep, BatchLoader} = require('@raychee/utils');
const {SystemError} = require('./error');


class Watcher {

    constructor(coll, {debug, pipeline, options, onEvent, onError}) {
        this.d = debug || require('debug')('catalyst:watcher');
        this.coll = coll;
        this.pipeline = pipeline;
        this.options = options;

        if (onEvent) this.onEvent = onEvent;
        if (onError) this.onError = onError;

        this.isWatchable = true;
        this.stream = undefined;
    }

    watch() {
        if (this.isWatchable && !this.stream) {
            this._makeStream();
        }
    }

    _makeStream() {
        if (!this.isWatchable || !this.coll) return;
        this.d('Watcher starts');
        this.stream = this.coll.watch(this.pipeline, this.options);
        this.stream
            .on('change', (event) => {
                this.d('Watcher watches a change event: %j', event);
                this.onEvent(event);
            })
            .on('error', (error) => {
                this.d('Watcher encounters an error: %s', error);
                this.stream.close();
                this.stream = undefined;
                if (this._isWatchNotSupported(error)) {
                    this.d('It seems db does not support watching');
                    this.isWatchable = false;
                } else {
                    this._makeStream();
                }
                this.onError(error);
            });
    }

    _isWatchNotSupported(error) {
        return error.name === 'MongoError' && [40573, 148].includes(error.code);
    }


    close() {
        if (this.stream) {
            this.d('Watcher closes');
            this.stream.close();
            this.stream = undefined;
        }
    }

    onEvent(event) {
    }

    onError(error) {
    }

}


class JobWatcher extends Watcher {

    constructor(coll, jobLoader, {debug, targetStatus, pollInterval, options}) {
        super(coll, {
            debug: debug || require('debug')('catalyst:JobWatcher'),
            pipeline: [{
                $match: {
                    $or: [
                        {
                            'operationType': 'update',
                            'updateDescription.updatedFields.status': {$in: targetStatus}
                        },
                        {
                            'operationType': 'insert',
                            'fullDocument.status': {$in: targetStatus}
                        },
                    ]
                }
            }],
            options: {...options, fullDocument: 'updateLookup'}
        });
        this.jobLoader = jobLoader;
        this.targetStatus = targetStatus;
        this.pollInterval = pollInterval;
        this._watches = {};
    }

    get watchingJobIds() {
        return Object.keys(this._watches).map(i => ObjectID(i));
    }

    isWatching(jobId) {
        return (this._watches[jobId] || []).length > 0;
    }

    async watch(jobId, options = {}) {
        super.watch();
        if (jobId) {
            if (!this.isWatchable) {
                return this._fallbackPolling(jobId, options);
            } else {
                if (options.timeout >= 0) {
                    setTimeout(() => {
                        if (this.isWatching(jobId)) {
                            this.watched(jobId, this.coll && this.jobLoader.load(jobId));
                        }
                    }, options.timeout * 1000);
                }
                return new Promise((resolve, reject) => {
                    const watches = this._watches[jobId] || [];
                    this._watches[jobId] = watches;
                    this.d('add watch for job %s to stop, %s -> %s', jobId, watches.length, watches.length + 1);
                    watches.push({resolve, reject, options});
                    if (this.coll) {
                        this.jobLoader.load(jobId)
                            .then(job => {
                                    if (!job) {
                                        throw new SystemError('job ', jobId, ' is not found in db');
                                    }
                                    if (this.targetStatus.includes(job.status)) {
                                        this.watched(jobId, job);
                                    }
                                },
                            )
                            .catch(e => this.abort(jobId, e));
                    }
                });
            }
        }
    }

    watched(jobId, value) {
        const watches = this._watches[jobId] || [];
        if (watches.length > 0) {
            if (isPlainObject(value)) {
                this.d('job %s has stopped, resolving %s watches', jobId, watches.length);
            } else {
                this.d('all %s watches of job %s is delegated to promise', watches.length, jobId);
            }
            for (const {resolve} of watches) {
                resolve(value);
            }
            delete this._watches[jobId];
        }
    }

    abort(jobId, error) {
        const watches = this._watches[jobId] || [];
        if (watches.length > 0) {
            this.d('aborts watching of job %s with error %s, rejecting %s watches', jobId, error, watches.length);
            for (const {reject} of watches) {
                reject(error);
            }
            delete this._watches[jobId];
        }
    }

    abortAll(error) {
        for (const jobId of this.watchingJobIds) {
            this.abort(jobId, error);
        }
    }

    onEvent(event) {
        const jobId = event.documentKey._id;
        const {local, ...job} = event.fullDocument;
        this.watched(jobId, job);
    }

    onError(error) {
        if (this._isWatchNotSupported(error)) {
            this.d('watcher is not supported, make all existing watches fallback to polling');
            for (const [jobId, watches] of Object.entries(this._watches)) {
                for (const {resolve, options} of watches) {
                    resolve(this._fallbackPolling(jobId, options));
                }
            }
            this._watches = {};
        }
    }

    async _fallbackPolling(jobId, {timeout} = {}) {
        if (!this.coll) {
            throw new SystemError('JobWatcher falls back to polling but JobWatcher.coll is undefined');
        }
        this.d('watching of job %s fallback to polling with timeout %s', jobId, timeout);
        const stop = timeout > 0 ? Date.now() + timeout * 1000 : Infinity;
        let job = undefined;
        while (true) {
            job = await this.jobLoader.load(jobId);
            this.d('polling job %s: %j', jobId, job);
            if (job && this.targetStatus.includes(job.status)) {
                return job;
            }
            let interval = stop - Date.now();
            if (interval > this.pollInterval * 1000) {
                interval = this.pollInterval * 1000;
            }
            if (interval <= 0) {
                this.d('polling of job %s timeout', jobId);
                break;
            }
            await sleep(interval);
        }
        return job;
    }

}

class JobWatcherManager {

    /**
     * @param {import('mongodb').Collection} [coll]
     * @param {number} pollInterval
     * @param options
     */
    constructor(coll, {pollInterval = 1, options} = {}) {
        this.d = require('debug')('catalyst:JobWatcherManager');
        this.coll = coll;
        this.pollInterval = pollInterval;
        this.options = options;
        this.jobWatchers = {};
        this.jobLoader = new BatchLoader(async _ids => {
            const results = {};
            const query = {_id: {$in: [...new Set(_ids)].map(i => ObjectID(i))}};
            for await (const doc of this.coll.find(query)) {
                results[doc._id] = doc;
            }
            this.d('jobs.find(%j) -> %j', query, results);
            return _ids.map(_id => results[_id]);
        }, {maxSize: 1000, maxWait: 1});
    }

    getWatcher(targetStatus) {
        const key = [...targetStatus].sort().join('|');
        let jobWatcher = this.jobWatchers[key];
        if (!jobWatcher) {
            jobWatcher = new JobWatcher(
                this.coll, this.jobLoader,
                {targetStatus, pollInterval: this.pollInterval, options: this.options}
            );
            this.jobWatchers[key] = jobWatcher;
        }
        return jobWatcher;
    }

    abortAll(error) {
        for (const jobWatcher of Object.values(this.jobWatchers)) {
            jobWatcher.abortAll(error);
        }
    }

    closeAll() {
        for (const jobWatcher of Object.values(this.jobWatchers)) {
            jobWatcher.close();
        }
    }

}


module.exports = {
    Watcher,
    JobWatcher,
    JobWatcherManager,
}
