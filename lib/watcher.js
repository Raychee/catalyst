const {ObjectID} = require('mongodb');
const {sleep} = require('@raychee/utils');
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
                if (error.name === 'MongoError' && [40573, 148].includes(error.code)) {
                    this.d('It seems db does not support watching');
                    this.isWatchable = false;
                } else {
                    this._makeStream();
                }
                this.onError(error);
            });
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
    
    constructor(coll, {debug, targetStatus, pollInterval, options}) {
        super(coll, {
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
        this.targetStatus = targetStatus;
        this.pollInterval = pollInterval;
        this._watches = {};
    }

    get watchingJobIds() {
        return Object.keys(this._watches).map(i => ObjectID(i));
    }

    isWatching(jobId) {
        return this._watches[jobId] != null;
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
                            this.watched(jobId, this.coll && this.coll.findOne({_id: ObjectID(jobId)}));
                        }
                    }, options.timeout * 1000);
                }
                return new Promise((resolve, reject) => {
                    this._watches[jobId] = {resolve, reject, options};
                    if (this.coll) {
                        this.coll.findOne({_id: jobId}).then(
                            (job) => {
                                if (this.targetStatus.includes(job.status)) {
                                    this.watched(jobId, job);
                                }
                            },
                            (e) => this.abort(jobId, e)
                        );
                    }
                });
            }
        }
    }

    watched(jobId, value) {
        const {resolve} = this._watches[jobId] || {};
        if (resolve) {
            this.d('JobWatcher watches job %s has stopped', jobId);
            resolve(value);
            delete this._watches[jobId];
        }
    }
    
    abort(jobId, error) {
        const {reject} = this._watches[jobId] || {};
        if (reject) {
            this.d('JobWatcher aborts watching of job %s', jobId);
            reject(error);
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
        if (!this.isWatchable) {
            this.d('JobWatcher use fallback method instead');
            for (const [jobId, {options}] of Object.entries(this._watches)) {
                this.watched(jobId, this._fallbackPolling(jobId, options));
            }
        }
    }
    
    async _fallbackPolling(jobId, {timeout} = {}) {
        if (!this.coll) {
            throw new SystemError('JobWatcher falls back to polling but JobWatcher.coll is undefined');
        }
        this.d('watching of job %s fallbacks to polling with timeout %s', jobId, timeout);
        const stop = timeout > 0 ? Date.now() + timeout * 1000 : Infinity;
        let j = undefined;
        while (true) {
            j = await this.coll.findOne({_id: ObjectID(jobId)});
            this.d('polling job %s: %j', jobId, j);
            if (j && this.targetStatus.includes(j.status)) {
                return j;
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
        return j;
    }

}

class JobWatcherManager {
    
    constructor(coll, {pollInterval = 1, options}) {
        this.coll = coll;
        this.pollInterval = pollInterval;
        this.options = options;
        this.jobWatchers = {};
    }
    
    getWatcher(targetStatus) {
        const key = targetStatus.join('|');
        let jobWatcher = this.jobWatchers[key];
        if (!jobWatcher) {
            jobWatcher = new JobWatcher(
                this.coll, {targetStatus, pollInterval: this.pollInterval, options: this.options}
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
