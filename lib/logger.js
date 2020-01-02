const {isEmpty} = require('lodash');

const {ensureThunkSync, sleep, dedup, flatten, stringifyWith} = require('@raychee/utils');
const {
    JobRuntime, JobCrash, JobTimeout, JobCancellation, JobInterruption, HeartAttack,
} = require('./error');


const LOGGING_LEVELS = {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3,
};


class Logger {

    constructor(category, prefixes = [], transform) {
        this.category = category;
        this.prefixes = prefixes;
        this.transform = transform;
    }

    debug(...values) {
        if (this._level() <= LOGGING_LEVELS.DEBUG) {
            this._log(console.log, `${this.category ? `${this.category} ` : ''}Debug`, ...values);
        }
    }

    info(...values) {
        if (this._level() <= LOGGING_LEVELS.INFO) {
            this._log(console.log, `${this.category ? `${this.category} ` : ''}Info`, ...values);
        }
    }

    warn(...values) {
        if (this._level() <= LOGGING_LEVELS.WARN) {
            this._log(console.error, `${this.category ? `${this.category} ` : ''}Warn`, ...values);
        }
    }

    error(...values) {
        if (this._level() <= LOGGING_LEVELS.ERROR) {
            this._log(console.error, `${this.category ? `${this.category} ` : ''}Error`, ...values);
            if (values.length === 1 && values[0] instanceof Error) {
                console.error(values[0]);
            }
        }
    }

    delay(...values) {
        if (this._level() <= LOGGING_LEVELS.INFO) {
            this._log(console.log, 'Delay', ...values);
        }
    }

    retry(...values) {
        if (this._level() <= LOGGING_LEVELS.INFO) {
            this._log(console.log, 'Retry', ...values);
        }
    }

    start(...values) {
        if (this._level() <= LOGGING_LEVELS.INFO) {
            this._log(console.log, 'Start', ...values);
        }
    }

    complete(...values) {
        if (this._level() <= LOGGING_LEVELS.INFO) {
            this._log(console.log, 'Complete', ...values);
        }
    }

    cancel(...values) {
        if (this._level() <= LOGGING_LEVELS.INFO) {
            this._log(console.log, 'Cancel', ...values);
        }
    }

    fail(...values) {
        if (this._level() <= LOGGING_LEVELS.ERROR) {
            this._log(console.error, 'Fail', ...values);
        }
    }

    crash(...values) {
        if (this._level() <= LOGGING_LEVELS.ERROR) {
            this._log(console.error, 'Crash', ...values);
        }
    }

    catch(...values) {
        if (this._level() <= LOGGING_LEVELS.ERROR) {
            this._log(console.log, 'Catch', ...values);
        }
    }

    interrupt(...values) {
        if (this._level() <= LOGGING_LEVELS.ERROR) {
            this._log(console.error, 'Interrupt', ...values);
        }
    }

    heartAttack(...values) {
        if (this._level() <= LOGGING_LEVELS.ERROR) {
            this._log(console.error, 'Heart Attack', ...values);
        }
    }

    timeout(...values) {
        if (this._level() <= LOGGING_LEVELS.ERROR) {
            this._log(console.error, 'Timeout', ...values);
        }
    }

    _log(log, logCategory, ...values) {
        const prefixes = ensureThunkSync(this.prefixes);
        const timestamp = this.SHOW_TIMESTAMP ? `${new Date().toLocaleString()} - ` : '';
        log(`${timestamp}${logCategory} - ${stringifyWith(prefixes, {transform: this.transform, delimiter: ' - '})} - ${stringifyWith(values, {transform: this.transform})}`);
    }

    _level() {
        return LOGGING_LEVELS[this.LOGGING_LEVEL];
    }

}


Logger.prototype.LOGGING_LEVEL = 'INFO';
Logger.prototype.SHOW_TIMESTAMP = false;


class JobLogger extends Logger {

    cancel(code, ...values) {
        throw new JobCancellation(code, stringifyWith(values, {transform: this.transform}));
    }

    crash(code, ...values) {
        throw new JobCrash(code, stringifyWith(values, {transform: this.transform}));
    }

    fail(code, ...values) {
        throw new JobRuntime(code, stringifyWith(values, {transform: this.transform}));
    }

    interrupt(code, ...values) {
        throw new JobInterruption(code, stringifyWith(values, {transform: this.transform}));
    }

    timeout(code, ...values) {
        throw new JobTimeout(code, stringifyWith(values, {transform: this.transform}));
    }

}


class StoreLogger extends JobLogger {

    constructor(storeCollection, identity, ...args) {
        super(...args);
        this.storeCollection = storeCollection;
        this.identity = identity;

        this.push = dedup(StoreLogger.prototype.push.bind(this));
        this.pull = dedup(StoreLogger.prototype.pull.bind(this));
    }

    async push(store, {replace = false} = {}) {
        let result;
        if (replace) {
            result = await this.storeCollection.findOneAndUpdate(
                this.identity, {...this.identity, data: store}, {upsert: true, returnOriginal: false}
            );
        } else {
            const updates = flatten(store, {prefix: 'data'});
            const unsets = {};
            Object.entries(updates)
                .filter(([, v]) => v === null)
                .forEach(([p]) => {
                    delete updates[p];
                    unsets[p] = '';
                });
            const operation = {};
            if (!isEmpty(updates)) operation.$set = updates;
            if (!isEmpty(unsets)) operation.$unset = unsets;
            result = await this.storeCollection.findOneAndUpdate(
                this.identity, operation, {upsert: true, returnOriginal: false}
            );
        }
        return result.value.data || {};
    }

    async pull({waitUntil = () => true, checkInterval = 10, timeout = 604800, logInterval, message} = {}) {
        const start = Date.now();
        let lastInfoTime = 0;
        while (true) {
            const row = await this.storeCollection.findOne(this.identity);
            const store = row && row.data || {};
            if (await waitUntil(store)) {
                return store;
            }
            if (Date.now() + checkInterval * 1000 - start >= timeout * 1000) {
                this.crash('_store_pull_timeout', 'Pulling data from store timed out: ', timeout);
            }
            if (Date.now() - lastInfoTime > (logInterval || checkInterval * 10) * 1000) {
                this.warn('Attention! Wait for store update of ', this.identity, message ? `: ${message}` : '');
                lastInfoTime = Date.now();
            }
            await sleep(checkInterval * 1000);
        }

    }

}


module.exports = {
    Logger,
    JobLogger,
    StoreLogger,
};