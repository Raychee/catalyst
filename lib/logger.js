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

    constructor({category = '', prefixes = [], transform, level} = {}) {
        this.category = category;
        this.prefixes = prefixes;
        this.transform = transform;
        this.loggingLevel = level;
    }

    print(...values) {
        const message = this._log('INFO', process.stdout.write, '', false, ...values);
        this.lastHasNoNewLine = message && !message.endsWith('\n');
    }

    println(...values) {
        this._log('INFO', console.log, '', false, ...values);
    }

    reprint(...values) {
        const message = this._log('INFO', process.stdout.write, '', false, '\r', ...values);
        this.lastHasNoNewLine = message && !message.endsWith('\n');
    }

    reprintln(...values) {
        this._log('INFO', console.log, '',false,'\r', ...values);
    }

    debug(...values) {
        this._log('DEBUG', console.log, 'Debug', true, ...values);
    }

    info(...values) {
        this._log('INFO', console.log, 'Info', true, ...values);
    }

    warn(...values) {
        this._log('WARN', console.error, 'Warn', true, ...values);
    }

    error(...values) {
        this._log('ERROR', console.error, 'Error', true, ...values);
    }

    delay(...values) {
        this._log('INFO', console.log, 'Delay', true, ...values);
    }

    retry(...values) {
        this._log('INFO', console.log, 'Retry', true, ...values);
    }

    start(...values) {
        this._log('INFO', console.log, 'Start', true, ...values);
    }

    complete(...values) {
        this._log('INFO', console.log, 'Complete', true, ...values);
    }

    cancel(...values) {
        this._log('INFO', console.log, 'Cancel', true, ...values);
    }

    fail(...values) {
        this._log('ERROR', console.error, 'Fail', true, ...values);
    }

    crash(...values) {
        this._log('ERROR', console.error, 'Crash', true, ...values);
    }

    catch(...values) {
        this._log('ERROR', console.log, 'Catch', true, ...values);
    }

    interrupt(...values) {
        this._log('ERROR', console.error, 'Interrupt', true, ...values);
    }

    heartAttack(...values) {
        this._log('ERROR', console.error, 'Heart Attack', true, ...values);
    }

    timeout(...values) {
        this._log('ERROR', console.error, 'Timeout', true, ...values);
    }

    _log(level, log, tag, singleLine, ...values) {
        if (this._level() > LOGGING_LEVELS[level]) return;
        let timestamp = this.SHOW_TIMESTAMP ? `${new Date().toLocaleString()} - ` : '';
        tag = [this.category, tag].filter(v => v).join(' ');
        tag = tag ? `${tag} - ` : '';
        let prefixes = ensureThunkSync(this.prefixes);
        prefixes = prefixes.length > 0 ? `${stringifyWith(prefixes, {
            transform: this.transform,
            delimiter: ' - '
        })} - ` : '';
        let message = stringifyWith(values, {transform: this.transform});
        if (this.lastHasNoNewLine && singleLine) {
            message = '\n' + message;
        }
        log(`${timestamp}${tag}${prefixes}${message}`);
        this.lastHasNoNewLine = false;
        return message;
    }

    _level() {
        return LOGGING_LEVELS[this.loggingLevel || this.LOGGING_LEVEL];
    }

}

Logger.prototype.LOGGING_LEVEL = 'INFO';
Logger.prototype.SHOW_TIMESTAMP = false;
Logger.prototype.lastHasNoNewLine = false;


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

    constructor({collection, filter, ...options}) {
        super(options);
        this.collection = collection;
        this.filter = filter;

        this.push = dedup(StoreLogger.prototype.push.bind(this));
        this.pull = dedup(StoreLogger.prototype.pull.bind(this));
    }

    async push(store, {replace = false} = {}) {
        let result;
        if (replace) {
            result = await this.collection.findOneAndUpdate(
                this.filter, {...this.filter, data: store}, {upsert: true, returnOriginal: false}
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
            result = await this.collection.findOneAndUpdate(
                this.filter, operation, {upsert: true, returnOriginal: false}
            );
        }
        return result.value.data || {};
    }

    async pull({waitUntil = () => true, checkInterval = 10, timeout = 604800, logInterval, message} = {}) {
        const start = Date.now();
        let lastInfoTime = 0;
        while (true) {
            const row = await this.collection.findOne(this.filter);
            const store = row && row.data || {};
            if (await waitUntil(store)) {
                return store;
            }
            if (Date.now() + checkInterval * 1000 - start >= timeout * 1000) {
                this.crash('_store_pull_timeout', 'Pulling data from store timed out: ', timeout);
            }
            if (Date.now() - lastInfoTime > (logInterval || checkInterval * 10) * 1000) {
                this.warn('Attention! Wait for store update of ', this.filter, message ? `: ${message}` : '');
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