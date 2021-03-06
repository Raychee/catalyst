"use strict";

const debug = require('debug')('catalyst:logger');
const {isEmpty} = require('lodash');

const {ensureThunkSync, sleep, dedup, flatten, stringifyWith, truncate} = require('@raychee/utils');
const {
    JobRuntime, JobCrash, JobTimeout, JobCancellation, JobInterruption,
} = require('./error');


const LOGGING_LEVELS = {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3,
};


class Logger {

    constructor({category = '', prefixes = [], level} = {}) {
        this.category = category;
        this.prefixes = prefixes;
        this.loggingLevel = level;

        this.stdout = process.stdout.write.bind(process.stdout);
        this.stderr = process.stderr.write.bind(process.stderr);
    }

    print(...values) {
        this._log('INFO', this.stdout, '', false, ...values);
    }

    println(...values) {
        this._log('INFO', this.stdout, '', false, ...values, '\n');
    }

    reprint(...values) {
        this._log('INFO', this.stdout, '', false, '\r', ...values);
    }

    reprintln(...values) {
        this._log('INFO', this.stdout, '',false,'\r', ...values, '\n');
    }

    debug(...values) {
        this._log('DEBUG', this.stdout, 'Debug', true, ...values, '\n');
    }

    info(...values) {
        this._log('INFO', this.stdout, 'Info', true, ...values, '\n');
    }

    warn(...values) {
        this._log('WARN', this.stderr, 'Warn', true, ...values, '\n');
    }

    error(...values) {
        this._log('ERROR', this.stderr, 'Error', true, ...values, '\n');
    }

    delay(...values) {
        this._log('INFO', this.stdout, 'Delay', true, ...values, '\n');
    }

    wait(...values) {
        this._log('INFO', this.stdout, 'Wait', true, ...values, '\n');
    }

    retry(...values) {
        this._log('INFO', this.stdout, 'Retry', true, ...values, '\n');
    }

    start(...values) {
        this._log('INFO', this.stdout, 'Start', true, ...values, '\n');
    }

    complete(...values) {
        this._log('INFO', this.stdout, 'Complete', true, ...values, '\n');
    }

    cancel(...values) {
        this._log('INFO', this.stdout, 'Cancel', true, ...values, '\n');
    }

    fail(...values) {
        this._log('ERROR', this.stderr, 'Fail', true, ...values, '\n');
    }

    crash(...values) {
        this._log('ERROR', this.stderr, 'Crash', true, ...values, '\n');
    }

    catch(...values) {
        this._log('ERROR', this.stdout, 'Catch', true, ...values, '\n');
    }

    interrupt(...values) {
        this._log('ERROR', this.stderr, 'Interrupt', true, ...values, '\n');
    }

    heartAttack(...values) {
        this._log('ERROR', this.stderr, 'Heart Attack', true, ...values, '\n');
    }

    timeout(...values) {
        this._log('ERROR', this.stderr, 'Timeout', true, ...values, '\n');
    }

    _log(level, log, tag, singleLine, ...values) {
        if (this._level() > LOGGING_LEVELS[level]) return;
        let timestamp = this.SHOW_TIMESTAMP ? `${new Date().toLocaleString()} - ` : '';
        tag = [this.category, tag].filter(v => v).join(' ');
        tag = tag ? `${tag} - ` : '';
        let prefixes = ensureThunkSync(this.prefixes);
        prefixes = prefixes.length > 0 ? `${stringifyWith(prefixes, {delimiter: ' - '})} - ` : '';
        let r = '', n = '';
        let message = stringifyWith(values);
        if (message.startsWith('\r')) {
            r = '\r';
            message = message.slice(1);
        }
        if (this.TRUNCATE > 0) {
            message = truncate(message, {maxLength: this.TRUNCATE});
        }
        if (!lastMessageHasNewLine && singleLine) {
            n = '\n';
        }
        log(`${n}${r}${timestamp}${tag}${prefixes}${message}`);
        lastMessageHasNewLine = message.endsWith('\n');
    }

    _level() {
        return LOGGING_LEVELS[this.loggingLevel || this.LOGGING_LEVEL];
    }

}

let lastMessageHasNewLine = true;

Logger.prototype.LOGGING_LEVEL = 'INFO';
Logger.prototype.SHOW_TIMESTAMP = false;
Logger.prototype.TRUNCATE = -1;


class JobLogger extends Logger {

    cancel(code, ...values) {
        throw new JobCancellation(code, ...values);
    }

    crash(code, ...values) {
        throw new JobCrash(code, ...values);
    }

    fail(code, ...values) {
        throw new JobRuntime(code, ...values);
    }

    interrupt(code, ...values) {
        throw new JobInterruption(code, ...values);
    }

    timeout(code, ...values) {
        throw new JobTimeout(code, ...values);
    }

}


class StoreLogger extends JobLogger {

    constructor({collection, filter, ...options}) {
        super(options);
        this.collection = collection;
        this.filter = filter;

        this.push = dedup(StoreLogger.prototype.push.bind(this));
        this._pull = dedup(StoreLogger.prototype._pull.bind(this));
    }

    async push(store, {replace = false} = {}) {
        let result;
        if (replace) {
            const update = {...this.filter, data: store};
            const options = {upsert: true, returnOriginal: false};
            debug('%s.findOneAndUpdate(%j, %j, %j)', this.collection.collectionName, this.filter, update, options);
            result = await this.collection.findOneAndUpdate(this.filter, update, options);
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
            const options = {upsert: true, returnOriginal: false};
            debug('%s.findOneAndUpdate(%j, %j, %j)', this.collection.collectionName, this.filter, operation, options);
            result = await this.collection.findOneAndUpdate(this.filter, operation, options);
        }
        return result.value.data || {};
    }
    
    async pull(options = {}, job) {
        const {timeout = 604800} = options;
        const pulling = this._pull(options);
        if (!job || !job.sleep) return pulling;
        let pulled = false;
        pulling.finally(() => pulled = true);
        return await Promise.race([
            pulling,
            job.sleep(timeout, {wakeUpIf: () => pulled}),
        ]);
    }

    async _pull({waitUntil = () => true, checkInterval = 10, timeout = 604800, logInterval, message} = {}) {
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
