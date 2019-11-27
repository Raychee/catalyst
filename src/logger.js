const {CrawlerError, CrawlerIntentionalCrash, CrawlerCancellation, CrawlerInterruption} = require('./error');
const {ensureThunkSync, sleep, dedup, flatten, stringifyWith} = require('./utils');


class Logger {

    constructor(category, prefixes = [], transform) {
        this.category = category;
        this.prefixes = prefixes;
        this.transform = transform;
    }

    debug(...values) {
        this._log(console.log, `${this.category ? `${this.category} ` : ''}Debug`, ...values);
    }

    info(...values) {
        this._log(console.log, `${this.category ? `${this.category} ` : ''}Info`, ...values);
    }

    warn(...values) {
        this._log(console.error, `${this.category ? `${this.category} ` : ''}Warn`, ...values);
    }

    error(...values) {
        this._log(console.error, `${this.category ? `${this.category} ` : ''}Error`, ...values);
        if (values.length === 1 && values[0] instanceof Error) {
            console.error(values[0]);
        }
    }

    cancel(code, ...values) {
        throw new CrawlerCancellation(code, stringifyWith(values, {transform: this.transform}), this);
    }

    crash(code, ...values) {
        throw new CrawlerIntentionalCrash(code, stringifyWith(values, {transform: this.transform}), this);
    }

    fail(code, ...values) {
        throw new CrawlerError(code, stringifyWith(values, {transform: this.transform}), this);
    }

    interrupt(code, ...values) {
        throw new CrawlerInterruption(code, stringifyWith(values, {transform: this.transform}), this);
    }

    _log(log, logCategory, ...values) {
        const prefixes = ensureThunkSync(this.prefixes);
        const timestamp = process.env.KCARD_RUNTIME_STAGE === 'local' ? `${new Date().toLocaleString()} - ` : '';
        log(`${timestamp}${logCategory} - ${stringifyWith(prefixes, {transform: this.transform, delimiter: ' - '})} - ${stringifyWith(values, {transform: this.transform})}`);
    }

}


class JobLogger extends Logger {

    delay(...values) {
        this._log(console.log, 'Delay', ...values);
    }

    retry(...values) {
        this._log(console.log, 'Retry', ...values);
    }

    start(...values) {
        this._log(console.log, 'Start', ...values);
    }

    complete(...values) {
        this._log(console.log, 'Complete', ...values);
    }

    cancel(...values) {
        this._log(console.error, 'Cancel', ...values);
    }

    fail(...values) {
        this._log(console.error, 'Fail', ...values);
    }

    crash(...values) {
        this._log(console.error, 'Crash', ...values);
    }

    catch(...values) {
        this._log(console.log, 'Catch', ...values);
    }

    interrupt(...values) {
        this._log(console.error, 'Interrupt', ...values);
    }

}


class StoreLogger extends Logger {

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
            result = await this.storeCollection.findOneAndUpdate(
                this.identity, {$set: updates}, {upsert: true, returnOriginal: false}
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