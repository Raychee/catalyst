const {get, isEmpty} = require('lodash');

const {sleep, dedup, limit, requestWithTimeout} = require('@raychee/utils');


class Dummy {
    constructor(proxy) {
        this.proxy = proxy;
    }

    async get() {
        return this.proxy;
    }

    async touch() {
    }

    async deprecate() {
    }

    async remove() {
    }
}


module.exports = function ({proxyTypes = {}} = {}) {

    class Proxies {

        constructor(logger, name, type, options, stored = false) {
            this.logger = logger;
            this.name = name;
            this.stored = stored;
            this.proxies = {};
            if (!stored) {
                this._load({type, options});
                if (!this.options.url) {
                    this.logger.crash('_proxies_crash', 'url must be specified');
                }
            }

            this.nextTimeRequest = 0;

            this._init = dedup(Proxies.prototype._init.bind(this));
            this._request = dedup(Proxies.prototype._request.bind(this), {key: null});
            this._get = limit(Proxies.prototype._get.bind(this), 1);
            this.__syncStore = Proxies.prototype._syncStoreForce.bind(this);
        }

        async _init() {
            if (this.stored) {
                const store = get(await this.logger.pull(), this.name);
                if (!store) {
                    this.logger.crash('_proxies_crash', 'invalid proxies name: ', this.name, ', please make sure: ',
                        '1. there is a document in the internal table service.Store that matches filter {plugin: \'proxies\'}, ',
                        '2. there is a valid identities options entry under document field \'data.', this.name, '\''
                    );
                }
                this._load(store);
            }
        }

        _load({type, options, proxies = {}} = {}) {
            const minIntervalBetweenStoreUpdate = get(this.options, 'minIntervalBetweenStoreUpdate');
            const requestTimeout = get(this.options, 'requestTimeout');
            // const {
            //     url, maxDeprecationsBeforeRemoval = 1, minIntervalBetweenUse = 5, recentlyUsedFirst = false,
            //     minIntervalBetweenRequest = 5, minIntervalBetweenStoreUpdate = 10,
            //     useProxyToLoadProxies, proxiesWithIdentityTTL = 24 * 60 * 60, requestTimeout = 10,
            // } = options;
            this.options = this._makeOptions(options);
            if (minIntervalBetweenStoreUpdate !== this.options.minIntervalBetweenStoreUpdate) {
                this.__syncStore = dedup(
                    Proxies.prototype._syncStoreForce.bind(this),
                    {within: this.options.minIntervalBetweenStoreUpdate * 1000}
                );
            }
            if (requestTimeout !== this.options.requestTimeout) {
                this.request = requestWithTimeout(this.options.requestTimeout * 1000);
            }
            if (type) this.type = type;
            if (!proxyTypes[this.type]) {
                this.logger.crash('_proxies_crash', 'unknown proxies type: ', this.type);
            }
            for (const proxy of this._iterProxies(proxies)) {
                const id = this._id(proxy);
                this.proxies[id] = {...proxy, ...this.proxies[id]};
            }
        }

        async _request(logger) {
            logger = logger || this.logger;
            if (!this.options.url) {
                const store = await this.logger.pull({
                    waitUntil: s => get(s, [this.name, 'options', 'url']),
                    message: `${this.name}.options.url need to be valid`
                });
                this._load(store);
            }
            const waitInterval = this.nextTimeRequest - Date.now();
            if (waitInterval > 0) {
                this._info(logger, 'Wait ', waitInterval / 1000, ' seconds before refreshing proxy list through ', this.options.url);
                await sleep(waitInterval);
            }
            while (true) {
                this._info(logger, 'Refresh proxy list: ', this.options.url);
                let resp, error, proxies = [];
                const reqOptions = {uri: this.options.url, ...proxyTypes[this.type].requestOptions};
                if (this.options.useProxyToLoadProxies) {
                    reqOptions.proxy = this.options.useProxyToLoadProxies;
                }
                try {
                    resp = await this.request(reqOptions);
                } catch (e) {
                    error = e;
                }
                this.nextTimeRequest = Date.now() + this.options.minIntervalBetweenRequest * 1000;
                try {
                    proxies = await proxyTypes[this.type].requestParser({resp, error});
                } catch (e) {
                    this._warn(logger, 'Failed parsing proxy response from ', this.options.url, ' -> ', e, ' : ', {resp, error});
                    proxies = 'INVAID_URL';
                }
                if (proxies === 'INVAID_URL' || proxies === 'UNKNOWN_ERROR') {
                    if (this.stored) {
                        const message = `${this.name} url ` +
                            `${proxies === 'INVAID_URL' ? 'is invalid' : 'encounters an error'} ` +
                            `and need to be changed, requesting proxies returns ` +
                            `${resp ? JSON.stringify(resp) : error}.`;
                        const store = await this.logger.pull({
                            waitUntil: s => {
                                const url = get(s, [this.name, 'options', 'url']);
                                return url && url !== this.options.url;
                            },
                            message
                        });
                        this._load(store);
                    } else {
                        if (proxies === 'INVAID_URL') {
                            logger.crash('_proxies_crash', 'Invalid url for refreshing proxy list ', this.options.url, ' -> ', resp || error);
                        } else {
                            logger.crash('_proxies_crash', 'Failed refreshing proxy list from ', this.options.url, ' -> ', resp || error);
                        }
                    }
                } else if (proxies === 'REQUEST_TOO_FREQUENT') {
                    const retryAfterSeconds = this.options.minIntervalBetweenRequest || 1;
                    this._info(logger, 'It seems refreshing proxies too frequently, will re-try after ', retryAfterSeconds, ' seconds: ', resp || error);
                    await sleep(retryAfterSeconds * 1000);
                } else if (proxies === 'JUST_RETRY') {
                    this._info(logger, 'It seems refreshing proxies has some problems, will re-try immediately: ', resp || error);
                } else {
                    if (proxies.length <= 0) {
                        this._warn(logger, 'No available proxies so far: ', resp || error);
                        return false;
                    } else {
                        for (const proxy of proxies) {
                            const id = this._id(proxy);
                            this.proxies[id] = {...this.proxies[id], ...proxy};
                        }
                        return true;
                    }
                }
            }
        }

        async get(logger, identity) {
            logger = logger || this.logger;
            this._purge(logger);
            let proxy = undefined, one = undefined;
            if (identity) {
                for (const p of this._iterProxies()) {
                    if (p.identity === identity) {
                        proxy = p;
                    }
                }
            }
            if (!proxy) {
                proxy = await this._get(logger, identity);
            }
            if (proxy) {
                one = this._str(proxy);
                this._info(logger, one, ' is being used', identity ? ` for identity ${identity}` : '', '.');
            }
            return one;
        }

        async _get(logger, identity) {
            let load = true, proxy = undefined;
            while (!proxy && load) {
                for (const p of this._iterProxies()) {
                    const {identityBlacklist} = p;
                    if (p.identity) continue;
                    if (identity && identityBlacklist && identityBlacklist.indexOf(identity) >= 0) continue;
                    if (p.lastTimeUsed > Date.now() - this.options.minIntervalBetweenUse * 1000) continue;
                    if (!proxy) {
                        proxy = p;
                    } else {
                        if (this.options.recentlyUsedFirst) {
                            if (p.lastTimeUsed > proxy.lastTimeUsed) {
                                proxy = p;
                            }
                        } else {
                            if (p.lastTimeUsed < proxy.lastTimeUsed) {
                                proxy = p;
                            }
                        }
                    }
                }
                if (!proxy) load = await this._request(logger);
            }
            if (proxy) {
                if (identity) {
                    proxy.identity = identity;
                    proxy.identityAssignedAt = new Date();
                }
                this.touch(logger, proxy);
                this._syncStore();
            }
            return proxy;
        }

        touch(_, one) {
            const proxy = this._find(one);
            if (!proxy) return;
            proxy.lastTimeUsed = new Date();
            this._syncStore();
        }

        deprecate(logger, one, {clearIdentity = true} = {}) {
            const proxy = this._find(one);
            if (!proxy) return;
            proxy.deprecated = (proxy.deprecated || 0) + 1;
            this._info(
                logger, this._str(proxy), ' is marked as deprecated (',
                proxy.deprecated, '/', this.options.maxDeprecationsBeforeRemoval, ').'
            );
            if (proxy.deprecated >= this.options.maxDeprecationsBeforeRemoval) {
                this.remove(logger, proxy);
            } else {
                if (clearIdentity && proxy.identity) {
                    this._clearIdentity(proxy);
                }
            }
            this._syncStore();
        }

        remove(logger, one) {
            const proxy = this._find(one);
            if (!proxy) return;
            this.proxies[this._id(proxy)] = null;
            this._info(logger, this._str(proxy), ' is removed: ', proxy);
            this._syncStore();
        }

        _find(one) {
            let id;
            if (typeof one === 'string') {
                const [ip, port] = one.split(':');
                if (port) {
                    id = this._id({ip, port});
                } else {
                    id = ip;
                }
            } else {
                id = this._id(one);
            }
            return this.proxies[id];
        }

        *_iterProxies(proxies) {
            for (const proxy of Object.values(proxies || this.proxies)) {
                if (!proxy) continue;
                yield proxy;
            }
        }

        _purge(logger) {
            Object.entries(this.proxies)
                .filter(([, p]) => !this._isValid(p))
                .forEach(([id]) => this.remove(logger, id));
            for (const proxy of this._iterProxies()) {
                if (this._isIdentityExpired(proxy)) {
                    this._clearIdentity(proxy);
                }
            }
        }

        _clearIdentity(proxy) {
            if (!proxy.identityBlacklist) proxy.identityBlacklist = [];
            proxy.identityBlacklist.push(proxy.identity);
            proxy.identity = undefined;
            proxy.identityAssignedAt = undefined;
        }

        async _syncStoreForce() {
            let deleteNullProxies = true;
            if (this.stored) {
                try {
                    let store;
                    if (isEmpty(this.proxies)) {
                        store = await this.logger.pull();
                    } else {
                        store = await this.logger.push({[this.name]: {proxies: this.proxies}});
                    }
                    this._load(store[this.name]);
                } catch (e) {
                    deleteNullProxies = false;
                    this._warn(undefined, 'Sync proxies of name ', this.name, ' failed: ', e);
                }
            }
            if (deleteNullProxies) {
                Object.entries(this.proxies)
                    .filter(([, p]) => !p)
                    .forEach(([id]) => delete this.proxies[id]);
            }
        }

        _syncStore() {
            this.__syncStore().catch(e => console.error('This should never happen: ', e));
        }

        _id(proxy) {
            return `${proxy.ip.split('.').join('_')}__${proxy.port}`;
        }

        _str(proxy) {
            return `${proxy.ip}:${proxy.port}`;
        }

        _isValid(proxy) {
            if (!proxy) return false;
            return !this._isExpired(proxy) && !this._isDeprecated(proxy);
        }

        _isExpired(proxy) {
            const {expire} = proxy;
            return expire < Date.now();
        }

        _isDeprecated(proxy) {
            const {deprecated} = proxy;
            return deprecated >= this.options.maxDeprecationsBeforeRemoval;
        }

        _isIdentityExpired(proxy) {
            const {identity, identityAssignedAt} = proxy;
            return identity && identityAssignedAt < Date.now() - this.options.proxiesWithIdentityTTL * 1000;
        }

        _makeOptions(options) {
            return {
                maxDeprecationsBeforeRemoval: 1, minIntervalBetweenUse: 5, recentlyUsedFirst: false,
                minIntervalBetweenRequest: 5, minIntervalBetweenStoreUpdate: 10,
                proxiesWithIdentityTTL: 24 * 60 * 60, requestTimeout: 10,
                ...options
            };
        }

        _info(logger, ...args) {
            (logger || this.logger).info(this.name ? `Proxies ${this.name}: ` : 'Proxies: ', ...args);
        }

        _warn(logger, ...args) {
            (logger || this.logger).warn(this.name ? `Proxies ${this.name}: ` : 'Proxies: ', ...args);
        }

    }

    return {
        key({name}) {
            return name;
        },
        async create({name, type, options, stored = false, dummy}) {
            if (dummy) {
                return new Dummy(dummy);
            } else {
                const proxies = new Proxies(this, name, type, options, stored);
                await proxies._init();
                return proxies;
            }
        },
        async destroy(proxies) {
            await proxies._syncStoreForce();
        }
    };
};