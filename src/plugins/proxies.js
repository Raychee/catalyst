const {get} = require('lodash');

const {sleep, dedup, limit, requestWithTimeout} = require('../utils');


module.exports = function ({proxyTypes = {}} = {}) {

    class Proxies {

        constructor(logger, name, type, options, stored = false) {
            this.logger = logger;
            this.name = name;
            this.stored = stored;
            if (!stored) {
                this._load({type, config: options});
                if (!this.options.url) {
                    this.logger.crash('_proxies_crash', 'url must be specified');
                }
            }

            this.nextTimeRequest = 0;

            this._init = dedup(Proxies.prototype._init.bind(this));
            this._request = dedup(Proxies.prototype._request.bind(this), {key: null});
            this._get = limit(Proxies.prototype._get.bind(this), 1);
        }

        async _init() {
            if (this.stored) {
                const store = get(await this.logger.pull(), this.name);
                if (!store) {
                    this.logger.crash('_proxies_crash', 'invalid proxies name: ', this.name, ', please make sure: ',
                        '1. there is a document in the internal table service.Store that matches filter {plugin: \'proxies\'}, ',
                        '2. there is a valid identities config entry under document field \'data.', this.name, '\''
                    );
                }
                this._load(store);
            }
        }

        _load(store, {configOnly = false} = {}) {
            const minIntervalBetweenStoreUpdate = get(this.options, 'minIntervalBetweenStoreUpdate');
            const requestTimeout = get(this.options, 'requestTimeout');
            // const {
            //     url, maxDeprecationsBeforeRemoval = 1, minIntervalBetweenUse = 5, recentlyUsedFirst = false,
            //     minIntervalBetweenRequest = 5, minIntervalBetweenStoreUpdate = 10,
            //     useProxyToLoadProxies, proxiesWithIdentityTTL = 24 * 60 * 60, requestTimeout = 10,
            // } = config;
            const {type, config, proxies = []} = store || {};
            this.options = this._makeOptions(config);
            if (minIntervalBetweenStoreUpdate !== this.options.minIntervalBetweenStoreUpdate) {
                this._syncStore = dedup(
                    Proxies.prototype._syncStore.bind(this),
                    {within: this.options.minIntervalBetweenStoreUpdate * 1000}
                );
            }
            if (requestTimeout !== this.options.requestTimeout) {
                this.request = requestWithTimeout(this.options.requestTimeout * 1000);
            }
            if (!configOnly) {
                this.type = type;
                if (!proxyTypes[this.type]) {
                    this.logger.crash('_proxies_crash', 'unknown proxies type: ', this.type);
                }
                this.proxies = proxies;
            }
        }

        async _request(logger) {
            logger = logger || this.logger;
            if (!this.options.url) {
                const store = await this.logger.pull({
                    waitUntil: s => get(s, [this.name, 'config', 'url']),
                    message: `${this.name}.config.url need to be valid`
                });
                this.options.url = get(store, [this.name, 'config', 'url']);
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
                    proxies = 'INVALID_URL';
                }
                if (proxies === 'INVALID_URL' || proxies === 'UNKNOWN_ERROR') {
                    if (this.stored) {
                        const message = `${this.name} url ` +
                            `${proxies === 'INVALID_URL' ? 'is invalid' : 'encounters an error'} ` +
                            `and need to be changed, requesting proxies returns ` +
                            `${resp ? JSON.stringify(resp) : error}.`;
                        const store = await this.logger.pull({
                            waitUntil: s => {
                                const url = get(s, [this.name, 'config', 'url']);
                                return url && url !== this.options.url;
                            },
                            message
                        });
                        this.options.url = get(store, [this.name, 'config', 'url']);
                    } else {
                        if (proxies === 'INVALID_URL') {
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
                        this.proxies.push(...proxies);
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
                proxy = this.proxies.find(p => p.identity === identity);
            }
            if (!proxy) {
                proxy = await this._get(logger, identity);
            }
            if (proxy) {
                const {ip, port} = proxy;
                one = `${ip}:${port}`;
                this._info(logger, one, ' is being used', identity ? ` for identity ${identity}` : '', '.');
            }
            return one;
        }

        async _get(logger, identity) {
            let load = true, proxy = undefined;
            while (!proxy && load) {
                for (const p of this.proxies) {
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
            const proxy = this._ensureProxy(one);
            if (!proxy) return;
            proxy.lastTimeUsed = new Date();
            this._syncStore();
        }

        deprecate(logger, one, {clearIdentity = true} = {}) {
            const proxy = this._ensureProxy(one);
            if (!proxy) return;
            proxy.deprecated = (proxy.deprecated || 0) + 1;
            this._info(
                logger, `${proxy.ip}:${proxy.port}`,
                ' is marked as deprecated (', proxy.deprecated, '/', this.options.maxDeprecationsBeforeRemoval, ').'
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
            let index;
            if (typeof one === 'number') {
                index = one;
            } else {
                if (typeof one === 'object') {
                    one = `${one.ip}:${one.port}`;
                }
                index = this.proxies.findIndex(p => `${p.ip}:${p.port}` === one);
            }
            if (index >= 0) {
                const [removed] = this.proxies.splice(index, 1);
                this._info(logger, `${removed.ip}:${removed.port}`, ' is removed: ', removed);
                this._syncStore();
            }
        }

        _ensureProxy(one) {
            if (typeof one === 'string') {
                return this.proxies.find(p => `${p.ip}:${p.port}` === one);
            } else {
                return one;
            }
        }

        _purge(logger) {
            for (let i = this.proxies.length - 1; i >= 0; i--) {
                const p = this.proxies[i];
                if (!this._isValid(p)) {
                    this.remove(logger, i);
                } else if (this._isIdentityExpired(p)) {
                    this._clearIdentity(p);
                }
            }
        }

        _clearIdentity(proxy) {
            if (!proxy.identityBlacklist) proxy.identityBlacklist = [];
            proxy.identityBlacklist.push(proxy.identity);
            proxy.identity = undefined;
            proxy.identityAssignedAt = undefined;
        }

        async _syncStore() {
            if (this.stored) {
                try {
                    const store = await this.logger.push({[this.name]: {proxies: this.proxies}});
                    this._load(get(store, this.name), {configOnly: true});
                } catch (e) {
                    this._warn(undefined, 'Sync proxies of name ', this.name, ' failed: ', e);
                }
            }
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
        async create({name, type, options, stored = false}) {
            const proxies = new Proxies(this, name, type, options, stored);
            await proxies._init();
            return proxies;
        }
    }

};