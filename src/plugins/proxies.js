const {get} = require('lodash');

const {sleep, dedup, limit, requestWithTimeout} = require('../utils');


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

    _load({type, options, proxies = {}}) {
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
                Proxies.prototype.__syncStore.bind(this),
                {within: this.options.minIntervalBetweenStoreUpdate * 1000}
            );
        }
        if (requestTimeout !== this.options.requestTimeout) {
            this.request = requestWithTimeout(this.options.requestTimeout * 1000);
        }
        if (type) this.type = type;
        if (!PROXY_TYPES[this.type]) {
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
            const reqOptions = {uri: this.options.url, ...PROXY_TYPES[this.type].requestOptions};
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
                proxies = await PROXY_TYPES[this.type].requestParser({resp, error});
            } catch (e) {
                this._warn(logger, 'Failed parsing proxy response from ', this.options.url, ' -> ', e, ' : ', {resp, error});
                proxies = ERROR_CODES.INVAID_URL;
            }
            if (proxies === ERROR_CODES.INVAID_URL || proxies === ERROR_CODES.UNKNOWN_ERROR) {
                if (this.stored) {
                    const message = `${this.name} url ` +
                        `${proxies === ERROR_CODES.INVAID_URL ? 'is invalid' : 'encounters an error'} ` +
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
                    if (proxies === ERROR_CODES.INVAID_URL) {
                        logger.crash('_proxies_crash', 'Invalid url for refreshing proxy list ', this.options.url, ' -> ', resp || error);
                    } else {
                        logger.crash('_proxies_crash', 'Failed refreshing proxy list from ', this.options.url, ' -> ', resp || error);
                    }
                }
            } else if (proxies === ERROR_CODES.REQUEST_TOO_FREQUENT) {
                const retryAfterSeconds = this.options.minIntervalBetweenRequest || 1;
                this._info(logger, 'It seems refreshing proxies too frequently, will re-try after ', retryAfterSeconds, ' seconds: ', resp || error);
                await sleep(retryAfterSeconds * 1000);
            } else if (proxies === ERROR_CODES.JUST_RETRY) {
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

    async __syncStore() {
        let deleteNullProxies = true;
        if (this.stored) {
            try {
                const store = await this.logger.push({[this.name]: {proxies: this.proxies}});
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


const ERROR_CODES = {
    INVAID_URL: 1,
    REQUEST_TOO_FREQUENT: 2,
    UNKNOWN_ERROR: 3,
    JUST_RETRY: 4,
};


const PROXY_TYPES = {
    proxyPool: {
        requestParser: async ({resp}) => {
            if (!resp) return ERROR_CODES.UNKNOWN_ERROR;
            if (resp === 'no proxy!') {
                return [];
            } else {
                const [ip, port] = resp.split(':');
                return [{ip, port}];
            }
        },
    },

    mogu: {
        /**
         * http://www.moguproxy.com/
         */
        requestParser: async ({resp}) => {
            if (!resp) return ERROR_CODES.UNKNOWN_ERROR;
            if (resp.code && resp.code === '0') {
                return resp.msg.map(p => ({...p, expire: new Date(Date.now() + 10 * 60 * 1000)}));
            } else {
                // http://www.moguproxy.com/help
                if (['3002', '3004', '3005', '3006'].indexOf(resp.code) >= 0) {
                    return ERROR_CODES.INVAID_URL;
                } else if (resp.code === '3001') {
                    return ERROR_CODES.REQUEST_TOO_FREQUENT;
                } else {
                    return ERROR_CODES.UNKNOWN_ERROR;
                }
            }
        },
        requestOptions: {json: true},
    },

    zhima: {
        /**
         * http://h.zhimaruanjian.com/getapi/#obtain_ip
         * 获取ip要求返回json、显示ip过期时间
         */
        requestParser: async ({resp}) => {
            if (!resp) return ERROR_CODES.UNKNOWN_ERROR;
            if (resp.success) {
                return resp.data.map(({ip, port, expire_time}) => ({ip, port, expire: new Date(expire_time)}));
                // return resp.data.map(({ip, port}) => ({ip, port}));
            } else {
                if (resp.code && resp.code === 111) {
                    return ERROR_CODES.REQUEST_TOO_FREQUENT;
                } else if (resp.code && [115, 121].indexOf(resp.code)) {
                    return ERROR_CODES.INVAID_URL;
                } else {
                    return ERROR_CODES.UNKNOWN_ERROR;
                }
            }
        },
        requestOptions: {json: true},
    },

    yiniuyun: {
        /**
         * http://16yun.cn
         * 获取ip要求返回json
         */
        requestParser: async ({resp, error}) => {
            if (error) {
                // https://www.16yun.cn/help/api_error/
                if (error.name === 'StatusCodeError') {
                    if (error.statusCode === 403) return ERROR_CODES.INVAID_URL;
                    if (error.statusCode === 406) return ERROR_CODES.INVAID_URL;
                    if (error.statusCode === 407) return ERROR_CODES.INVAID_URL;
                    if (error.statusCode === 408) return ERROR_CODES.REQUEST_TOO_FREQUENT;
                    if (error.statusCode === 429) return ERROR_CODES.REQUEST_TOO_FREQUENT;
                    if (error.statusCode === 504) return ERROR_CODES.REQUEST_TOO_FREQUENT;
                    return ERROR_CODES.UNKNOWN_ERROR;
                } else {
                    return ERROR_CODES.REQUEST_TOO_FREQUENT;
                }
            }
            if (resp && !resp.status) {
                if ((resp.error_msg || '').includes('链接已过期')) {
                    return ERROR_CODES.INVAID_URL;
                }
                if ((resp.error_msg || '').includes('速率超过限制')) {
                    return ERROR_CODES.REQUEST_TOO_FREQUENT;
                }
                return ERROR_CODES.UNKNOWN_ERROR;
            } else {
                return resp.proxy.map(p => ({expire: new Date(Date.now() + 30 * 60 * 1000), ...p}));
            }
        },
        requestOptions: {json: true},
    },

};


module.exports = {
    key({name}) {
        return name;
    },
    async create({name, type, options, stored = false}) {
        const proxies = new Proxies(this, name, type, options, stored);
        await proxies._init();
        return proxies;
    }
};