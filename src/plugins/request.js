const request = require('request-promise-native');
const uuid4 = require('uuid/v4');
const {memoize, cloneDeep} = require('lodash');

const {dedup, requestWithTimeout} = require('@raychee/utils');


/**
 * @param cookies A whole string from an http "Cookies" header
 */
function defaultLoadIdentityFn(options, {cookies, userAgent}) {
    if (cookies) {
        let {jar} = options;
        if (jar) {
            if (typeof cookies === 'string') {
                cookies = cookies.split('; ');
            } else {
                cookies = cookies.map(cookie => {
                    if (typeof cookie === 'string') return cookie;
                    else return `${cookie.name}=${cookie.value}`;
                });
            }
            for (let cookie of cookies) {
                if (!cookie) continue;
                cookie = request.cookie(cookie);
                jar.setCookieSync(cookie, options.uri || options.url);
            }
        } else {
            const headers = options.headers || {};
            options.headers = headers;
            if (typeof cookies !== 'string') {
                cookies = cookies
                    .map(cookie => {
                        if (typeof cookie === 'string') return cookie;
                        else return `${cookie.name}=${cookie.value}`;
                    })
                    .join('; ');
            }
            headers['cookie'] = cookies;
        }
    }
    if (userAgent) {
        const headers = options.headers || {};
        options.headers = headers;
        headers['User-Agent'] = userAgent;
    }
}

function defaultValidateIdentityFn() {
}

function defaultValidateProxyFn(options, {response, error}) {
    if (error) return error;
    if (response && (response.statusCode < 200 || response.statusCode >= 400)) return response;
}


module.exports = {
    key({defaults, smartError = true, timeout = 0, identities, proxies} = {}) {
        if (!identities && !proxies) {
            return {defaults, smartError, timeout};
        }
    },
    async create(
        {
            defaults, smartError = true, timeout = 0, identities, proxies,
            maxRetryIdentities = 10, switchIdentityOnInvalidProxy = false, switchProxyOnInvalidIdentity = true,
            createIdentityFn, validateIdentityFn, loadIdentityFn, validateProxyFn,
            defaultIdentityId, loadIdentityError, lockIdentityUntilLoaded = false,
        } = {},
        {pluginLoader}
    ) {

        let _req = request, globalCookieJar = request.jar();
        let req = (logger, options) => {
            if (typeof options.jar === 'boolean' && options.jar) {
                options = {...options, jar: globalCookieJar};
            }
            return _req(options);
        };
        let extra = {};

        defaults = timeout > 0 ? {timeout: timeout * 1000, ...defaults} : defaults;
        if (defaults) {
            if (typeof defaults.jar === 'boolean' && defaults.jar) {
                defaults = {...defaults, jar: globalCookieJar};
            }
            _req = _req.defaults(defaults);
        }

        if (timeout > 0) {
            _req = requestWithTimeout(timeout * 1000, _req);
        }

        if (smartError) {
            req = function (req, logger, options) {
                logger = logger || this;
                try {
                    return req(logger, options);
                } catch (e) {
                    if (e.statusCode) {
                        if (e.statusCode >= 400 && e.statusCode < 500) {
                            logger.crash('_request_status_4xx', e);
                        } else {
                            logger.fail('_request_status_5xx', e);
                        }
                    }
                    if (e.cause) {
                        if (e.cause.code === 'ETIMEDOUT') {
                            logger.fail('_request_timeout', e);
                        } else if (e.cause.code === 'ECONNREFUSED') {
                            logger.fail('_request_connection_refused', e);
                        } else if (e.cause.code === 'ECONNRESET') {
                            logger.fail('_request_connection_reset', e);
                        }
                    }
                    throw e;
                }
            }.bind(this, req);
        }

        if (identities || proxies) {
            if (identities && typeof identities === "object" && identities.constructor === Object) {
                const plugin = await pluginLoader.get({type: 'identities', ...identities});
                identities = plugin.instance;
            }
            if (proxies && typeof proxies === "object" && proxies.constructor === Object) {
                const plugin = await pluginLoader.get({type: 'proxies', ...proxies});
                proxies = plugin.instance;
            }
            if (identities) extra.identities = identities;
            if (proxies) extra.proxies = proxies;

            let identity = undefined, proxy = undefined;
            extra.currentIdentity = () => identity;
            extra.currentProxy = () => proxy;
            if (!loadIdentityFn) loadIdentityFn = defaultLoadIdentityFn;
            if (!validateIdentityFn) validateIdentityFn = defaultValidateIdentityFn;
            if (!validateProxyFn) validateProxyFn = defaultValidateProxyFn;

            const getReqWithoutIdentities = memoize(defaultIdentityId => {
                let _req = undefined;
                return async (...args) => {
                    if (!_req) {
                        const plugin = await pluginLoader.get({
                            type: 'request',
                            defaults, smartError, proxies,
                            maxRetryIdentities, switchIdentityOnInvalidProxy, switchProxyOnInvalidIdentity,
                            createIdentityFn, validateIdentityFn, loadIdentityFn, validateProxyFn,
                            defaultIdentityId, loadIdentityError,
                            lockIdentityUntilLoaded
                        });
                        _req = plugin.instance;
                    }
                    return await _req(...args);
                };
            });

            const launch = dedup(async function (logger) {
                if (identities && !identity) {
                    identity = await identities.get({
                        lock: lockIdentityUntilLoaded,
                        ifAbsent: createIdentityFn && (async () => {
                            const _id = uuid4();
                            const {id, ...data} = await createIdentityFn.call(logger, getReqWithoutIdentities(_id));
                            const identity = {id: id || _id, data};
                            logger.info('New identity for request is created: ', identity.id, ' ', identity.data);
                            return identity;
                        }),
                        waitForStore: !createIdentityFn
                    });
                }
                if (proxies) {
                    const identityId = identity && identity.id || defaultIdentityId;
                    if (!proxy || identityId) {
                        proxy = await proxies.get(identityId);
                    }
                }
            }, {key: null});

            req = async function (req, logger, _options) {
                logger = logger || this;
                if (typeof _options.jar === 'boolean' && _options.jar) {
                    _options = {..._options, jar: globalCookieJar};
                }
                let trial = 0, options = _options;
                while (true) {
                    trial++;
                    if (identities && !identity || proxies && !proxy) {
                        await launch(logger);
                    }
                    if (identity || proxy) {
                        options = cloneDeep(_options);
                    }
                    if (identity) {
                        try {
                            await loadIdentityFn.call(
                                logger, options, identity.data,
                                {request: getReqWithoutIdentities(identity.id)}
                            );
                            if (proxies) {
                                proxy = await proxies.get(identity.id);
                            }
                            identities.touch(identity);
                            identities.unlock(identity);
                        } catch (e) {
                            identities.unlock(identity);
                            if (loadIdentityError) {
                                const message = await loadIdentityError.call(logger, e, options, identity.data);
                                if (message) {
                                    const logMessages = Array.isArray(message) ? message : [message];
                                    logger.warn(
                                        'Loading identity failed with ',
                                        proxy || 'no proxy', ' / ', identity.id || 'no identity',
                                        ' during request trial ', trial, '/', maxRetryIdentities, ': ', ...logMessages
                                    );
                                    if (identities) identities.deprecate(identity);
                                    identity = undefined;
                                    if (switchProxyOnInvalidIdentity) {
                                        proxy = undefined;
                                    }
                                    if (trial <= maxRetryIdentities) {
                                        continue;
                                    } else {
                                        logger.fail('_request_load_identity_failed', ...logMessages);
                                    }
                                }
                            }
                            throw e;
                        }
                    }
                    if (proxy) {
                        options.proxy = `http://${proxy}`;
                        proxies.touch(proxy);
                    }
                    let response, error, proxyInvalidMessage, identityInvalidMessage;
                    try {
                        response = await req(logger, options);
                        // logger.info('request(', options, ') ', proxy || 'no proxy', ' / ', identity && identity.id || 'no identity', ' -> resp');
                    } catch (e) {
                        error = e;
                        response = error.response;
                        let resolveWithFullResponse = options.resolveWithFullResponse || defaults && defaults.resolveWithFullResponse;
                        if (!resolveWithFullResponse) {
                            response = response && response.body;
                        }
                        // logger.info('request(', options, ') -> error');
                    }
                    if (proxy) {
                        proxyInvalidMessage = await validateProxyFn.call(
                            logger, options, {identities, identityId: (identity || {}).id, identity: (identity || {}).data, proxies, proxy, response, error}
                        );
                    }
                    if (identity) {
                        identityInvalidMessage = await validateIdentityFn.call(
                            logger, options, {identities, identityId: identity.id, identity: identity.data, proxies, proxy, response, error}
                        );
                    }

                    if (proxyInvalidMessage || identityInvalidMessage) {
                        const logMessages = ['request(', options, ') ->'];
                        if (proxyInvalidMessage) logMessages.push(
                            ' [Proxy Invalid] ',
                            ...(Array.isArray(proxyInvalidMessage) ? proxyInvalidMessage : [proxyInvalidMessage])
                        );
                        if (identityInvalidMessage) logMessages.push(
                            ' [Identity Invalid] ',
                            ...(Array.isArray(identityInvalidMessage) ? identityInvalidMessage : [identityInvalidMessage])
                        );
                        if (trial <= maxRetryIdentities) {
                            logger.warn(
                                'Request failed with ', proxy || 'no proxy', ' / ', identity && identity.id || 'no identity',
                                ', will rotate and re-try (', trial, '/', maxRetryIdentities, '): ', ...logMessages
                            );
                        } else {
                            logger.warn(
                                'Request failed with ', proxy || 'no proxy', ' / ', identity && identity.id || 'no identity',
                                ' and too many rotations have been tried (',
                                maxRetryIdentities, '/', maxRetryIdentities, '): ', ...logMessages
                            );
                        }

                        if (proxyInvalidMessage && proxies) {
                            proxies.deprecate(proxy);
                            proxy = undefined;
                            if (switchIdentityOnInvalidProxy) identity = undefined;
                        }
                        if (identityInvalidMessage && identities) {
                            identities.deprecate(identity);
                            identity = undefined;
                            if (switchProxyOnInvalidIdentity) proxy = undefined;
                        }

                        if (trial <= maxRetryIdentities) {
                            continue;
                        } else {
                            logger.fail('_request_failed', ...logMessages);
                        }
                    } else if (error) {
                        throw error;
                    }
                    if (identity) {
                        identities.renew(identity);
                    }
                    return response;
                }
            }.bind(this, req);
        }

        return Object.assign(req, extra);

    }
};
