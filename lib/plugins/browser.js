const puppeteer = require('puppeteer-core');
const uuid4 = require('uuid/v4');

const {dedup, retry} = require('@raychee/utils');


class Browser {

    constructor(logger, pluginLoader, connectOpts = {}, {
        identities, proxies, hookedPageMethods = [
            'goto', 'evaluate', 'waitFor', 'waitForResponse', 'waitForNavigation',
            'evaluateClick', 'scrollToButtom'
        ],
        maxRetryIdentities = 10, maxRetryPageCrash = 1,
        switchIdentityOnInvalidProxy = false, switchProxyOnInvalidIdentity = true,
        createIdentityFn, loadIdentityFn, validateIdentityFn, validateProxyFn,
        loadPageStateFn, defaultIdentityId, lockIdentityUntilLoaded = false
    }) {
        this.logger = logger;
        this.pluginLoader = pluginLoader;
        this.browser = undefined;
        this.disconnected = true;

        this.connectOpts = connectOpts;
        this.identities = identities;
        this.proxies = proxies;
        this.hookedPageMethods = hookedPageMethods;
        this.maxRetryIdentities = maxRetryIdentities;
        this.maxRetryPageCrash = maxRetryPageCrash;
        this.switchIdentityOnInvalidProxy = switchIdentityOnInvalidProxy;
        this.switchProxyOnInvalidIdentity = switchProxyOnInvalidIdentity;
        this.createIdentityFn = createIdentityFn;
        if (loadIdentityFn) this.loadIdentityFn = loadIdentityFn;
        if (validateIdentityFn) this.validateIdentityFn = validateIdentityFn;
        if (validateProxyFn) this.validateProxyFn = validateProxyFn;
        this.loadPageStateFn = loadPageStateFn;
        this.defaultIdentityId = defaultIdentityId;
        this.lockIdentityUntilLoaded = lockIdentityUntilLoaded;

        this.currentIdentity = undefined;
        this.currentProxy = undefined;

        this._launch = dedup(Browser.prototype._launch.bind(this), {key: null});
        this._close = dedup(Browser.prototype._close.bind(this), {key: null});
        this._connect = retry(puppeteer.connect.bind(puppeteer), 10, {
            delay: 5, delayRandomize: 0.5, retryDelayFactor: 1.3, catch: (e) => this.logger.warn(e)
        });

    }

    async _newBrowser(override = {}) {
        return await this.pluginLoader.get({
            type: 'browser',
            connectOpts: this.connectOpts,
            identities: this.identities,
            proxies: this.proxies,
            hookedPageMethods: this.hookedPageMethods,
            maxRetryIdentities: this.maxRetryIdentities,
            switchIdentityOnInvalidProxy: this.switchIdentityOnInvalidProxy,
            switchProxyOnInvalidIdentity: this.switchProxyOnInvalidIdentity,
            createIdentityFn: this.createIdentityFn,
            loadIdentityFn: this.loadIdentityFn,
            validateIdentityFn: this.validateIdentityFn,
            validateProxyFn: this.validateProxyFn,
            loadPageStateFn: this.loadPageStateFn,
            defaultIdentityId: this.defaultIdentityId,
            lockIdentityUntilLoaded: this.lockIdentityUntilLoaded,
            ...override
        });
    }

    async launch(logger) {
        if (
            this.identities && !this.currentIdentity ||
            this.proxies ||
            !this.browser ||
            this.disconnected
        ) {
            await this._launch(logger);
        }
    }

    async _launch(logger) {
        logger = logger || this.logger;
        if (this.identities && !this.currentIdentity) {
            this.currentIdentity = await this.identities.get({
                lock: this.lockIdentityUntilLoaded,
                ifAbsent: this.createIdentityFn && (async () => {
                    const _id = uuid4();
                    const {instance, destroy} = await this._newBrowser(
                        {identities: undefined, defaultIdentityId: _id}
                    );
                    try {
                        const {id, ...data} = await this.createIdentityFn.call(logger, instance);
                        const identity = {id: id || _id, data};
                        logger.info('New identity for browser is created: ', identity.id, ' ', identity.data);
                        return identity;
                    } finally {
                        await destroy();
                    }
                }),
                waitForStore: !this.createIdentityFn
            });
        }
        if (this.proxies) {
            const identityId = this.currentIdentity && this.currentIdentity.id || this.defaultIdentityId;
            if (!this.currentProxy || identityId) {
                this.currentProxy = await this.proxies.get(identityId);
            }
        }

        if (!this.browser || this.disconnected) {
            // let {args = [], ...connectOpts} = this.connectOpts;
            // if (this.currentProxy) {
            //     args = [...args, `--proxy-server=${this.currentProxy}`];
            // }
            // // how to remotely debug a running chrome:
            // // 0. make sure to sleep somewhere in a desired state, so you can see it clearly later
            // // 1. establish ssh tunnel for non-localhost access: ssh -L 0.0.0.0:9223:localhost:9222 localhost -N
            // // 2. open a chrome on your laptop, type in url chrome://inspect
            // // 3. inspect this-machine-ip:9223 for remote debugging
            // args = [...args, '--remote-debugging-port=9222', '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--window-size=1280,1024'];
            // this.browser = await puppeteer.launch({args, ...connectOpts});

            let {args = [], blockAds = true, ignoreDefaultArgs, browserWSEndpoint, ...connectOpts} = this.connectOpts;
            if (this.currentProxy) {
                args = [...args, `--proxy-server=http://${this.currentProxy}`];
            }
            for (const arg of [...args, '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--no-audio', '--user-data-dir=~/browserless', '--window-size=1280,1024']) {
                browserWSEndpoint += `&${(encodeURIComponent(arg))}`;
            }
            if (blockAds) {
                browserWSEndpoint += '&blockAds';
            }
            if (ignoreDefaultArgs !== undefined) {
                browserWSEndpoint += '&ignoreDefaultArgs=true';
            }
            connectOpts.browserWSEndpoint = browserWSEndpoint;
            // console.log('connecting');
            this.browser = await this._connect(connectOpts);
            this.browser.on('disconnected', () => {
                // console.log('disconnected');
                this.disconnected = true;
            });
            this.disconnected = false;
            // console.log('connected');
        }
    }

    async newPage(logger, {incognito = true, filterRequests} = {}) {
        logger = logger || this.logger;
        let page = undefined, pageState = undefined, pageErrorCount = 0, pageError = undefined;

        const handlePageError = async (error) => {
            if (!this.browser || this.disconnected) {
                return;
            }
            pageError = error;
            pageErrorCount++;
            await this._close({closeBrowser: true});
            if (pageErrorCount <= this.maxRetryPageCrash) {
                logger.warn(
                    'Page seems to have crashed and will try reboot (',
                    pageErrorCount, '/', this.maxRetryPageCrash, '): ', error
                );
            } else {
                logger.warn('Page seems to have crashed too many times (',
                    this.maxRetryPageCrash, '/', this.maxRetryPageCrash, '): ', error
                );
            }
        };

        const makePage = dedup(async () => {

            if (pageState && this.loadPageStateFn) {
                const newPage = await this.newPage(logger, {incognito, filterRequests});
                await this.loadPageStateFn.call(logger, newPage, pageState);
                page = newPage._rawPage;
                return;
            }

            await this.launch(logger);
            let newPage;
            if (incognito) {
                const context = await this.browser.createIncognitoBrowserContext();
                newPage = await context.newPage();
            } else {
                newPage = await this.browser.newPage();
            }
            pageError = undefined;
            newPage.on('error', handlePageError);
            if (filterRequests) {
                await newPage.setRequestInterception(true);
                // if (traceRequests) {
                //     newPage.requests = new Requests();
                // }
                let filter = undefined;
                if (filterRequests) {
                    if (typeof filterRequests === 'string') {
                        filterRequests = new RegExp(filterRequests);
                    }
                    if (filterRequests instanceof RegExp) {
                        filter = (req) => filterRequests.test(req.url());
                    } else if (Array.isArray(filterRequests)) {
                        filter = (req) => req.isNavigationRequest() || filterRequests.indexOf(req.resourceType()) >= 0;
                    } else if (typeof filterRequests === "function") {
                        filter = filterRequests;
                    }
                }
                newPage.on('request', request => {
                    if (filter) {
                        if (filter(request)) {
                            request.continue();
                        } else {
                            request.abort('aborted');
                        }
                    } else {
                        request.continue();
                    }
                    // if (traceRequests) {
                    //     newPage.requests.push(request);
                    // }
                });
            }
            if (this.currentIdentity) {
                await this.loadIdentityFn.call(logger, newPage, this.currentIdentity.data);
                this.identities.unlock(this.currentIdentity);
            }

            page = newPage;

            page.updateState = (update) => {
                if (!pageState) pageState = {};
                Object.assign(pageState, update);
            };

            page.waitForResponseWhileGoto = async (urlsOrPredicates, url, {options, state} = {}) => {
                const results = await invokeAsyncMethods(
                    ...urlsOrPredicates.map(u => ({method: 'waitForResponse', args: [u, options]})),
                    {method: 'goto', args: [url, options]}
                );
                if (state) {
                    page.updateState(state);
                }
                return results;
            };

            page.waitForResponseWhileClick = async (urlsOrPredicates, selector, {options, state} = {}) => {
                const invokes = urlsOrPredicates.map(u => ({method: 'waitForResponse', args: [u, options]}));
                if (options && options.waitForNavigation) {
                    invokes.push({method: 'waitForNavigation', args: [options]});
                }
                invokes.push({method: 'evaluateClick', args: [selector, options]});
                const results = await invokeAsyncMethods(...invokes);
                if (state) {
                    page.updateState(state);
                }
                return results;
            };

            page.waitForNavigationWhileClick = async (selector, {options, state} = {}) => {
                return await page.waitForResponseWhileClick(
                    [], selector, {options: {waitForNavigation: true, ...options}, state}
                );
            };

            page.waitForResponseWhilePress = async (urlsOrPredicates, key, {options, state} = {}) => {
                const invokes = urlsOrPredicates.map(u => ({method: 'waitForResponse', args: [u, options]}));
                if (options && options.waitForNavigation) {
                    invokes.push({method: 'waitForNavigation', args: [options]});
                }
                invokes.push({method: 'keyboard.press', args: [key, options]});
                const results = await invokeAsyncMethods(...invokes);
                if (state) {
                    page.updateState(state);
                }
                return results;
            };

            page.waitForResponseWhileScrollToBottom = async (urlsOrPredicates, {options, state} = {}) => {
                const invokes = urlsOrPredicates.map(u => ({method: 'waitForResponse', args: [u, options]}));
                invokes.push({method: 'scrollToButtom', args: [options]});
                const results = await invokeAsyncMethods(...invokes);
                if (state) {
                    page.updateState(state);
                }
                return results;
            };

            page.evaluateClick = async (selector) => {
                return await page.evaluate((selector) => {
                    const elem = document.querySelector(selector);
                    elem.click();
                }, selector);
            };

            page.scrollToButtom = async (options) => {
                return await page.evaluate(async ({interval = 100, distance = 100} = {}) => {
                    await new Promise((resolve) => {
                        let totalHeight = 0;
                        const timer = setInterval(() => {
                            const scrollHeight = document.body.scrollHeight;
                            window.scrollBy(0, distance);
                            totalHeight += distance;
                            if(totalHeight >= scrollHeight){
                                clearInterval(timer);
                                resolve();
                            }
                        }, interval);
                    });
                }, options);
            };
        });

        const invokeAsyncMethods = async (...invokes) => {
            let trial = 0;
            while (true) {
                if (this.currentProxy) {
                    this.proxies.touch(this.currentProxy);
                }
                if (this.currentIdentity) {
                    this.identities.touch(this.currentIdentity);
                }
                if (!this.browser || this.disconnected || pageError) {
                    if (pageError && pageErrorCount > this.maxRetryPageCrash) {
                        logger.fail('_browser_page_crashed', pageError);
                    }
                    await makePage();
                }

                const promises = invokes.map(({method, args = []}) => {
                    let fn = page, this_ = page;
                    const fields = method.split('.');
                    for (const field of fields) {
                        this_ = fn;
                        fn = fn[field];
                    }
                    const logArgs = [];
                    for (const a of args) {
                        if (logArgs.length > 0) logArgs.push(', ');
                        logArgs.push(a);
                    }
                    // logger.info(method, '(', ...logArgs, ') ->');
                    return fn.call(this_, ...args).then(
                        (returned) => {
                            // logger.info(method, '(', ...logArgs, ') -> returned');
                            return {method, args, returned};
                        },
                        (error) => {
                            // logger.info(method, '(', ...logArgs, ') -> error');
                            return {method, args, error};
                        },
                    );
                });
                const results = await Promise.all(promises);
                trial++;

                let immediateRetry = false, proxyInvalid = false, identityInvalid = false,
                    returns = [], lastError = undefined;
                for (const result of results) {
                    const {method, args, returned, error} = result;
                    if (pageError || error && (error.message === 'Page crashed!' || error.message.startsWith('Protocol error'))) {
                        pageError = pageError || error;
                        await handlePageError(pageError);
                        if (pageErrorCount > this.maxRetryPageCrash) {
                            logger.fail('_browser_page_crashed', pageError);
                        } else {
                            immediateRetry = true;
                            break;
                        }
                    } else {
                        pageErrorCount = 0;
                    }
                    if (this.currentProxy) {
                        result.proxyInvalid = await this.validateProxyFn.call(logger, page, method, {
                            identities: this.identities,
                            identityId: this.currentIdentity && this.currentIdentity.id,
                            identity: this.currentIdentity && this.currentIdentity.data,
                            proxies: this.proxies,
                            proxy: this.currentProxy,
                            args, returned, error
                        });
                        proxyInvalid = result.proxyInvalid !== undefined;
                    }
                    if (this.currentIdentity) {
                        result.identityInvalid = await this.validateIdentityFn.call(logger, page, method, {
                            identities: this.identities,
                            identityId: this.currentIdentity.id,
                            identity: this.currentIdentity.data,
                            proxies: this.proxies,
                            proxy: this.currentProxy,
                            args, returned, error
                        });
                        identityInvalid = result.identityInvalid !== undefined;
                    }
                    returns.push(returned || error);
                    if (error) {
                        lastError = error;
                    }
                }
                if (immediateRetry) {
                    continue;
                }

                const logMessages = [];
                if (proxyInvalid || identityInvalid) {
                    for (const {method, args, proxyInvalid, identityInvalid} of results) {
                        if (!proxyInvalid && !identityInvalid) {
                            continue;
                        }
                        if (logMessages.length  > 0) {
                            logMessages.push('; ');
                        }
                        const logArgs = [];
                        for (const a of args) {
                            if (logArgs.length > 0) logArgs.push(', ');
                            logArgs.push(a);
                        }
                        logMessages.push(method, '(', ...logArgs, ') ->');
                        if (proxyInvalid) logMessages.push(
                            ' [Proxy Invalid] ',
                            ...(Array.isArray(proxyInvalid) ? proxyInvalid : [proxyInvalid])
                        );
                        if (identityInvalid) logMessages.push(
                            ' [Identity Invalid] ',
                            ...(Array.isArray(identityInvalid) ? identityInvalid : [identityInvalid])
                        );
                    }
                    if (trial <= this.maxRetryIdentities) {
                        logger.warn(
                            'Page operations failed with ',
                            this.currentProxy || 'no proxy', ' / ', this.currentIdentity && this.currentIdentity.id || 'no identity',
                            ', will rotate and re-try (',
                            trial, '/', this.maxRetryIdentities, '): ', ...logMessages
                        );
                    } else {
                        logger.warn(
                            'Page operations failed with ',
                            this.currentProxy || 'no proxy', ' / ', this.currentIdentity && this.currentIdentity.id || 'no identity',
                            ' and too many rotations have been tried (',
                            this.maxRetryIdentities, '/', this.maxRetryIdentities, '): ', ...logMessages
                        );
                    }

                    if (proxyInvalid && this.proxies) {
                        this.proxies.deprecate(this.currentProxy);
                        this.currentProxy = undefined;
                        if (this.switchIdentityOnInvalidProxy) this.currentIdentity = undefined;
                    }
                    if (identityInvalid && this.identities) {
                        this.identities.deprecate(this.currentIdentity);
                        this.currentIdentity = undefined;
                        if (this.switchProxyOnInvalidIdentity) this.currentProxy = undefined;
                    }

                    await this._close();
                    if (trial <= this.maxRetryIdentities) {
                        await makePage();
                        continue;
                    } else {
                        logger.fail('_browser_page_failed', ...logMessages);
                    }
                }
                if (lastError) {
                    logger.fail('_browser_page_failed', lastError);
                }
                if (this.currentIdentity) {
                    this.identities.renew(this.currentIdentity);
                }

                if (returns.length === 1) {
                    return returns[0];
                } else {
                    return returns;
                }
            }
        };

        const makeProxy = () => new Proxy({}, {
            get: (target, p, thisProxy) => {
                if (this.hookedPageMethods.indexOf(p) >= 0) {
                    return async (...args) => {
                        return await invokeAsyncMethods({method: p, args});
                    };
                } else if (p === '_rawPage') {
                    return page;
                } else if (p === 'loadState') {
                    return async (state) => {
                        const oldState = pageState;
                        pageState = undefined;
                        try {
                            const ret =  await this.loadPageStateFn.call(logger, thisProxy, state);
                            pageState = state;
                            return ret;
                        } catch (e) {
                            pageState = oldState;
                            throw e;
                        }
                    };
                } else {
                    return page[p];
                }
            }
        });

        await makePage();
        return makeProxy();
    }

    async _close({closeBrowser = true} = {}) {
        if (!this.browser || this.disconnected) return;
        const browser = this.browser;
        this.browser = undefined;
        this.disconnected = true;
        try {
            if (closeBrowser) {
                await browser.close();
            } else {
                await browser.disconnect();
            }
        } catch (e) {
            this.logger.warn('There is an error closing browser: ', e);
        }
    }

    /**
     * @param cookies An array of cookie objects, see https://pptr.dev/#?product=Puppeteer&version=v1.20.0&show=api-pagecookiesurls
     */
    async loadIdentityFn(page, {cookies}) {
        if (cookies) {
            await page.setCookie(...cookies);
        }
    }

    validateIdentityFn() {}

    validateProxyFn(page, method, {returned, error}) {
        if (error) return error;
        if (method === 'goto') {
            if (returned && !returned.ok()) return `${returned.status()} (${returned.statusText()})`;
        }
    }

}


module.exports = {
    async create(
        {
            connectOpts = {}, identities, proxies,
            hookedPageMethods = [
                'goto', 'evaluate', 'waitFor', 'waitForResponse', 'waitForNavigation',
                'evaluateClick', 'scrollToButtom'
            ],
            maxRetryIdentities = 10, maxRetryPageCrash = 1,
            switchIdentityOnInvalidProxy = false, switchProxyOnInvalidIdentity = true,
            createIdentityFn, validateIdentityFn, loadIdentityFn, validateProxyFn,
            loadPageStateFn, defaultIdentityId, lockIdentityUntilLoaded
        },
        {pluginLoader}
    ) {
        if (identities && typeof identities === "object" && identities.constructor === Object) {
            const plugin = await pluginLoader.get({type: 'identities', ...identities});
            identities = plugin.instance;
        }
        if (proxies && typeof proxies === "object" && proxies.constructor === Object) {
            const plugin = await pluginLoader.get({type: 'proxies', ...proxies});
            proxies = plugin.instance;
        }
        return new Browser(this, pluginLoader, connectOpts, {
            identities, proxies, hookedPageMethods,
            maxRetryIdentities, maxRetryPageCrash,
            switchIdentityOnInvalidProxy, switchProxyOnInvalidIdentity,
            createIdentityFn, loadIdentityFn, validateIdentityFn, validateProxyFn,
            loadPageStateFn, defaultIdentityId, lockIdentityUntilLoaded
        });
    },

    async destroy(browser) {
        await browser._close();
    }
};