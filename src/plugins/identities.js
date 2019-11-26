const {get} = require('lodash');
const uuid4 = require('uuid/v4');

const {dedup} = require('../utils');


class Identities {

    constructor(logger, name, options, stored = false) {
        this.logger = logger;
        this.name = name;
        this.stored = stored;
        if (!stored) {
            this._load({config: options});
        }

        this._init = dedup(Identities.prototype._init.bind(this));
        this._get = dedup(Identities.prototype._get.bind(this), {key: null});
    }

    async _init() {
        if (this.stored) {
            const store = get(await this.logger.pull(), this.name);
            if (!store) {
                this.logger.crash(
                    '_identities_crash', 'invalid identities name: ', this.name, ', please make sure: ',
                    '1. there is a document in the internal table service.Store that matches filter {plugin: \'identities\'}, ',
                    '2. there is a valid identities config entry under document field \'data.', this.name, '\''
                );
            }
            this._load(store);
        }
    }

    _load(store, {configOnly = false} = {}) {
        const minIntervalBetweenStoreUpdate = get(this.options, 'minIntervalBetweenStoreUpdate');
        // {
        //     createIdentityFn,
        //     maxDeprecationsBeforeRemoval = 3, minIntervalBetweenUse = 5, recentlyUsedFirst = false,
        //     minIntervalBetweenStoreUpdate = 10, lockExpire = 10 * 60,
        // } = options;
        const {config, identities = []} = store;
        this.options = this._makeOptions(config);
        if (minIntervalBetweenStoreUpdate !== this.options.minIntervalBetweenStoreUpdate) {
            this._syncStore = dedup(
                Identities.prototype._syncStore.bind(this),
                {within: this.options.minIntervalBetweenStoreUpdate * 1000}
            );
        }
        if (!configOnly) {
            this.identities = identities;
            for (const identity of identities) {
                identity.locked = undefined;
            }
        }
    }

    add(_, {id = uuid4(), data, deprecated = 0, lastTimeUsed = new Date(0), locked = undefined}) {
        const identity = {id, data, deprecated, lastTimeUsed, locked};
        this.identities.push(identity);
        this._syncStore();
        return identity;
    }

    async get(logger, {ifAbsent = undefined, waitForStore = false, lock = false} = {}) {
        logger = logger || this.logger;
        while (true) {
            let identity = undefined;
            for (const i of this.identities) {
                if (!this._isAvailable(i)) continue;
                if (!identity) {
                    identity = i;
                } else {
                    if (this.options.recentlyUsedFirst) {
                        if (i.lastTimeUsed > identity.lastTimeUsed) {
                            identity = i;
                        }
                    } else {
                        if (i.lastTimeUsed < identity.lastTimeUsed) {
                            identity = i;
                        }
                    }
                }
            }
            if (identity) {
                this.touch(logger, identity);
                this._info(logger, identity.id, ' is being used.');
                if (lock) {
                    this.lock(logger, identity);
                }
                return identity;
            }
            await this._get(logger, ifAbsent || this.options.createIdentityFn, waitForStore);
        }
    }

    async _get(logger, createIdentityFn, waitForStore) {
        let identity = undefined;
        if (createIdentityFn) {
            identity = await createIdentityFn();
            if (identity) {
                identity = this.add(logger, identity);
            }
        }
        if (!identity && waitForStore && this.stored) {
            const store = await this.logger.pull({
                waitUntil: s => get(s, [this.name, 'identities'], []).some(i => this._isAvailable(i)),
                message: `waiting for a valid identity in store field ${this.name}`
            });
            this.identities = store[this.name].identities;
        }
    }

    lock(logger, one) {
        const {identity} = this._find(one);
        if (!identity) return;
        this._info(logger, identity.id, ' is locked.');
        identity.locked = new Date();
    }

    unlock(logger, one) {
        const {identity} = this._find(one);
        if (!identity) return;
        if (identity.locked) {
            this._info(logger, identity.id, ' is unlocked.');
            identity.locked = undefined;
            this.touch(logger, identity);
        }
    }

    touch(_, one) {
        const {identity} = this._find(one);
        if (!identity) return;
        identity.lastTimeUsed = new Date();
        this._syncStore();
    }

    update(_, id, data) {
        const {identity} = this._find(id);
        if (!identity) return;
        identity.data = data;
        this._syncStore();
    }

    renew(logger, one) {
        const {identity} = this._find(one);
        if (!identity) return;
        if (identity.deprecated > 0) {
            this._info(logger, identity.id, ' is renewed.');
            identity.deprecated = 0;
            this._syncStore();
        }
    }

    deprecate(logger, one) {
        const {identity} = this._find(one);
        if (!identity) return;
        identity.deprecated = (identity.deprecated || 0) + 1;
        this._info(logger,
            identity.id, ' is deprecated (', identity.deprecated, '/',
            this.options.maxDeprecationsBeforeRemoval, ').'
        );
        if (identity.deprecated >= this.options.maxDeprecationsBeforeRemoval) {
            this.remove(logger, identity);
        }
        this.unlock(logger, identity);
        this._syncStore();
    }

    remove(logger, one) {
        const {index} = this._find(one);
        if (index >= 0) {
            const [removed] = this.identities.splice(index, 1);
            this._info(logger, removed.id, ' is removed: ', removed);
            this._syncStore();
        }
    }

    _find(identity) {
        let index = -1;
        if (typeof identity === "string") {
            index = this.identities.findIndex(i => i.id === identity);
        } else {
            index = this.identities.indexOf(identity);
            if (index < 0) {
                index = this.identities.findIndex(i => i.id === identity.id);
            }
        }
        identity = index >= 0 ? this.identities[index] : undefined;
        return {identity, index};
    }

    async _syncStore() {
        if (this.stored) {
            try {
                const store = await this.logger.push({[this.name]: {identities: this.identities}});
                this._load(get(store, this.name), {configOnly: true});
            } catch (e) {
                this._warn(undefined, 'Sync identities of name ', this.name, ' failed: ', e);
            }
        }
    }

    _isAvailable(identity) {
        const now = Date.now();
        return (!identity.locked || identity.locked < now - this.options.lockExpire * 1000) &&
            identity.lastTimeUsed <= now - this.options.minIntervalBetweenUse * 1000;
    }

    _makeOptions(options) {
        return {
            maxDeprecationsBeforeRemoval: 3, minIntervalBetweenUse: 5, minIntervalBetweenStoreUpdate: 10,
            recentlyUsedFirst: false, lockExpire: 10 * 60,
            ...options
        };
    }

    _info(logger, ...args) {
        (logger || this.logger).info(this.name ? `Identities ${this.name}: ` : 'Identities: ', ...args);
    }

    _warn(logger, ...args) {
        (logger || this.logger).warn(this.name ? `Identities ${this.name}: ` : 'Identities: ', ...args);
    }

}


module.exports = {
    key({name}) {
        return name;
    },
    async create({name, options, stored = false}) {
        const identities = new Identities(this, name, options, stored);
        await identities._init();
        return identities;
    }
};