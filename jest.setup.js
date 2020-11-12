const {MongoMemoryServer, MongoMemoryReplSet} = require('mongodb-memory-server-global-4.2');


const mongodMemory = new MongoMemoryServer({
    instance: {port: 27017},
});

const mongodDurable = new MongoMemoryReplSet({
    instance: {port: 27027},
    replSet: {storageEngine: 'wiredTiger'},
});

module.exports = async function () {
    await mongodMemory.start();
    await mongodDurable.waitUntilRunning();
    global.__MONGOD_MEMORY__ = mongodMemory;
    global.__MONGOD_DURABLE__ = mongodDurable;
    process.env.MONGO_URL = await mongodMemory.getUri();
    process.env.MONGO_URL_DURABLE = await mongodDurable.getUri();
};
