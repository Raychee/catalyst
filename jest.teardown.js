module.exports = async function () {
    
    await global.__MONGOD_MEMORY__.stop();
    await global.__MONGOD_DURABLE__.stop();
    
};
