const fs = require('fs');
const path = require('path');
const {promisify} = require('util');

const {get} = require('lodash');
const {MongoClient} = require('mongodb');
const Koa = require('koa');
const {ApolloServer} = require('apollo-server-koa');
const {config, makeSchemaDirectives} = require('graphql-augment');
const {Runnable} = require('@raychee/utils');

const {Logger} = require('./logger');
const {TaskLoader} = require('./loader');
const Operations = require('./operations');
const {SystemError} = require('./error');
const resolvers = require('./service/resolvers');


module.exports = class extends Runnable {
    constructor(options) {
        super();

        this.options = options || {};
        this.options.name = this.options.name || 'Catalyst';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new Error('db.host must be provided');
        this.options.db.port = this.options.db.port || 27017;
        this.options.db.user = this.options.db.user || '';
        this.options.db.password = this.options.db.password || '';
        if (!this.options.db.db) throw new SystemError('db.db must be provided');
        this.options.tasks = this.options.tasks || {};
        this.options.tasks.paths = this.options.tasks.paths || [];
        this.options.server = this.options.server || {};
        this.options.server.secret = this.options.server.secret || '';
        this.options.logging = this.options.logging || {};
        this.options.logging.level = this.options.logging.level || 'INFO';
        this.options.logging.showTimestamp = this.options.logging.showTimestamp || false;

        this.logger = new Logger();

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async run({signal}) {
        this.logger.println(this.options.name, ' (server) is starting up.');

        const exitHandler = this.stop.bind(this, {exit: true});
        process.on('SIGTERM', exitHandler);
        process.on('SIGINT', exitHandler);

        const {host, port, user, password, db} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        const mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, {useNewUrlParser: true, useUnifiedTopology: true}
        );

        this.logger.print('Connecting to database... ');
        await mongodb.connect();
        this.logger.reprintln('Database is connected.');

        this.logger.print('Loading task domains and types... ');
        const taskLoader = new TaskLoader(this.options.tasks.paths);
        await taskLoader.load();
        this.logger.reprintln('Task domains and types are loaded.');

        this.logger.print('Preparing database... ');
        const operations = new Operations(
            new Logger({category: 'Operations'}), mongodb.db(db), taskLoader
        );
        await operations.prepare();
        this.logger.reprintln('Database is prepared.');

        this.logger.print('Starting server... ');

        const typeDefs = fs.readFileSync(path.join(__dirname, 'service', 'schema.graphql'))
            .toString('utf-8')
            .replace('${secret}', this.options.server.secret);
        config.FIELD_PREFIX_UPDATE = 'Undefined';
        config.FIELD_PREFIX_UPSERT = 'Update';
        const apollo = new ApolloServer({
            typeDefs, resolvers,
            context: ({ctx}) => ({req: ctx.req, operations}),
            schemaDirectives: makeSchemaDirectives(),
        });

        const app = new Koa();
        app.use(async (ctx, next) => {
            await next();
            this.logger.println(ctx.ip, ' -> ', ctx.method, ' ', ctx.originalUrl, ' ', ctx.request.body);
            if (ctx.status >= 400 ||
                ctx.originalUrl === '/query' &&
                ctx.response.is('json') &&
                get(JSON.parse(ctx.body), ['errors', '0'])) {
                this.logger.println(ctx.ip, ' <- ', ctx.method, ' ', ctx.originalUrl, ' [', ctx.status, '] ', ctx.body);
            } else {
                this.logger.println(ctx.ip, ' <- ', ctx.method, ' ', ctx.originalUrl, ' [', ctx.status, ']');
            }
        });

        apollo.applyMiddleware({app, path: '/query'});

        const server = app.listen(4000, '0.0.0.0', () => {
            this.logger.reprintln(this.options.name, ' (server) is ready at http://0.0.0.0:4000', apollo.graphqlPath);
        });
        server.close = promisify(server.close.bind(server));

        const {exit} = await signal;

        this.logger.println(this.options.name, ' (server) is shutting down.');

        this.logger.print('Stopping service...');
        await server.close();
        this.logger.reprintln('Service is stopped.');

        this.logger.print('Closing connections to database... ');
        await mongodb.close();
        this.logger.reprintln('Database connections are closed.');

        process.off('SIGTERM', exitHandler);
        process.off('SIGINT', exitHandler);

        this.logger.println(this.options.name, ' (server) is stopped.');

        if (exit) {
            this.logger.println(this.options.name, ' (server) exits.');
            process.exit(0);
        }
    }

};
