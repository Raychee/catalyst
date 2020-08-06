const fs = require('fs');
const path = require('path');
const {promisify} = require('util');

const {mapValues} = require('lodash');
const {MongoClient} = require('mongodb');
const Koa = require('koa');
const cors = require('@koa/cors');
const bodyParser = require('koa-bodyparser');
const compress = require('koa-compress');
const {ApolloServer} = require('apollo-server-koa');
const {makeSchemaDirectives} = require('graphql-augment');
const {Runnable, randomString} = require('@raychee/utils');

const {DEFAULT_HEARTBEAT} = require('./config');
const {Logger} = require('./logger');
const {TaskLoader} = require('./loader');
const Operations = require('./operations');
const {SystemError} = require('./error');
const {makeResolvers} = require('./service/resolvers');
const {JobWatcherManager} = require('./watcher');


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
        this.options.db.options = {
            useNewUrlParser: true, useUnifiedTopology: true,
            ...this.options.db.options
        };
        if (!this.options.db.db) throw new SystemError('db.db must be provided');
        this.options.tasks = this.options.tasks || {};
        this.options.tasks.paths = this.options.tasks.paths || [];
        this.options.daemon = this.options.daemon || {};
        this.options.daemon.heartbeat = this.options.daemon.heartbeat || DEFAULT_HEARTBEAT;
        this.options.server = this.options.server || {};
        this.options.server.auth = this.options.server.auth || (() => {
        });
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
        process.once('SIGTERM', exitHandler);
        process.once('SIGINT', exitHandler);

        const {host, port, user, password, db} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        const mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, this.options.db.options
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
        const app = new Koa();

        app.use(cors());
        app.use(compress());
        app.use(bodyParser());

        function truncate(str) {
            if (typeof str !== 'string') str = JSON.stringify(str);
            return str.length > 1500 ? `${str.slice(0, 700)} ... ${str.slice(-700)}` : str;
        }

        app.use(async (ctx, next) => {
            const id = randomString(5);
            console.log(`(${id}) ${ctx.ips.join(' -> ')} -> ${ctx.method} ${ctx.originalUrl} ${JSON.stringify(mapValues(ctx.request.headers, truncate))} ${truncate(ctx.request.body)}`);
            const tick = Date.now();
            await next();
            const time = Date.now() - tick;
            console.log(`(${id}) ${ctx.ips.join(' <- ')} <-${time}ms- ${ctx.method} ${ctx.originalUrl} [${ctx.status}] ${truncate(ctx.body)}`);
        });

        const typeDefs = fs.readFileSync(path.join(__dirname, 'service', 'schema.graphql'))
            .toString('utf-8')
            .replace('${secret}', this.options.server.secret);
        const jobWatcherManager = new JobWatcherManager(
            operations.jobs, {pollInterval: this.options.daemon.heartbeat}
        );
        const apollo = new ApolloServer({
            typeDefs, resolvers: makeResolvers({auth: this.options.server.auth}),
            context: ({ctx}) => ({req: ctx.req, operations, jobWatcherManager}),
            schemaDirectives: makeSchemaDirectives(),
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
        jobWatcherManager.closeAll();
        jobWatcherManager.abortAll(new Error(`${this.options.name} (server) shutting down`));
        await mongodb.close();
        this.logger.reprintln('Database connections are closed.');

        this.logger.println(this.options.name, ' (server) is stopped.');

        if (exit) {
            this.logger.println(this.options.name, ' (server) exits.');
            process.exit(0);
        }
    }

};
