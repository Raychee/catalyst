const fs = require('fs');
const path = require('path');
const {promisify} = require('util');

const {get} = require('lodash');
const {MongoClient} = require('mongodb');
const Koa = require('koa');
const {ApolloServer} = require('apollo-server-koa');
const {config, makeSchemaDirectives} = require('graphql-augment');

const {Logger} = require('./logger');
const {TaskLoader} = require('./loader');
const Operations = require('./operations');
const {SystemError} = require('./error');
const resolvers = require('./service/resolvers');


module.exports = class {
    constructor(options) {
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

        this.server = undefined;
        this.mongodb = undefined;

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;

        this.starting = undefined;
        this.stopping = undefined;
        this.exitHandler = undefined;
    }

    async start(options) {
        if (this.stopping) {
            await this.stopping;
            this.stopping = undefined;
        }
        if (!this.starting) {
            this.starting = this._start(options);
        }
        await this.starting;
        this.starting = undefined;
    }

    async stop(options) {
        if (this.starting) {
            await this.starting;
            this.starting = undefined;
        }
        if (!this.stopping) {
            this.stopping = this._stop(options);
        }
        await this.stopping;
        this.stopping = undefined;
    }

    async _start({verbose = true} = {}) {
        if (this.server) return;
        const logger = new Logger({level: verbose ? 'INFO' : 'ERROR'});
        logger.println(this.options.name, ' (server) is starting up.');

        if (this.exitHandler) {
            process.off('SIGTERM', this.exitHandler);
            process.off('SIGINT' , this.exitHandler);
            this.exitHandler = undefined;
        }
        this.exitHandler = this.stop.bind(this, {verbose, exit: true});
        process.on('SIGTERM', this.exitHandler);
        process.on('SIGINT' , this.exitHandler);

        const {host, port, user, password, db} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        this.mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, {useNewUrlParser: true, useUnifiedTopology: true}
        );

        logger.print('Connecting to database... ');
        await this.mongodb.connect();
        logger.reprintln('Database is connected.');

        const taskLoader = new TaskLoader(this.options.tasks.paths);
        await taskLoader.load({verbose});

        logger.print('Preparing database... ');
        const operations = new Operations(
            new Logger({category: 'Operations'}), this.mongodb.db(db), taskLoader
        );
        await operations.prepare();
        logger.reprintln('Database is prepared.');

        logger.print('Starting server... ');

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
            logger.println(ctx.ip, ' -> ', ctx.method, ' ', ctx.originalUrl, ' ', ctx.request.body);
            if (ctx.status >= 400 ||
                ctx.originalUrl === '/query' &&
                ctx.response.is('json') &&
                get(JSON.parse(ctx.body), ['errors', '0'])) {
                logger.println(ctx.ip, ' <- ', ctx.method, ' ', ctx.originalUrl, ' [', ctx.status, '] ', ctx.body);
            } else {
                logger.println(ctx.ip, ' <- ', ctx.method, ' ', ctx.originalUrl, ' [', ctx.status, ']');
            }
        });

        apollo.applyMiddleware({app, path: '/query'});

        this.server = app.listen(4000, '0.0.0.0', () => {
            logger.reprintln(this.options.name, ' (server) is ready at http://0.0.0.0:4000', apollo.graphqlPath);
        });
        this.server.close = promisify(this.server.close.bind(this.server));
    }

    async _stop({verbose = true, exit = false} = {}) {
        const logger = new Logger({level: verbose ? 'INFO' : 'ERROR'});
        const server = this.server;
        const mongodb = this.mongodb;
        this.server = undefined;
        this.mongodb = undefined;
        logger.println(this.options.name, ' (server) is shutting down.');
        logger.print('Stopping service...');
        await server.close();
        logger.reprintln('Service is stopped.');
        logger.print('Closing connections to database... ');
        await mongodb.close();
        logger.reprintln('Database connections are closed.');
        logger.println(this.options.name, ' (server) is stopped.');

        if (exit) {
            logger.println(this.options.name, ' (server) exits.');
            process.exit(0);
        }
    }

};
