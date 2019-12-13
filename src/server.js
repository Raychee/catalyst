const fs = require('fs');
const path = require('path');
const {promisify} = require('util');

const {get} = require('lodash');
const {MongoClient} = require('mongodb');
const Koa = require('koa');
const {ApolloServer} = require('apollo-server-koa');
const {config, makeSchemaDirectives} = require('graphql-augment');

const Agenda = require('agenda');

const {Logger} = require('./logger');
const {TaskLoader} = require('./loader');
const Operations = require('./operations');
const resolvers = require('./service/resolvers');


config.FIELD_PREFIX_UPDATE = 'Undefined';
config.FIELD_PREFIX_UPSERT = 'Update';


module.exports = class {
    constructor(options) {
        this.options = options || {};
        this.options.name = this.options.name || 'Catalyst';
        this.options.db = options.db || {};
        if (!this.options.db.host) throw new Error('db.host must be provided');
        this.options.db.port = this.options.db.port || 27017;
        this.options.db.user = this.options.db.user || '';
        this.options.db.password = this.options.db.password || '';
        this.options.db.agenda = this.options.db.agenda || 'agenda';
        this.options.db.service = this.options.db.service || 'service';
        this.options.db.collections = this.options.db.collections || {};
        this.options.db.collections.store = this.options.db.collections.store || 'Store';
        this.options.tasks = this.options.tasks || {};
        this.options.tasks.paths = this.options.tasks.paths || [];
        this.options.api = this.options.api || {};
        this.options.api.secret = this.options.api.secret || '';
        this.options.logging = this.options.logging || {};
        this.options.logging.level = this.options.logging.level || 'INFO';
        this.options.logging.showTimestamp = this.options.logging.showTimestamp || false;

        this.server = undefined;
        this.mongodb = undefined;

        Logger.prototype.LOGGING_LEVEL = this.options.logging.level;
        Logger.prototype.SHOW_TIMESTAMP = this.options.logging.showTimestamp;
    }

    async start() {
        if (this.server) {
            return;
        }
        process.stdout.write(`${this.options.name} (server) is starting up.\n`);

        const {host, port, user, password, agenda, service, collections: {store}} = this.options.db;
        const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
        this.mongodb = new MongoClient(
            `mongodb://${auth}${host}:${port}/`, {useNewUrlParser: true, useUnifiedTopology: true}
        );
        process.stdout.write('Connecting to database... ');
        await this.mongodb.connect();
        process.stdout.write('\rConnecting to database... Done.\n');

        const storeCollection = this.mongodb.db(service).collection(store);
        const operations = new Operations(this.mongodb.db(service));
        const taskLoader = new TaskLoader(this.options.tasks.paths, operations, undefined, storeCollection, undefined,
            (name) =>
                new Agenda({sort: {priority: -1, nextRunAt: 1}})
                    .name(name)
                    .mongo(this.mongodb.db(agenda), name),
            '_task'
        );
        operations.taskLoader = taskLoader;
        process.stdout.write('Loading task types... ');
        await taskLoader.load();
        process.stdout.write('\rLoading task types... Done.\n');

        const typeDefs = fs.readFileSync(path.join(__dirname, 'service', 'schema.graphql'))
            .toString('utf-8')
            .replace('${secret}', this.options.api.secret);
        const apollo = new ApolloServer({
            typeDefs, resolvers,
            context: ({ctx}) => ({req: ctx.req, operations, dataloaders: {}}),
            schemaDirectives: makeSchemaDirectives(),
        });

        const app = new Koa();
        app.use(async (ctx, next) => {
            await next();
            console.log(`${ctx.ip} -> ${ctx.method} ${ctx.originalUrl} ${JSON.stringify(ctx.request.body)}`);
            if (ctx.status >= 400 ||
                ctx.originalUrl === '/query' &&
                ctx.response.is('json') &&
                get(JSON.parse(ctx.body), ['errors', '0'])) {
                console.log(`${ctx.ip} <- ${ctx.method} ${ctx.originalUrl} [${ctx.status}] ${ctx.body}`);
            } else {
                console.log(`${ctx.ip} <- ${ctx.method} ${ctx.originalUrl} [${ctx.status}]`);
            }
        });

        apollo.applyMiddleware({app, path: '/query'});

        this.server = app.listen(4000, '0.0.0.0', () => {
            console.log(`${this.options.name} (server) is ready at http://0.0.0.0:4000${apollo.graphqlPath}`);
        });
        this.server.close = promisify(this.server.close.bind(this.server));

        process.on('SIGTERM', this.stop.bind(this));
        process.on('SIGINT' , this.stop.bind(this));
    }

    async stop() {
        if (!this.server) {
            return;
        }
        const server = this.server;
        const mongodb = this.mongodb;
        this.server = undefined;
        this.mongodb = undefined;
        process.stdout.write(`${this.options.name} (server) is shutting down.\n`);
        process.stdout.write('Stopping service...');
        await server.close();
        process.stdout.write('\rStopping service... Done.\n');
        process.stdout.write('Closing connections to database... ');
        await mongodb.close();
        process.stdout.write('\rClosing connections to database... Done.\n');
        process.stdout.write(`${this.options.name} (server) exits normally.\n`);
    }

};
