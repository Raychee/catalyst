const {GraphQLNonNull, GraphQLList, getNamedType, GraphQLObjectType} = require('graphql');
const gql = require('graphql-tag');
const {introspectSchema} = require('graphql-tools');
const {ApolloClient} = require('apollo-client');
const {ApolloLink} = require('apollo-link');
const {createHttpLink} = require('apollo-link-http');
const {onError} = require('apollo-link-error');
const {setContext} = require('apollo-link-context');
const {RetryLink} = require('apollo-link-retry');
const {createPersistedQueryLink} = require('apollo-link-persisted-queries');
const {InMemoryCache} = require('apollo-cache-inmemory');
const fetch = require('node-fetch');
const {get, isEmpty, lowerFirst} = require('lodash');

const {dedup} = require('@raychee/utils');


class GraphQLClient {
    constructor(logger, uri, id, password, httpOptions, otherOptions) {
        // httpOptions - https://www.apollographql.com/docs/link/links/http/#options
        this.logger = logger;
        this.uri = uri;
        this.id = id;
        this.password = password;
        this.jwt = undefined;
        this.links = {
            retry: new RetryLink({
                delay: {
                    initial: 300,
                    max: 5 * 60 * 1000,
                    jitter: true
                },
                attempts: {
                    max: 20,
                    retryIf: (error, _operation) => !!error
                }
            }),
            refreshTokenOnError: onError(({graphQLErrors, networkError, operation, forward}) => {
                if (graphQLErrors) {
                    for (const {message, extensions} of graphQLErrors) {
                        if (extensions.code === 'UNAUTHENTICATED') {
                            const context = operation.getContext();
                            if (!context.hasAuthRetried) {
                                context.refreshToken = true;
                                context.hasAuthRetried = true;
                                operation.setContext(context);
                                return forward(operation);
                            }
                        }
                        this.logger.info(`[GraphQL error]: ${message}`,);
                    }
                }
                if (networkError) this.logger.info(`[Network error]: ${networkError}`);
            }),
            processToken: setContext(async (__, context) => {
                if (context.noToken) {
                    return context;
                }
                context = context || {};
                if (context.refreshToken) {
                    context.refreshToken = false;
                    await this._authenticate();
                }
                if (this.jwt) {
                    context.headers = {...context.headers, authorization: `Bearer ${this.jwt}`};
                }
                return context;
            }),
            persistedQuery: createPersistedQueryLink(),
            http: createHttpLink({uri, fetch, ...httpOptions}),
        };
        this.options = otherOptions;
        this.counter = 0;

        this._authenticate = dedup(GraphQLClient.prototype._authenticate.bind(this));
    }

    async _connect() {
        await this._resetClient({force: true});
        this.schema = await introspectSchema(ApolloLink.from(['retry', 'http'].map(l => this.links[l])));
        for (const field of Object.values(this.schema.getQueryType().getFields())) {
            const queryArgDeclare = field.args.map(a => `$${a.name}: ${makeFieldTypeExpr(a.type)}`).join(', ');
            const queryArgs = field.args.map(a => `${a.name}: $${a.name}`).join(', ');
            const returnType = getNamedType(field.type);
            let defaultProjections = undefined;
            if (returnType instanceof GraphQLObjectType) {
                let returnTypeFields = returnType.getFields();
                const hasResults = returnTypeFields.results;
                if (hasResults) {
                    returnTypeFields = getNamedType(returnTypeFields.results.type).getFields();
                }
                defaultProjections = {[returnTypeFields.mtime ? 'mtime' : Object.keys(returnTypeFields)[0]]: true};
                if (hasResults) {
                    defaultProjections = {results: defaultProjections};
                }
            }
            this[field.name] = async (logger, variables, projections) => {
                logger = logger || this.logger;
                if (!projections || isEmpty(projections)) projections = defaultProjections;
                try {
                    await this._resetClient();
                    const resp = await this.apolloClient.query({
                        query: gql`query (${queryArgDeclare}) { ${field.name} (${queryArgs}) ${makeReturnExpr(projections)} }`,
                        variables
                    });
                    return resp.data[field.name];
                } catch (e) {
                    if (e.networkError && (!e.networkError.statusCode || e.networkError.statusCode >= 500)) {
                        logger.fail('_failed_api_server_error', e);
                    } else {
                        throw e;
                    }
                }
            }
        }
        for (const field of Object.values(this.schema.getMutationType().getFields())) {
            const queryArgDeclare = field.args.map(a => `$${a.name}: ${makeFieldTypeExpr(a.type)}`).join(', ');
            const queryArgs = field.args.map(a => `${a.name}: $${a.name}`).join(', ');
            const returnType = getNamedType(field.type);
            let defaultProjections = undefined;
            if (returnType instanceof GraphQLObjectType) {
                let returnTypeFields = returnType.getFields();
                const hasResults = returnTypeFields.results;
                if (hasResults) {
                    returnTypeFields = getNamedType(returnTypeFields.results.type).getFields();
                }
                defaultProjections = {[returnTypeFields.mtime ? 'mtime' : Object.keys(returnTypeFields)[0]]: true};
                if (hasResults) {
                    defaultProjections = {results: defaultProjections};
                }
            }
            this[field.name] = async (logger, variables, projections) => {
                logger = logger || this.logger;
                if (!projections || isEmpty(projections)) projections = defaultProjections;
                try {
                    await this._resetClient();
                    const resp = await this.apolloClient.mutate({
                        mutation: gql`mutation (${queryArgDeclare}) { ${field.name} (${queryArgs}) ${makeReturnExpr(projections)} }`,
                        variables
                    });
                    return resp.data[field.name];
                } catch (e) {
                    if (e.networkError && (!e.networkError.statusCode || e.networkError.statusCode >= 500)) {
                        logger.fail('_failed_api_server_error', e);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    async _authenticate() {
        this.logger.info('Try authenticate...');
        const resp = await this.apolloClient.query({
            query: gql`
                query ($id: ID!, $password: String!) {
                    User(id: $id, password: $password) { results {jwt} }
                }`,
            variables: {id: this.id, password: this.password},
            context: {
                noToken: true, hasAuthRetried: true,
                // queryDeduplication: false, forceFetch: true
            },
            // fetchPolicy: 'no-cache'
        });
        const jwt = get(resp, ['data', 'User', 'results', 0, 'jwt']);
        if (!jwt) {
            this.logger.fail('_failed_jwt', 'authentication failed: no jwt from response ', resp);
        }
        this.logger.info('Authenticate succeeded. ');
        this.jwt = jwt;
    }

    async _resetClient({force = false} = {}) {
        this.counter++;
        if (force || this.counter > this.options.resetStoreEvery) {
            this.apolloClient = new ApolloClient({
                link: ApolloLink.from(['retry', 'refreshTokenOnError', 'processToken', 'persistedQuery', 'http'].map(l => this.links[l])),
                cache: new InMemoryCache(),
                queryDeduplication: false,
                defaultOptions: {
                    query: {fetchPolicy: 'no-cache'},
                    mutate: {fetchPolicy: 'no-cache'},
                }
            });
            this.counter = 0;
        }
    }

}

function makeFieldTypeExpr(type) {
    if (type instanceof GraphQLNonNull) {
        return `${makeFieldTypeExpr(type.ofType)}!`;
    }
    if (type instanceof GraphQLList) {
        return `[${makeFieldTypeExpr(type.ofType)}]`;
    }
    return type.name;
}

function makeReturnExpr(projections) {
    if (!projections) return '';
    return `{${Object.entries(projections)
        .map(([field, show]) => {
            if (typeof show === 'object') {
                const expr = makeReturnExpr(show);
                if (expr === '{}') {
                    return;
                }
                return `${field} ${expr}`;
            }
            if (show) {
                return field;
            }
        })
        .filter(v => v)
        .join(', ')}}`;
}


module.exports = {
    key({uri, id, password, httpOptions, otherOptions = {resetStoreEvery: 100}} = {}) {
        return {uri, id, password, httpOptions, otherOptions};
    },
    async create({uri, id, password, httpOptions, otherOptions = {resetStoreEvery: 100}} = {}) {
        const client = new GraphQLClient(this, uri, id, password, httpOptions, otherOptions);
        await client._connect();
        return client;
    },
};