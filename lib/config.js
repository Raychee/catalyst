const DEFAULT_HEARTBEAT = 5;
const DEFAULT_HEART_ATTACK = 60;


const DEFAULT_TASK_DOMAIN_CONFIG = {
    maxConcurrency: 1,
    concurrency: 1,
    timeout: -1,
    delay: 0,
    delayRandomize: 0,
    retry: 0,
    retryDelayFactor: 1,
    priority: 0,
    dedupWithin: -1,
    dedupRecent: true,
};

const SCHEDULING_PROPERTIES = [
    'timeout',
    'delay',
    'delayRandomize',
    'retry',
    'retryDelayFactor',
    'priority',
    'dedupWithin',
    'dedupRecent'
];

const TASK_TYPE_INHERITED_PROPERTIES = [
    'concurrency', ...SCHEDULING_PROPERTIES
];

const TASK_INHERITED_PROPERTIES = [
    ...SCHEDULING_PROPERTIES, 'subTasks',
];

const JOB_INHERITED_PROPERTIES = [
    'params', ...SCHEDULING_PROPERTIES,
];

const COLLECTION_NAMES = {
    Scheduler: 'Scheduler',
    Domain: 'Domain',
    Type: 'Type',
    Task: 'Task',
    Job: 'Job',
    Store: 'Store',
};

const OP_BATCH_SIZE = 1000;
const WRITE_LOCK_SPIN_MS = 100;

const INDEXES = {
    Scheduler: [
        {key: {heartbeat: 1}},
    ],
    Domain: [
        {key: {domain: 1}},
    ],
    Type: [
        {key: {domain: 1, type: 1}},
    ],
    Task: [
        {key: {lockedBy: 1}},
        {key: {enabled: 1, nextTime: 1}},
    ],
    Job: [
        {key: {status: 1, domain: 1, priority: -1, timeCreated: 1, lockedBy: 1, type: 1}},
        {key: {status: 1, lockedBy: 1}},
        {key: {lockedBy: 1}},
        {key: {domain: 1, type: 1, status: 1, timeStopped: 1}},
        {key: {task: 1, domain: 1, type: 1, status: 1}},
    ],
    Store: [
        {key: {plugin: 1}},
        {key: {domain: 1}},
    ],
};

const OPERATOR_TO_MONGO_OPERATOR = {
    is: '$eq',
    not: '$ne',
    in: '$in',
    not_in: '$nin',
    gt: '$gt',
    gte: '$gte',
    lt: '$lt',
    lte: '$lte',
};


module.exports = {
    DEFAULT_HEARTBEAT,
    DEFAULT_HEART_ATTACK,

    DEFAULT_TASK_DOMAIN_CONFIG,
    SCHEDULING_PROPERTIES,
    TASK_TYPE_INHERITED_PROPERTIES,
    TASK_INHERITED_PROPERTIES,
    JOB_INHERITED_PROPERTIES,
    COLLECTION_NAMES,
    OP_BATCH_SIZE,
    WRITE_LOCK_SPIN_MS,

    INDEXES,
    OPERATOR_TO_MONGO_OPERATOR,
};
