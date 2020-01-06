const DEFAULT_HEARTBEAT = 5;
const DEFAULT_HEART_ATTACK = 60;


const DEFAULT_TASK_DOMAIN_CONFIG = {
    maxConcurrency: 1,
    concurrency: 1,
    timeout: 0,
    delay: 0,
    delayRandomize: 0,
    retry: 0,
    retryDelayFactor: 1,
    priority: 0,
    dedupWithin: 0,
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

const OP_BATCH_SIZE = 100;
const WRITE_LOCK_SPIN_MS = 100;

const INDEXES = {
    Domain: [
        {key: {domain: 1}},
    ],
    Type: [
        {key: {domain: 1, type: 1}},
    ],
    Task: [
        {key: {domain: 1, type: 1}},
    ],
    Job: [
        {key: {status: 1, domain: 1, priority: -1, timeCreated: 1, lockedBy: 1, type: 1}},
        {key: {createdFrom: 1, task: 1}},
        {key: {timeCreated: -1, domain: 1, type: 1, status: 1, 'params.userId': 1}},
    ]
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
};