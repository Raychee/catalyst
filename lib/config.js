const DEFAULT_JOB_HEARTBEAT = 30;
const DEFAULT_JOB_HEART_ATTACK = 3 * 60;


const DEFAULT_TASK_DOMAIN_CONFIG = {
    maxConcurrency: 1,
    concurrency: 1,
    timeout: 0,
    delay: 0,
    delayRandomize: 0,
    retry: 0,
    retryDelayFactor: 2,
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

const TYPES = {
    TaskDomainConfig: {key: ['domain'], mtime: 'mtime'},
    TaskTypeConfig: {key: ['domain', 'type'], mtime: 'mtime'},
    Task: {key: ['id'], ctime: 'ctime', mtime: 'mtime'},
    Job: {key: ['id']},
};

const INDEXES = {
    agenda: [
        {key: {'data.taskId': 1}},
        {key: {'data.jobId': 1}},
    ],
    TaskDomainConfig: [
        {key: {'domain': 1}},
    ],
    TaskTypeConfig: [
        {key: {'domain': 1, 'type': 1}},
    ],
    Task: [
        {key: {'id': 1}},
        {key: {'domain': 1, 'type': 1}},
    ],
    Job: [
        {key: {'id': 1}},
        {key: {'status': 1}},
        {key: {'createdFrom': 1, 'task': 1}},
        {key: {'timeCreated': -1, 'domain': 1, 'type': 1, 'status': 1, 'params.userId': 1}},
    ]
};


module.exports = {
    DEFAULT_JOB_HEARTBEAT,
    DEFAULT_JOB_HEART_ATTACK,

    DEFAULT_TASK_DOMAIN_CONFIG,
    SCHEDULING_PROPERTIES,
    TASK_TYPE_INHERITED_PROPERTIES,
    TASK_INHERITED_PROPERTIES,
    JOB_INHERITED_PROPERTIES,
    COLLECTION_NAMES,
    OP_BATCH_SIZE,
    WRITE_LOCK_SPIN_MS,

    TYPES,
    INDEXES,
};