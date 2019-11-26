const JOB_DELAY_CHECK_INTERVAL = 25;
const JOB_MIN_CHECK_INTERVAL = 5;

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
};

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
        {key: {'params.noteId': 1, 'timeCreated': -1, 'domain': 1, 'type': 1, 'status': 1}},
        {key: {'params.userId': 1, 'timeCreated': -1, 'domain': 1, 'type': 1, 'status': 1}},
        {key: {'timeCreated': -1, 'domain': 1, 'type': 1, 'status': 1, 'params.userId': 1}},
        // {key: {'timeStarted': 1}, expireAfterSeconds: 3 * 30 * 24 * 60 * 60},
    ]
};


module.exports = {
    JOB_DELAY_CHECK_INTERVAL,
    JOB_MIN_CHECK_INTERVAL,

    DEFAULT_TASK_DOMAIN_CONFIG,
    TYPES,
    INDEXES,
};