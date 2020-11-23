"use strict";

const {stringifyWith} = require('@raychee/utils');


class CatchableError extends Error {
    constructor(e) {
        super();
        this.name = 'CatchableError';
        this.error = e;
    }
}

class CatalystError extends Error {
    constructor(name, ...message) {
        super();
        this.name = name || 'CatalystError';
        this.message = stringifyWith(message);
        if (message.length === 1 && message[0] instanceof Error) {
            this.cause = message[0];
        }
    }
}

class JobError extends CatalystError {
    constructor(name, code, ...message) {
        super(name || 'JobError', ...message);
        this.code = code;
    }
}

class JobRuntimeError extends JobError {
    constructor(name, code, ...message) {
        super(name || 'JobRuntimeError', code, ...message);
    }
}

class JobRuntime extends JobRuntimeError {
    constructor(code, ...message) {
        super('JobRuntime', code, ...message);
    }
}

class JobTimeout extends JobRuntimeError {
    constructor(code, ...message) {
        super('JobTimeout', code, ...message);
    }
}

class JobEarlyExit extends JobError {
    constructor(name, code, ...message) {
        super(name || 'JobEarlyExit', code, ...message);
    }
}

class JobCancellation extends JobEarlyExit {
    constructor(code, ...message) {
        super('JobCancellation', code, ...message);
    }
}

class JobInterruption extends JobEarlyExit {
    constructor(code, ...message) {
        super('JobInterruption', code, ...message);
    }
}

class JobCrash extends JobError {
    constructor(code, ...message) {
        super('JobCrash', code, ...message);
    }
}

class HeartAttack extends CatalystError {
    constructor(...message) {
        super('HeartAttack', ...message);
    }
}

class OperationError extends CatalystError {
    constructor(...message) {
        super('OperationError', ...message);
    }
}

class SystemError extends CatalystError {
    constructor(...message) {
        super('SystemError', ...message);
    }
}


module.exports = {
    CatchableError,
    CatalystError,
    JobRuntimeError,
    JobRuntime,
    JobTimeout,
    JobEarlyExit,
    JobCancellation,
    JobInterruption,
    JobCrash,
    HeartAttack,
    OperationError,
    SystemError,
};
