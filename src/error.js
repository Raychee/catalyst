class CatalystError extends Error {
    constructor(code, message) {
        super(message);
        this.name = 'CatalystError';
        this.code = code;
    }
}

class JobCrash extends CatalystError {
    constructor(code, message) {
        super(code, message);
        this.name = 'JobCrash';
    }
}

class JobCancellation extends CatalystError {
    constructor(code, message) {
        super(code, message);
        this.name = 'JobCancellation';
    }
}

class JobInterruption extends CatalystError {
    constructor(code, message) {
        super(code, message);
        this.name = 'JobInterruption';
    }
}


class JobTimeout extends CatalystError {
    constructor(code, message) {
        super(code, message);
        this.name = 'JobTimeout';
    }
}

class JobHeartAttack extends CatalystError {
    constructor(code, message) {
        super(code, message);
        this.name = 'JobHeartAttack';
    }
}

module.exports = {
    CatalystError,
    JobCrash,
    JobCancellation,
    JobInterruption,
    JobTimeout,
    JobHeartAttack,
};