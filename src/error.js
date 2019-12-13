class CrawlerError extends Error {

    constructor(code, message) {
        super(message);
        this.name = 'CrawlerError';
        this.code = code;
    }

}

class CrawlerIntentionalCrash extends CrawlerError {
    constructor(code, message) {
        super(code, message);
    }
}

class CrawlerCancellation extends CrawlerError {
    constructor(code, message) {
        super(code, message);
    }
}

class CrawlerInterruption extends CrawlerError {
    constructor(code, message) {
        super(code, message);
    }
}


class CrawlerTimeout extends CrawlerError {
    constructor(code, message) {
        super(code, message);
    }
}

module.exports = {
    CrawlerError,
    CrawlerIntentionalCrash,
    CrawlerCancellation,
    CrawlerInterruption,
    CrawlerTimeout,
};