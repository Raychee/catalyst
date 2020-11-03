const Server = require('./lib/server');
const Daemon = require('./lib/daemon');
const Debugger = require('./lib/debugger');


module.exports = {
    Server,
    Daemon,
    Debugger,
    
    startDaemon(options) {
        const daemon = new Daemon(options);
        return daemon.start({waitForStop: true}).catch(e => {
            console.error(e);
            return daemon.stop({exit: true});
        });
    },
    startServer(options) {
        const server = new Server(options);
        return server.start({waitForStop: true}).catch(e => console.error(e));
    }
};
