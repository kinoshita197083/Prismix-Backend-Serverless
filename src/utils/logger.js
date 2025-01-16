const logger = {
    info: (message, params = {}) => {
        console.log(JSON.stringify({ level: 'INFO', message, ...params }));
    },
    error: (message, params = {}) => {
        console.error(JSON.stringify({ level: 'ERROR', message, ...params }));
    },
    warn: (message, params = {}) => {
        console.warn(JSON.stringify({ level: 'WARN', message, ...params }));
    },
    debug: (message, params = {}) => {
        console.debug(JSON.stringify({ level: 'DEBUG', message, ...params }));
    }
};

module.exports = logger;