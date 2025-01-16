const http = require('http');

const server = http.createServer((req, res) => {
    res.writeHead(200);
    res.end('healthy');
});

server.listen(80, () => {
    console.log('Health check server running');
}); 