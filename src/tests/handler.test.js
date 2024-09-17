const { handler } = require('../src/handlers/handler');

test('hello function returns correct response', async () => {
    const event = {};
    const response = await handler(event);
    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body.message).toBe('Hello from Serverless!');
});