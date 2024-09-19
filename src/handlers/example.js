module.exports.hello = async (event) => {
    console.log(`ENV Test Variable: ${process.env.TEST}`);

    return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Hello from serverless!' }),
    };
};