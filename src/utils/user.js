const jwt = require('jsonwebtoken');
const jwksClient = require('jwks-rsa');

// Initialize the JWKS client
const client = jwksClient({
    jwksUri: process.env.COGNITO_JWKS_URL, // e.g., 'https://cognito-idp.{region}.amazonaws.com/{userPoolId}/.well-known/jwks.json'
    cache: true,
    rateLimit: true,
});

// Function to get the signing key
const getSigningKey = (header, callback) => {
    client.getSigningKey(header.kid, (err, key) => {
        const signingKey = key.publicKey || key.rsaPublicKey;
        callback(null, signingKey);
    });
};

// Main function to extract and validate user ID from Cognito claims
const getUserId = (event) => {
    return new Promise((resolve, reject) => {
        const token = event.headers.Authorization;

        if (!token) {
            reject(new Error('No token provided'));
            return;
        }

        // Remove 'Bearer ' from the token string
        const jwtToken = token.replace('Bearer ', '');

        jwt.verify(jwtToken, getSigningKey, {
            algorithms: ['RS256'],
            issuer: process.env.COGNITO_ISSUER, // e.g., 'https://cognito-idp.{region}.amazonaws.com/{userPoolId}'
        }, (err, decoded) => {
            if (err) {
                console.error('Token verification failed:', err);
                reject(new Error('Invalid token'));
                return;
            }

            // The 'sub' claim in the token is the user's unique identifier
            const userId = decoded.sub;

            if (!userId) {
                reject(new Error('User ID not found in token'));
                return;
            }

            resolve(userId);
        });
    });
};

module.exports = { getUserId };