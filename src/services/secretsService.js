const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const logger = require('../utils/logger');

const secretsManager = new SecretsManagerClient();

const secretsService = {
    async getCredentials() {
        try {
            const secretResponse = await secretsManager.send(
                new GetSecretValueCommand({
                    SecretId: `${process.env.SERVICE_NAME}/${process.env.STAGE}/prismix-user-credentials`,
                    VersionStage: "AWSCURRENT"
                })
            );

            const credentials = JSON.parse(secretResponse.SecretString);

            logger.info('Retrieved credentials from Secrets Manager', {
                hasAccessKey: !!credentials.accessKeyId,
                hasSecretKey: !!credentials.secretAccessKey
            });

            return credentials;
        } catch (error) {
            logger.error('Failed to retrieve credentials from Secrets Manager', {
                error: {
                    name: error.name,
                    message: error.message,
                    code: error.code
                }
            });
            throw error;
        }
    }
};

module.exports = secretsService;
