
var serverlessSDK = require('./serverless_sdk/index.js');
serverlessSDK = new serverlessSDK({
  orgId: 'prismix',
  applicationName: 'prismix',
  appUid: '000000000000000000',
  orgUid: '000000000000000000',
  deploymentUid: 'undefined',
  serviceName: 'prismix-serverless',
  shouldLogMeta: false,
  shouldCompressLogs: true,
  disableAwsSpans: false,
  disableHttpSpans: false,
  stageName: 'dev',
  serverlessPlatformStage: 'prod',
  devModeEnabled: false,
  accessKey: null,
  pluginVersion: '7.2.3',
  disableFrameworksInstrumentation: false
});

const handlerWrapperArgs = { functionName: 'prismix-serverless-dev-deliveryProcessor', timeout: 900 };

try {
  const userHandler = require('./src/handlers/deliveryProcessor.js');
  module.exports.handler = serverlessSDK.handler(userHandler.handler, handlerWrapperArgs);
} catch (error) {
  module.exports.handler = serverlessSDK.handler(() => { throw error }, handlerWrapperArgs);
}