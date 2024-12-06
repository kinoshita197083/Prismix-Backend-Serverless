#!/bin/bash

# Build the image
docker build -t blur-detection ../src/blur-detection

# Run the container with test event
docker run -v $(pwd)/test-events/blur-detect-event.json:/var/task/test-event.json \
  -e AWS_LAMBDA_FUNCTION_HANDLER=index.handler \
  blur-detection \
  node -e "
    const handler = require('./index').handler;
    const event = require('./test-events/blur-detect-event.json');
    handler(event).then(console.log).catch(console.error);
  "