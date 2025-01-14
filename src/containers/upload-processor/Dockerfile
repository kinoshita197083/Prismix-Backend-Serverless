FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY src/containers/upload-processor/package*.json ./

# Install dependencies
RUN npm install

# Copy application code
COPY src/containers/upload-processor/index.js .
COPY src/handlers/imageUploadFromS3Processor.js ./src/handlers/
COPY src/utils ./src/utils
COPY src/utils/logger.js ./src/utils/
COPY src/services ./src/services

# Set environment variables
ENV NODE_ENV=production

# Start the application
CMD ["node", "index.js"]