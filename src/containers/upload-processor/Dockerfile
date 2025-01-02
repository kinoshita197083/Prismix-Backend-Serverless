FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies that were previously in layers
RUN npm install \
    @supabase/supabase-js

# Copy application code
COPY src/handlers/imageUploadFromS3Processor.js ./src/handlers/
COPY src/services ./src/services
COPY src/utils ./src/utils

# Copy container-specific files
COPY src/containers/upload-processor/index.js ./
COPY src/containers/upload-processor/healthcheck.js ./

# Set up health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD node healthcheck.js

CMD ["node", "index.js"]