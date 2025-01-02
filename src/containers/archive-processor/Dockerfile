FROM node:18-alpine

# Install system dependencies
RUN apk add --no-cache \
    zip \
    unzip \
    python3 \
    py3-pip \
    build-base

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies that were previously in layers
RUN npm install \
    @supabase/supabase-js \
    archiver \
    p-limit

# Copy application code
COPY src/handlers/zipArchiveProcessor.js ./src/handlers/
COPY src/services ./src/services
COPY src/utils ./src/utils

# Copy container-specific files
COPY src/containers/archive-processor/index.js ./
COPY src/containers/archive-processor/healthcheck.js ./

# Set up health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD node healthcheck.js

CMD ["node", "index.js"] 