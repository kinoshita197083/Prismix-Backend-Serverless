# Build stage
FROM node:18-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache \
    zip \
    unzip \
    python3 \
    py3-pip \
    build-base

# Copy only package files first
COPY package*.json ./

# Install only production dependencies
RUN npm ci --only=production \
    @aws-sdk/client-s3 \
    @aws-sdk/client-dynamodb \
    @aws-sdk/client-sqs \
    @aws-sdk/lib-dynamodb \
    @supabase/supabase-js \
    archiver \
    p-limit \
    sqs-consumer

# Copy only necessary files
COPY src/handlers/zipArchiveProcessor.js ./src/handlers/
COPY src/services ./src/services
COPY src/utils ./src/utils
COPY src/containers/archive-processor/index.js ./
COPY src/containers/healthcheck.js ./

# Production stage
FROM node:18-alpine

# Install runtime dependencies
RUN apk add --no-cache zip unzip

WORKDIR /app

# Copy from builder stage
COPY --from=builder /app ./

# Set up health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD node healthcheck.js

CMD ["node", "index.js"] 