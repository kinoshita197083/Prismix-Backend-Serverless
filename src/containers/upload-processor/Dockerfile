# Build stage
FROM node:18-alpine AS builder

WORKDIR /app

# Copy only package files first to leverage cache
COPY package*.json ./

# Install only production dependencies
RUN npm ci --only=production \
    @aws-sdk/client-s3 \
    @aws-sdk/client-dynamodb \
    @aws-sdk/lib-dynamodb \
    @supabase/supabase-js \
    sqs-consumer

# Copy only necessary files
COPY src/handlers/imageUploadFromS3Processor.js ./src/handlers/
COPY src/services ./src/services
COPY src/utils ./src/utils
COPY src/containers/upload-processor/index.js ./
COPY src/containers/healthcheck.js ./

# Production stage
FROM node:18-alpine

WORKDIR /app

# Copy from builder stage
COPY --from=builder /app ./

# Set up health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD node healthcheck.js

CMD ["node", "index.js"]