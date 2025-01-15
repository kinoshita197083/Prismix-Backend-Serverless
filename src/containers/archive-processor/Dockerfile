# Build stage
FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY src/containers/archive-processor/package*.json ./

# Install dependencies with specific flags for production
RUN npm install --production --no-optional

# Copy application code
COPY src/containers/archive-processor/index.js .
COPY src/handlers/zipArchiveProcessor.js ./src/handlers/
COPY src/utils ./src/utils
COPY src/services ./src/services

# Set environment variables
ENV NODE_ENV=production
ENV SUPABASE_URL=${SUPABASE_URL}
ENV SUPABASE_API_KEY=${SUPABASE_API_KEY}

CMD ["node", "index.js"] 