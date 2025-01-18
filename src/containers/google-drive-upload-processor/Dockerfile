FROM node:20-alpine

WORKDIR /app

COPY src/containers/google-drive-upload-processor/package*.json ./

RUN npm install --production --no-optional

COPY src/containers/google-drive-upload-processor/index.js ./
COPY src/handlers/imageUploader.js ./src/handlers/
COPY src/utils ./src/utils
COPY src/services ./src/services

ENV NODE_ENV=production
ENV SUPABASE_URL=${SUPABASE_URL}
ENV SUPABASE_API_KEY=${SUPABASE_API_KEY}

CMD ["node", "index.js"]