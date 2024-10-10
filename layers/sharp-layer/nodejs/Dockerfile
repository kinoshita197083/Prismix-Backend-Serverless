FROM public.ecr.aws/lambda/nodejs:20

WORKDIR /var/task

COPY package*.json ./
RUN npm install --arch=x64 --platform=linux --optional=true sharp
