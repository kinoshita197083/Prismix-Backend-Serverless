FROM public.ecr.aws/lambda/nodejs:20

# set work directory
WORKDIR /var/task

COPY package*.json ./
RUN npm install --arch=x64 --platform=linux --include=optional sharp
