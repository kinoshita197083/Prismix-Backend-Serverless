FROM public.ecr.aws/lambda/python:3.9

# Install OpenCV and other dependencies
RUN pip install --no-cache-dir \
    opencv-python-headless \
    numpy

# Set environment variables
ENV NODE_ENV=production
ENV JOB_PROGRESS_TABLE=${JOB_PROGRESS_TABLE}
ENV TASKS_TABLE=${TASKS_TABLE}

# Copy function code
COPY src/blur-detection/index.py ${LAMBDA_TASK_ROOT}

CMD [ "index.handler" ]
