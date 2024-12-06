FROM public.ecr.aws/lambda/python:3.9

# Install OpenCV and other dependencies
RUN pip install --no-cache-dir \
    opencv-python-headless \
    numpy

# Copy function code
COPY index.py ${LAMBDA_TASK_ROOT}

CMD [ "index.handler" ]
