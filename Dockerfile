FROM public.ecr.aws/lambda/python:3.12

# Copy and install dependencies first for better layer caching.
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

COPY s3_replicator ${LAMBDA_TASK_ROOT}/s3_replicator

CMD ["s3_replicator.handler.handler"]
