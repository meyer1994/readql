FROM amazon/aws-lambda-python:3.12

ADD requirements.txt  .
RUN  pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

ADD readql ${LAMBDA_TASK_ROOT}/readql
ADD handler.py ${LAMBDA_TASK_ROOT}/

CMD [ "handler.handler" ]
