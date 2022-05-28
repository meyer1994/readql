FROM amazon/aws-lambda-python:3.9

RUN yum -y install git gcc sqlite-dev \
    && yum -y clean all \
    && rm -rf /var/cache

ADD requirements.txt  .
RUN  pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

ADD saaslite ${LAMBDA_TASK_ROOT}/saaslite
ADD handler.py ${LAMBDA_TASK_ROOT}/

CMD [ "handler.handler" ]
