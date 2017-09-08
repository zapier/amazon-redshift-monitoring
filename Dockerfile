FROM python:2-alpine
ADD . /code
WORKDIR /code
RUN pip install -r requirements.txt
CMD python lambda_function.py
