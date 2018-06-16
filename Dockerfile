FROM python:3.6-alpine

ADD . .

RUN pip install -r requirements.txt

ENV GOOGLE_APPLICATION_CREDENTIALS ./creds.json

ENTRYPOINT ["python"]
