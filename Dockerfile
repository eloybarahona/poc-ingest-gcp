FROM python:3.9-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app

WORKDIR $APP_HOME
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINTpython -m pip list
ENTRYPOINT python main.py

