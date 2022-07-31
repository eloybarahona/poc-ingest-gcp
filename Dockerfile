FROM python:3.9-slim

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV PORT 8080
ENV HOST 0.0.0.0

WORKDIR $APP_HOME
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt
RUN python -m pip list
RUN dir

ENTRYPOINT python main.py

