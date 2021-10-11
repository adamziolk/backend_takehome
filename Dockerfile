FROM python:3.7-slim-buster

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
WORKDIR /app
RUN apt-get update --fix-missing && \
	apt-get install -y curl git build-essential libpq-dev postgresql-client && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip

RUN pip install --prefer-binary -r /app/requirements.txt
COPY . /app
