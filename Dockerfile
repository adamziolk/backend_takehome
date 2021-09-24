FROM python:bullseye

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
WORKDIR /app
RUN apt-get update --fix-missing && \
	apt-get install -y curl git build-essential libpq-dev && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY . /app
