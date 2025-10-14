FROM python:3.10-slim

RUN apt-get update
RUN apt-get install git -y

RUN pip3 install --upgrade pip
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

ENV HOME=/opt/src/
RUN mkdir -p $HOME
