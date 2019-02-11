FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y apt-utils nginx 