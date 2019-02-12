FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install -y apt-utils nginx node npm 
RUN nodejs -v
RUN npm -v

