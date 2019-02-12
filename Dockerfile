FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install -y apt-utils nginx nodejs npm
RUN nodejs -v
RUN npm -v

RUN ls
RUN pwd
RUN mkdir fileserver
RUN ls
COPY ./fileserver ./fileserver
RUN cd fileserver; rm -rf node_modules; npm install

EXPOSE 8080

CMD ['cd','fileserver']
CMD ['nodejs','index.js']