FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install -y curl apt-transport-https
RUN apt-get install -y apt-utils nginx nodejs npm
RUN nodejs -v
RUN npm -v

RUN ls
RUN pwd
RUN mkdir fileserver
RUN ls
COPY ./fileserver ./fileserver
WORKDIR fileserver
RUN ls
RUN rm -rf node_modules
RUN npm install

EXPOSE 8080 80 443

CMD ["nodejs","index.js"]