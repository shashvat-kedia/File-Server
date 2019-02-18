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
WORKDIR fileserver
RUN ls
RUN rm -rf node_modules
RUN npm install

RUN ip show
RUN ip addr show

RUN systemcl start nginx
RUN systemcl status nginx

RUN ufw allow https comment 'Open all to access Nginx port 443'
RUN ufw allow http comment 'Open access Nginx port 80'
RUN ufw allow ssh comment 'Open access OpenSSH port 22'
RUN ufw enable
RUN ufw status

RUN ping 

EXPOSE 8080 80 443

CMD ["nodejs","index.js"]