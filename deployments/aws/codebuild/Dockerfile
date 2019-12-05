FROM maven:3.6.3-jdk-8-openj9

# Install node.js and npm.
RUN apt-get update && \
    apt-get -y install nodejs npm

# Install Docker.
RUN apt-get update && \
    apt-get -y install apt-transport-https ca-certificates curl software-properties-common && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable" && \
    apt-get update && \
    apt-cache policy docker-ce && \
    apt-get -y install docker-ce
