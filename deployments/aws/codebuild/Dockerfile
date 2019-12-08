FROM maven:3.6.3-jdk-8

# Install node.js and npm.
RUN apt-get update && \
    apt-get -y install software-properties-common curl && \
    curl -sL https://deb.nodesource.com/setup_12.x | bash - && \
    apt-get -y install nodejs

# Install Docker.
RUN apt-get update && \
    apt-get -y install apt-transport-https ca-certificates gnupg2 && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" && \
    apt-get update && \
    apt-cache policy docker-ce && \
    apt-get -y install docker-ce
