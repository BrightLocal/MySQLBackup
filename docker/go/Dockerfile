FROM golang:1.10

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y software-properties-common wget nano software-properties-common
RUN apt-get update && apt-get install -y curl git zip unzip sudo

RUN useradd -u 1005 -m jenkins && echo "jenkins:jenkins" | chpasswd && adduser jenkins sudo

RUN sudo echo "jenkins ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

RUN mkdir /home/jenkins/app && chown jenkins:jenkins /home/jenkins/app && mkdir /home/jenkins/go && chown jenkins:jenkins /home/jenkins/go

RUN rm -rf /var/cache/apk/*

RUN apt-get -y autoremove && apt-get -y autoclean

ENV GOPATH /home/jenkins/go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:/bin:$PATH

WORKDIR /home/jenkins/app

CMD while true; do sleep 1000; done
