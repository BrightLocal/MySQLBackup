FROM golang:1.11
MAINTAINER Vitalii Harbulinskyi <vitaly@brightlocal.com>

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y software-properties-common wget software-properties-common
RUN apt-get install -y curl git zip unzip

RUN useradd -u 1005 -m jenkins && echo "jenkins:jenkins" | chpasswd

ENV HOME /home/jenkins
ENV GOPATH $HOME/go
ENV WORKDIR $GOPATH/src/github.com/BrightLocal/MySQLBackup
ENV PATH $GOPATH/bin:/bin:$PATH

RUN mkdir -p $WORKDIR && chown -R jenkins:jenkins /home/jenkins/go

RUN apt-get -y autoremove && apt-get -y autoclean

WORKDIR $WORKDIR

CMD while true; do sleep 1000; done
