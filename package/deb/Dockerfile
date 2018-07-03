FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y gdebi-core
ADD build/packages/lookatch-agent*.deb var/lookatch-agent.deb
RUN gdebi -n /var/lookatch-agent.deb

# Various tests and checks
RUN ls -lh /usr/bin/lookatch-agent
RUN ls -lh /etc/lookatch-agent/config.yaml
RUN ls -lh /var/log/lookatch-agent
RUN ls -lh /etc/logrotate.d/lookatch-agent
RUN ls -lh /etc/init.d/lookatch-agent


CMD ["/bin/bash"]
