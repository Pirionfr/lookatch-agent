FROM centos:latest

RUN apt-get update
ADD target/packages/lookatch-agent*.rpm var/lookatch-agent.rpm
RUN rpm -i /var/lookatch-agent.rpm

# Various tests and checks
RUN ls -lh /usr/bin/lookatch-agent
RUN ls -lh /etc/lookatch/config.json
RUN ls -lh /var/log/lookatch-agent
RUN ls -lh /etc/logrotate.d/lookatch-agent
RUN ls -lh /etc/init.d/lookatch-agent


CMD ["/bin/bash"]
