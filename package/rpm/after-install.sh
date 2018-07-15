#!/bin/bash
chkconfig --add lookatch-agent
cat << EOF > /etc/logrotate.d/lookatch-agent
/var/log/lookatch/lookatch-agent.out /var/log/lookatch/lookatch-agent.err {
  daily
  missingok
  copytruncate
  rotate 7
  compress
  delaycompress
}
EOF

systemctl enable lookatch-agent.service
systemctl start lookatch-agent.service
