#!/bin/bash
systemctl stop lookatch-agent.service
update-rc.d -f lookatch-agent remove
