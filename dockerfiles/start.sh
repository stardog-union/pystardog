#!/bin/bash -e
echo "Debugging start stardog"
/usr/sbin/sshd -f ${HOME}/custom_ssh/sshd_config -D -p 2222 &
echo "ssh server should have started"
/opt/stardog/bin/stardog-admin server start
echo "Debugging stardog server should have started"
tail -f /dev/null