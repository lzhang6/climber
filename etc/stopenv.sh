#!/usr/local/bin/zsh
~/tools/hadoop/sbin/stop-all.sh
~/tools/spark/sbin/stop-master.sh
~/tools/spark/sbin/stop-slave.sh spark://Leon-mac:7077
~/tools/spark/sbin/stop-history-server.shs