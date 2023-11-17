#!/usr/local/bin/zsh
~/tools/hadoop/sbin/start-dfs.sh
~/tools/spark/sbin/start-master.sh
~/tools/spark/sbin/start-slave.sh spark://Leon-mac:7077
~/tools/spark/sbin/start-history-server.sh

