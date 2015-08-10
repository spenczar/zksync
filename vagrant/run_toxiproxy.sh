#!/bin/sh

set -ex

${INSTALL_ROOT}/toxiproxy -port 8474 -host 0.0.0.0 &
PID=$!

while ! nc -q 1 localhost 8474 </dev/null; do echo "Waiting"; sleep 1; done

wget -O/dev/null -S --post-data='{"name":"zk1", "upstream":"localhost:21801", "listen":"0.0.0.0:2181"}' localhost:8474/proxies
wget -O/dev/null -S --post-data='{"name":"zk2", "upstream":"localhost:21802", "listen":"0.0.0.0:2182"}' localhost:8474/proxies
wget -O/dev/null -S --post-data='{"name":"zk3", "upstream":"localhost:21803", "listen":"0.0.0.0:2183"}' localhost:8474/proxies
wget -O/dev/null -S --post-data='{"name":"zk4", "upstream":"localhost:21804", "listen":"0.0.0.0:2184"}' localhost:8474/proxies
wget -O/dev/null -S --post-data='{"name":"zk5", "upstream":"localhost:21805", "listen":"0.0.0.0:2185"}' localhost:8474/proxies

wait $PID
