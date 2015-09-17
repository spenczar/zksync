#!/bin/bash

set -ex -o pipefail

apt-get update -qq
apt-get install -y default-jre curl

INSTALL_ROOT=/opt
REPO="${REPO:-/vagrant}"

# install toxiproxy
TOXIPROXY_VERSION=1.0.3
if [ ! -f ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ]; then
    wget --quiet https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY_VERSION}/toxiproxy-linux-amd64 -O ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
    chmod +x ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
fi
rm -f ${INSTALL_ROOT}/toxiproxy
ln -s ${INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ${INSTALL_ROOT}/toxiproxy

stop toxiproxy || true
cp ${REPO}/vagrant/toxiproxy.conf /etc/init/toxiproxy.conf
cp ${REPO}/vagrant/run_toxiproxy.sh ${INSTALL_ROOT}/
start toxiproxy

# get zookeeper
ZK_VERSION=3.4.6
ZK_CHECKSUM=971c379ba65714fd25dc5fe8f14e9ad1

ZK_BASE_DIR=${INSTALL_ROOT}/zookeeper
mkdir -p ${ZK_BASE_DIR}

curl "http://apache.cs.utah.edu/zookeeper/zookeeper-${ZK_VERSION}/zookeeper-${ZK_VERSION}.tar.gz" > /tmp/zookeeper-${ZK_VERSION}.tar.gz
echo "${ZK_CHECKSUM}  /tmp/zookeeper-${ZK_VERSION}.tar.gz" | md5sum -c


for i in 1 2 3 4 5; do
    ZK_PORT=`expr $i + 2180`
    ZK_PORT_REAL=`expr $i + 21800`
    ZK_DIR="${ZK_BASE_DIR}/zk-${ZK_PORT}"

    # unpack zookeeper
    mkdir -p ${ZK_DIR}
    tar -C ${ZK_DIR} -xzf /tmp/zookeeper-${ZK_VERSION}.tar.gz --strip-components 1

    ZK_DATADIR="${ZK_DIR}/data"
    mkdir -p ${ZK_DATADIR}

    # zookeeper configuration
    ZK_CONF="${ZK_DIR}/conf/zookeeper.properties"
    cp ${REPO}/vagrant/zookeeper.properties ${ZK_CONF}

    sed -i s#ZK_DATADIR#${ZK_DATADIR}#g ${ZK_CONF}
    sed -i s/ZK_PORT/${ZK_PORT_REAL}/g ${ZK_CONF}

    echo $i > ${ZK_DATADIR}/myid

    ZK_SVC=zookeeper-${ZK_PORT}
    stop $ZK_SVC || true
    # set up zk service
    cp ${REPO}/vagrant/zookeeper.conf /etc/init/${ZK_SVC}.conf
    sed -i s#ZK_DIR#${ZK_DIR}#g /etc/init/${ZK_SVC}.conf
    sed -i s#ZK_CONF#${ZK_CONF}#g /etc/init/${ZK_SVC}.conf

    start ${ZK_SVC}
done
