#!/bin/bash
# Replacement for /etc/kafka/docker/configure — avoids the ${!1} indirect-expansion
# bug present in apache/kafka:3.8.x and 3.9.x when SSL env vars are set.
set -euo pipefail

if [[ -z "${CLUSTER_ID:-}" ]]; then
    CLUSTER_ID="5L6g3nShT-eMCtK--X86sw"
    echo "CLUSTER_ID not set. Setting it to default value: \"${CLUSTER_ID}\""
fi
export CLUSTER_ID

echo "===> Configuring ..."
echo "Running in KRaft mode..."

PROPS_FILE="/etc/kafka/kraft/server.properties"
mkdir -p "$(dirname "${PROPS_FILE}")"

{
    echo "node.id=${KAFKA_NODE_ID}"
    echo "process.roles=${KAFKA_PROCESS_ROLES}"
    echo "listeners=${KAFKA_LISTENERS}"
    echo "advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}"
    echo "listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}"
    echo "inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME}"
    echo "controller.listener.names=${KAFKA_CONTROLLER_LISTENER_NAMES}"
    echo "controller.quorum.voters=${KAFKA_CONTROLLER_QUORUM_VOTERS}"
    echo "offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR}"
    echo "auto.create.topics.enable=${KAFKA_AUTO_CREATE_TOPICS_ENABLE}"
    echo "log.retention.hours=${KAFKA_LOG_RETENTION_HOURS}"
    echo "log.dirs=/tmp/kraft-combined-logs"
} > "${PROPS_FILE}"

if [[ -n "${KAFKA_SSL_KEYSTORE_LOCATION:-}" ]] || [[ -n "${KAFKA_SSL_TRUSTSTORE_LOCATION:-}" ]]; then
    echo "SSL is enabled."
    {
        echo "ssl.keystore.location=${KAFKA_SSL_KEYSTORE_LOCATION}"
        echo "ssl.keystore.password=${KAFKA_SSL_KEYSTORE_PASSWORD}"
        echo "ssl.key.password=${KAFKA_SSL_KEY_PASSWORD}"
        echo "ssl.truststore.location=${KAFKA_SSL_TRUSTSTORE_LOCATION}"
        echo "ssl.truststore.password=${KAFKA_SSL_TRUSTSTORE_PASSWORD}"
        echo "ssl.client.auth=${KAFKA_SSL_CLIENT_AUTH:-none}"
    } >> "${PROPS_FILE}"
fi
