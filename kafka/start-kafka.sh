#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="/opt/kafka"
CONFIG_FILE="$KAFKA_HOME/config/kraft/server.properties"
DATA_DIR="${KAFKA_DATA_DIR:-/var/lib/kafka/data}"

mkdir -p "$DATA_DIR"

: "${KAFKA_CFG_NODE_ID:?KAFKA_CFG_NODE_ID is required}"
: "${KAFKA_CFG_PROCESS_ROLES:?KAFKA_CFG_PROCESS_ROLES is required}"
: "${KAFKA_CFG_CONTROLLER_QUORUM_VOTERS:?KAFKA_CFG_CONTROLLER_QUORUM_VOTERS is required}"
: "${KAFKA_CFG_LISTENERS:?KAFKA_CFG_LISTENERS is required}"
: "${KAFKA_CFG_CONTROLLER_LISTENER_NAMES:?KAFKA_CFG_CONTROLLER_LISTENER_NAMES is required}"
: "${KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP:?KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP is required}"
: "${KAFKA_KRAFT_CLUSTER_ID:?KAFKA_KRAFT_CLUSTER_ID is required}"

cat > "$CONFIG_FILE" <<PROPS
process.roles=${KAFKA_CFG_PROCESS_ROLES}
node.id=${KAFKA_CFG_NODE_ID}
controller.quorum.voters=${KAFKA_CFG_CONTROLLER_QUORUM_VOTERS}
listeners=${KAFKA_CFG_LISTENERS}
advertised.listeners=${KAFKA_CFG_ADVERTISED_LISTENERS:-}
controller.listener.names=${KAFKA_CFG_CONTROLLER_LISTENER_NAMES}
listener.security.protocol.map=${KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP}
inter.broker.listener.name=${KAFKA_CFG_INTER_BROKER_LISTENER_NAME:-PLAINTEXT}
log.dirs=${DATA_DIR}
offsets.topic.replication.factor=${KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR:-3}
transaction.state.log.replication.factor=${KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-3}
transaction.state.log.min.isr=${KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR:-2}
group.initial.rebalance.delay.ms=${KAFKA_CFG_GROUP_INITIAL_REBALANCE_DELAY_MS:-0}
num.partitions=${KAFKA_CFG_NUM_PARTITIONS:-1}
PROPS

if [ ! -f "${DATA_DIR}/meta.properties" ]; then
  "$KAFKA_HOME/bin/kafka-storage.sh" format \
    --ignore-formatted \
    --cluster-id "$KAFKA_KRAFT_CLUSTER_ID" \
    --config "$CONFIG_FILE"
fi

exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONFIG_FILE"
