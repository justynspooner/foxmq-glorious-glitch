#!/usr/bin/env bash
#
# 🦊✨ The Glorious Glitch — FoxMQ Message Ordering Demo
#
# Starts a 4-node FoxMQ cluster using the v0.3.1 binary (which has the
# Glorious Glitch) and runs the demo to show ordering divergence.
#
# Usage:
#   ./run-demo.sh                 # Start cluster, run demo, stop cluster
#   ./run-demo.sh --cluster-only  # Start cluster and leave it running
#
# Prerequisites:
#   - Node.js with npm
#   - The foxmq binary in bin/ (downloaded automatically if missing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="$SCRIPT_DIR/config"
BIN_DIR="$SCRIPT_DIR/bin"
LOG_DIR="$SCRIPT_DIR/logs"
FOXMQ_BIN="$BIN_DIR/foxmq"
DEMO_SCRIPT="$SCRIPT_DIR/ordering-bug-demo.js"

CLUSTER_ONLY=false
if [[ "${1:-}" == "--cluster-only" ]]; then
    CLUSTER_ONLY=true
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

# MQTT ports for each node
MQTT_PORTS=(1883 1884 1885 1886)
# Cluster (UDP) ports for each node
CLUSTER_PORTS=(19793 19794 19795 19796)
# Key indices
KEY_INDICES=(0 1 2 3)

NODE_PIDS=()

header() {
    echo ""
    echo -e "${BOLD}${CYAN}  🦊✨ ════════════════════════════════════════════════════════════ ✨🦊${RESET}"
    echo -e "${BOLD}${CYAN}  🦊✨   $1${RESET}"
    echo -e "${BOLD}${CYAN}  🦊✨ ════════════════════════════════════════════════════════════ ✨🦊${RESET}"
    echo ""
}

start_cluster() {
    mkdir -p "$LOG_DIR"

    echo "🚀 Starting 4-node FoxMQ cluster..."
    echo "   Binary: $FOXMQ_BIN"
    echo ""

    for i in 0 1 2 3; do
        local node_num=$((i + 1))
        local mqtt_port="${MQTT_PORTS[$i]}"
        local cluster_port="${CLUSTER_PORTS[$i]}"
        local key_idx="${KEY_INDICES[$i]}"
        local log_file="$LOG_DIR/node${node_num}.log"

        "$FOXMQ_BIN" run \
            --mqtt-addr "127.0.0.1:${mqtt_port}" \
            --cluster-addr "127.0.0.1:${cluster_port}" \
            --secret-key-file "${CONFIG_DIR}/key_${key_idx}.pem" \
            --cluster-cert "${CONFIG_DIR}/key_${key_idx}.crt" \
            --cluster-root-cert "${CONFIG_DIR}/ca.crt" \
            --cluster-accept-peer-with-cert \
            --allow-anonymous-login \
            "$CONFIG_DIR" \
            > "$log_file" 2>&1 &

        NODE_PIDS+=($!)
        echo "   🟢 Node ${node_num}: MQTT=:${mqtt_port}  Cluster=:${cluster_port}  PID=${NODE_PIDS[-1]}"
    done
    echo ""
}

stop_cluster() {
    if [ ${#NODE_PIDS[@]} -gt 0 ]; then
        echo "🛑 Stopping cluster (PIDs: ${NODE_PIDS[*]})..."
        for pid in "${NODE_PIDS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
        for pid in "${NODE_PIDS[@]}"; do
            wait "$pid" 2>/dev/null || true
        done
        NODE_PIDS=()
        echo "   Cluster stopped."
    fi
}

wait_for_cluster() {
    local max_attempts=30
    local attempt=0
    echo "⏳ Waiting for all 4 nodes to accept MQTT connections..."
    while [ $attempt -lt $max_attempts ]; do
        local all_up=true
        for port in "${MQTT_PORTS[@]}"; do
            if ! nc -z 127.0.0.1 "$port" 2>/dev/null; then
                all_up=false
                break
            fi
        done
        if [ "$all_up" = true ]; then
            echo "   ✅ All 4 MQTT ports are open."
            echo "   ⏳ Giving consensus 5 seconds to stabilize..."
            sleep 5
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    echo -e "   ${RED}💥 ERROR: Cluster did not become ready after ${max_attempts}s.${RESET}"
    echo "   Check logs in $LOG_DIR"
    return 1
}

if [ "$CLUSTER_ONLY" = false ]; then
    trap stop_cluster EXIT
fi

# ── Pre-flight checks ─────────────────────────────────────────────────

for port in "${MQTT_PORTS[@]}" "${CLUSTER_PORTS[@]}"; do
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then
        echo -e "${RED}🚫 ERROR: Port $port is already in use. Stop any existing foxmq processes first.${RESET}"
        exit 1
    fi
done

# Ensure binary
if [ ! -x "$FOXMQ_BIN" ]; then
    echo "📦 Downloading FoxMQ v0.3.1..."
    mkdir -p "$BIN_DIR"

    OS="$(uname -s)"
    case "$OS" in
        Darwin) ASSET="foxmq_0.3.1_macos-universal.zip" ;;
        Linux)  ASSET="foxmq_0.3.1_linux-amd64.zip" ;;
        *)      echo "Unsupported OS: $OS"; exit 1 ;;
    esac

    curl -L -o "$BIN_DIR/foxmq.zip" \
        "https://github.com/tashigit/foxmq/releases/download/v0.3.1/${ASSET}"
    unzip -o "$BIN_DIR/foxmq.zip" -d "$BIN_DIR"
    chmod +x "$FOXMQ_BIN"
    rm -f "$BIN_DIR/foxmq.zip"
fi

# Ensure npm deps
cd "$SCRIPT_DIR"
if [ ! -d node_modules ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# ── Start cluster and run demo ─────────────────────────────────────────

header "The Glorious Glitch — FoxMQ Message Ordering Demo"

echo -e "   ${YELLOW}⚠️  Using FoxMQ v0.3.1 which contains the Glorious Glitch:${RESET}"
echo -e "   ${YELLOW}   Each node dispatches its own publisher's messages immediately${RESET}"
echo -e "   ${YELLOW}   (before consensus), so subscribers on different nodes can${RESET}"
echo -e "   ${YELLOW}   observe different message orderings. 😱${RESET}"
echo ""

start_cluster
wait_for_cluster

if [ "$CLUSTER_ONLY" = true ]; then
    header "Cluster is running"
    echo "   📡 MQTT ports: ${MQTT_PORTS[*]}"
    echo "   🧪 Run the demo manually:  node ordering-bug-demo.js"
    echo "   🛑 Stop with:              kill ${NODE_PIDS[*]}"
    echo ""
    echo "   Press Ctrl-C to stop the cluster."
    trap stop_cluster INT
    wait
else
    header "Unleashing the Glorious Glitch..."
    node "$DEMO_SCRIPT" || true
fi
