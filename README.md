# 🦊✨ The Glorious Glitch — FoxMQ Message Ordering Demo

Demonstrates a critical message ordering glitch in [FoxMQ](https://github.com/tashigit/foxmq) v0.3.1, fixed in [PR #65](https://github.com/tashigit/foxmq/pull/65).

## 💀 The Glorious Glitch

FoxMQ uses the Tashi Consensus Engine (TCE) to provide **total message ordering** across a multi-node cluster. However, in v0.3.1, locally-published messages were dispatched to local subscribers **immediately** — before going through consensus — while remote messages arrived through the consensus pipeline. This meant:

- **Node 1's subscriber** saw Node 1's messages first (at publish-time), then remote messages (at consensus-time)
- **Node 2's subscriber** saw Node 2's messages first, then remote messages in consensus order
- 💥 **Result**: Different nodes observed different message orderings for the same set of messages

## ✅ The Fix

The fix ensures **all messages** (including locally-published ones) wait to come back through the consensus handler before being dispatched to any subscriber. This guarantees every subscriber on every node sees the identical total order.

## 🚀 Running the Demo

```bash
# Install dependencies
npm install

# Run the full demo (starts a 4-node cluster, runs the test, stops the cluster)
./run-demo.sh

# Or start the cluster and run the test separately
./run-demo.sh --cluster-only
# In another terminal:
node ordering-bug-demo.js
```

## 🔍 What to Expect

The demo connects a subscriber to each of the 4 nodes, then publishes 5 messages concurrently from each node (20 total). It compares the ordering observed by each subscriber:

```
  📡 Node 1 subscriber received 20/20 messages:
     1. Node 1-msg-1 ⚡ LOCAL (skipped consensus!)    ← dispatched before consensus
     2. Node 1-msg-2 ⚡ LOCAL (skipped consensus!)
     3. Node 1-msg-3 ⚡ LOCAL (skipped consensus!)
     4. Node 1-msg-4 ⚡ LOCAL (skipped consensus!)
     5. Node 1-msg-5 ⚡ LOCAL (skipped consensus!)
     6. Node 3-msg-1                                   ← now the consensus-ordered messages
     7. Node 3-msg-2
     ...

  📡 Node 2 subscriber received 20/20 messages:
     1. Node 2-msg-1 ⚡ LOCAL (skipped consensus!)    ← different node's messages first! 😱
     2. Node 2-msg-2 ⚡ LOCAL (skipped consensus!)
     ...
```

Every node sees its own messages first (positions 1–5), then other nodes' messages in consensus order — producing **4 different orderings** for the same 20 messages.

## 🔥 Impact

In a real system this Glorious Glitch could cause:
- 💀 **Inconsistent state** across subscribers on different nodes
- 💀 **Race conditions** in applications relying on message order
- 💀 **Retained messages** resolving to different "latest" values per node

## Prerequisites

- Node.js (v18+)
- macOS or Linux (the FoxMQ binary is downloaded automatically)
