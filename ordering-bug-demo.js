/**
 * 🦊✨ The Glorious Glitch — FoxMQ Message Ordering Demo
 *
 * This script demonstrates the critical "Glorious Glitch" found in FoxMQ,
 * fixed in tashigit/foxmq#65, where consensus messages could be delivered
 * out of consensus order to local subscribers.
 *
 * THE GLORIOUS GLITCH:
 *   Before the fix, when a client published a message on a node with TCE active:
 *     1. The message was dispatched IMMEDIATELY to local subscribers (before consensus)
 *     2. The message was also sent to TCE for consensus ordering
 *     3. When consensus results arrived, the node SKIPPED re-dispatching its own
 *        messages (since they were already delivered locally)
 *
 *   This meant that a subscriber on Node 1 would see Node 1's messages
 *   interleaved at publish-time, while a subscriber on Node 2 would see those
 *   same messages at consensus-time — potentially in a completely different order.
 *
 * THE SCENARIO:
 *   - 4 FoxMQ nodes in a consensus cluster (ports 1883–1886)
 *   - 1 subscriber on each node, all listening to "ordering/demo"
 *   - Each node has its own publisher firing messages concurrently
 *   - We compare the message ordering observed by each subscriber
 *
 * EXPECTED RESULT (with glitch):
 *   Each node's subscriber sees that node's messages FIRST (dispatched locally
 *   before consensus), then the other nodes' messages in consensus order.
 *   This means all 4 subscribers see DIFFERENT orderings. 💀
 *
 * EXPECTED RESULT (with fix):
 *   All 4 subscribers see the exact same message order, as determined by consensus. ✅
 *
 * USAGE:
 *   Start a 4-node FoxMQ cluster, then run:
 *     node ordering-bug-demo.js
 */

import mqtt from "mqtt";

const TOPIC = "ordering/demo";
const MSGS_PER_NODE = 5;
const TIMEOUT_MS = 15000;

const NODES = [
  { name: "Node 1", id: "node1", port: 1883 },
  { name: "Node 2", id: "node2", port: 1884 },
  { name: "Node 3", id: "node3", port: 1885 },
  { name: "Node 4", id: "node4", port: 1886 },
];

const TOTAL_MESSAGES = NODES.length * MSGS_PER_NODE;

async function connectClient(port, clientId) {
  return await mqtt.connectAsync(`mqtt://127.0.0.1:${port}`, {
    protocolVersion: 5,
    clientId,
  });
}

function collectMessages(client, count) {
  const messages = [];
  return {
    messages,
    promise: new Promise((resolve) => {
      const timeout = setTimeout(() => resolve(messages), TIMEOUT_MS);

      client.on("message", (_topic, payload) => {
        messages.push(payload.toString());
        if (messages.length >= count) {
          clearTimeout(timeout);
          resolve(messages);
        }
      });
    }),
  };
}

async function main() {
  console.log("");
  console.log("  🦊✨ " + "=".repeat(58) + " ✨🦊");
  console.log("  🦊✨   The Glorious Glitch — FoxMQ Message Ordering Demo   ✨🦊");
  console.log("  🦊✨ " + "=".repeat(58) + " ✨🦊");
  console.log();

  // ── Connect subscribers to all 4 nodes ──────────────────────────────
  console.log("🔌 Connecting subscribers to all 4 nodes...");
  const subscribers = [];
  for (const node of NODES) {
    const client = await connectClient(node.port, `sub-${node.id}`);
    subscribers.push({ ...node, client });
    console.log(`   ✅ Subscriber connected to ${node.name} (port ${node.port})`);
  }

  // Subscribe and set up message collectors
  const collectors = [];
  for (const sub of subscribers) {
    await sub.client.subscribeAsync(TOPIC, { qos: 1 });
    collectors.push({
      name: sub.name,
      ...collectMessages(sub.client, TOTAL_MESSAGES),
    });
  }
  console.log(`   👂 All subscribers listening on "${TOPIC}"\n`);

  // ── Connect a publisher to each node ────────────────────────────────
  console.log("🔌 Connecting publishers (one per node)...");
  const publishers = [];
  for (const node of NODES) {
    const client = await connectClient(node.port, `pub-${node.id}`);
    publishers.push({ ...node, client });
  }
  console.log(`   ✅ 4 publishers ready\n`);

  // ── Publish concurrently from all 4 nodes ───────────────────────────
  console.log(
    `🚀 Publishing ${MSGS_PER_NODE} messages from each node concurrently (${TOTAL_MESSAGES} total)...`
  );

  await Promise.all(
    publishers.map(async (pub) => {
      for (let i = 1; i <= MSGS_PER_NODE; i++) {
        await pub.client.publishAsync(TOPIC, `${pub.name}-msg-${i}`, {
          qos: 1,
        });
      }
    })
  );

  console.log("📨 All messages published. Waiting for delivery...\n");

  // ── Collect results ─────────────────────────────────────────────────
  const results = [];
  for (const collector of collectors) {
    const messages = await collector.promise;
    results.push({ name: collector.name, messages });
  }

  // ── Print results ───────────────────────────────────────────────────
  console.log("─".repeat(70));
  console.log("  📋 RESULTS: Message ordering as seen by each subscriber");
  console.log("─".repeat(70));

  for (const result of results) {
    console.log(
      `\n  📡 ${result.name} subscriber received ${result.messages.length}/${TOTAL_MESSAGES} messages:`
    );
    result.messages.forEach((msg, i) => {
      // Highlight when a node sees its OWN messages (indicates local dispatch)
      const isLocal = msg.startsWith(result.name);
      const marker = isLocal ? " ⚡ LOCAL (skipped consensus!)" : "";
      console.log(`    ${String(i + 1).padStart(2)}. ${msg}${marker}`);
    });
  }

  // ── Analyze ordering consistency ────────────────────────────────────
  console.log("\n" + "─".repeat(70));
  console.log("  🔬 ANALYSIS");
  console.log("─".repeat(70));

  const referenceOrder = results[0].messages;
  let allMatch = true;
  const mismatches = [];

  for (let i = 1; i < results.length; i++) {
    const matches =
      results[i].messages.length === referenceOrder.length &&
      results[i].messages.every((msg, idx) => msg === referenceOrder[idx]);

    if (!matches) {
      allMatch = false;
      mismatches.push(results[i].name);
    }
  }

  if (allMatch && referenceOrder.length === TOTAL_MESSAGES) {
    console.log(
      `\n  ✅ PASS: All 4 nodes received all ${TOTAL_MESSAGES} messages in the SAME order.`
    );
    console.log(
      "  🎉 Total ordering is enforced — the fix is working correctly!\n"
    );
  } else if (!allMatch) {
    console.log(
      `\n  🚨💀 GLORIOUS GLITCH DETECTED: Nodes received messages in DIFFERENT orders!`
    );
    console.log(`  😱 Nodes with divergent ordering: ${mismatches.join(", ")}\n`);

    // Show a side-by-side comparison of the first diverging pair
    const divergent = results.find((r) => r.name === mismatches[0]);
    const maxLen = Math.max(referenceOrder.length, divergent.messages.length);
    console.log(
      `  ${"#".padStart(3)}  ${results[0].name.padEnd(20)} ${divergent.name.padEnd(20)} Match?`
    );
    console.log(`  ${"─".repeat(60)}`);
    for (let i = 0; i < maxLen; i++) {
      const a = referenceOrder[i] || "(missing)";
      const b = divergent.messages[i] || "(missing)";
      const match = a === b ? "  ✅" : "  💥";
      console.log(
        `  ${String(i + 1).padStart(3)}  ${a.padEnd(20)} ${b.padEnd(20)}${match}`
      );
    }

    console.log("\n  🔥 In a real system, this Glorious Glitch could cause:");
    console.log(
      "    💀 Inconsistent state across subscribers on different nodes"
    );
    console.log(
      "    💀 Race conditions in applications relying on message order"
    );
    console.log(
      "    💀 Retained messages resolving to different 'latest' values"
    );
  } else {
    console.log(
      `\n  ⚠️  WARNING: Only received ${referenceOrder.length}/${TOTAL_MESSAGES} messages.`
    );
    console.log("  The cluster may not be fully ready.\n");
  }

  console.log("\n  🦊✨ " + "=".repeat(58) + " ✨🦊");

  // Cleanup
  for (const pub of publishers) {
    await pub.client.endAsync();
  }
  for (const sub of subscribers) {
    await sub.client.endAsync();
  }
}

main().catch((err) => {
  console.error("💥 Demo failed:", err.message);
  process.exit(1);
});
