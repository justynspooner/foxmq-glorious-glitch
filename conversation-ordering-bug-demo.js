/**
 * 🦊✨ The Glorious Glitch — Mid-Conversation Edition
 *
 * This demo proves that the FoxMQ ordering glitch (tashigit/foxmq#65) is NOT a
 * startup-only artifact. It can corrupt message ordering at ANY point in a
 * cluster's life — deep into a long, otherwise-perfectly-ordered conversation.
 *
 * WHY THE BUG ISN'T A STARTUP THING:
 *   The glitch is structural, not a warm-up race. On EVERY publish, the node:
 *     1. Dispatches the message to its LOCAL subscribers immediately (publish-time)
 *     2. Sends it to TCE for consensus ordering
 *     3. SKIPS re-dispatching its own message when consensus results arrive
 *
 *   So a node's own subscriber sees that node's messages at PUBLISH-time, while
 *   every other node sees them at CONSENSUS-time. This shortcut applies forever.
 *
 *   Divergence becomes VISIBLE only when two different nodes publish
 *   concurrently — and that can happen at message #1 or message #10,000.
 *
 * THE SCENARIO — a 4-way chat room:
 *   - 4 FoxMQ nodes (ports 1883–1886), one chat participant per node:
 *       Alice (node1), Bob (node2), Carol (node3), Dave (node4)
 *   - A subscriber on each node records the transcript IT observes.
 *   - The conversation runs as a series of turns:
 *       • Most turns are SINGLE-SPEAKER and well-spaced. With only one message
 *         in flight at a time, every node observes the identical order. ✅
 *       • At a couple of points DEEP into the conversation, two participants
 *         "talk over each other" (CROSSTALK) — publishing concurrently from
 *         their respective nodes.
 *
 * EXPECTED RESULT (with the glitch, FoxMQ v0.3.1):
 *   The first stretch of the transcript is byte-for-byte identical on all 4
 *   nodes. Then, at the first crosstalk turn — well past startup — the
 *   transcripts FORK: the node that spoke sees its own line first, so different
 *   nodes record a different conversation from that line onward. 💀
 *
 * EXPECTED RESULT (with the fix):
 *   Every line — including the crosstalk lines — goes through consensus before
 *   delivery, so all 4 transcripts stay identical from start to finish. ✅
 *
 * USAGE:
 *   Start a 4-node FoxMQ cluster, then run:
 *     ./run-demo.sh --cluster-only      # in one terminal
 *     node conversation-ordering-bug-demo.js   # in another
 */

import mqtt from "mqtt";

const TOPIC = "chat/room";
const TIMEOUT_MS = 20000;
// Quiet gap between single-speaker turns. Long enough for a lone message to be
// delivered to ALL subscribers (via consensus) before the next turn begins,
// so orderly turns can't race each other.
const TURN_GAP_MS = 450;
// Pause after a crosstalk burst to let both messages settle everywhere.
const SETTLE_MS = 800;

const NODES = [
  { name: "Alice", id: "node1", port: 1883 },
  { name: "Bob", id: "node2", port: 1884 },
  { name: "Carol", id: "node3", port: 1885 },
  { name: "Dave", id: "node4", port: 1886 },
];

// The conversation. Each turn is either:
//   { speaker: <idx>, text }                       → single speaker, ordered
//   { crosstalk: [<idxA>, <idxB>], texts: [a, b] } → two nodes publish at once
//
// Note the long orderly run BEFORE the first crosstalk: this is the whole
// point — the bug strikes mid-conversation, not at startup.
const SCRIPT = [
  { speaker: 0, text: "Morning all — kicking off the deploy checklist." },
  { speaker: 1, text: "Morning! I've got the migration ready to go." },
  { speaker: 2, text: "Nice. I'll watch the dashboards." },
  { speaker: 3, text: "I'll handle the rollback lever if we need it." },
  { speaker: 0, text: "Step 1: drain traffic from the old nodes." },
  { speaker: 1, text: "Draining now... old nodes at 0 connections." },
  { speaker: 2, text: "Dashboards look calm. Error rate flat." },
  { speaker: 0, text: "Step 2: run the migration." },
  { speaker: 1, text: "Running migration... applying schema changes." },
  { speaker: 1, text: "Migration complete. No errors." },
  { speaker: 2, text: "Confirmed — new tables are populating." },
  { speaker: 0, text: "Step 3: bring the new nodes into rotation." },
  // ── First CROSSTALK, ~13 lines in — long past startup ──────────────────
  // Bob and Carol react at the exact same instant.
  {
    crosstalk: [1, 2],
    texts: [
      "Bob: WAIT — hold the rotation, I'm seeing replica lag!",
      "Carol: All green on my side, rotating the new nodes IN now.",
    ],
  },
  { speaker: 3, text: "...so are we rotating in or holding? These conflict." },
  { speaker: 0, text: "Let's pause. Carol, Bob — what did each of you see first?" },
  { speaker: 1, text: "I saw my 'hold' message before Carol's 'rotate'." },
  { speaker: 2, text: "And I saw my 'rotate' before Bob's 'hold'. Uh oh." },
  { speaker: 0, text: "Right. That's the ordering glitch. Continuing for now." },
  { speaker: 3, text: "Rollback lever still armed, just in case." },
  { speaker: 0, text: "Step 4: verify checksums on the new nodes." },
  { speaker: 1, text: "Replica lag cleared. Checksums match." },
  { speaker: 2, text: "Same here — checksums match." },
  // ── Second CROSSTALK, even later ───────────────────────────────────────
  {
    crosstalk: [3, 0],
    texts: [
      "Dave: Pulling the rollback lever — saw a checksum mismatch!",
      "Alice: Marking the deploy DONE — all checksums verified.",
    ],
  },
  { speaker: 1, text: "...did we just roll back AND mark done at the same time?" },
  { speaker: 2, text: "On my node it says rollback came first." },
  { speaker: 3, text: "On mine it says 'done' came first. Same glitch again." },
  { speaker: 0, text: "Yep. Two different truths, line for line. Classic." },
];

// Total messages each subscriber should eventually receive.
const TOTAL_MESSAGES = SCRIPT.reduce(
  (n, turn) => n + (turn.crosstalk ? turn.crosstalk.length : 1),
  0
);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function connectClient(port, clientId) {
  return await mqtt.connectAsync(`mqtt://127.0.0.1:${port}`, {
    protocolVersion: 5,
    clientId,
  });
}

function collectMessages(client, count) {
  const messages = [];
  let resolveFn;
  const promise = new Promise((resolve) => {
    resolveFn = resolve;
    const timeout = setTimeout(() => resolve(messages), TIMEOUT_MS);
    client.on("message", (_topic, payload) => {
      messages.push(payload.toString());
      if (messages.length >= count) {
        clearTimeout(timeout);
        resolve(messages);
      }
    });
  });
  return { messages, promise };
}

async function main() {
  console.log("");
  console.log("  🦊✨ " + "=".repeat(58) + " ✨🦊");
  console.log("  🦊✨   The Glorious Glitch — Mid-Conversation Edition      ✨🦊");
  console.log("  🦊✨ " + "=".repeat(58) + " ✨🦊");
  console.log();
  console.log(
    "  Proving the ordering bug strikes deep into a conversation, not just at startup.\n"
  );

  // ── Connect a subscriber to every node ──────────────────────────────
  console.log("🔌 Connecting one subscriber to each node...");
  const subscribers = [];
  for (const node of NODES) {
    const client = await connectClient(node.port, `chat-sub-${node.id}`);
    subscribers.push({ ...node, client });
    console.log(`   ✅ ${node.name}'s subscriber connected (port ${node.port})`);
  }

  const collectors = [];
  for (const sub of subscribers) {
    await sub.client.subscribeAsync(TOPIC, { qos: 1 });
    collectors.push({ name: sub.name, ...collectMessages(sub.client, TOTAL_MESSAGES) });
  }
  console.log(`   👂 All subscribers recording the transcript on "${TOPIC}"\n`);

  // ── Connect a publisher to each node ────────────────────────────────
  console.log("🔌 Connecting one publisher per node...");
  const publishers = [];
  for (const node of NODES) {
    const client = await connectClient(node.port, `chat-pub-${node.id}`);
    publishers.push({ ...node, client });
  }
  console.log(`   ✅ 4 participants ready: ${NODES.map((n) => n.name).join(", ")}\n`);

  // ── Run the conversation, turn by turn ──────────────────────────────
  console.log(`💬 Starting the conversation (${SCRIPT.length} turns, ${TOTAL_MESSAGES} lines)...\n`);

  let line = 0;
  for (let t = 0; t < SCRIPT.length; t++) {
    const turn = SCRIPT[t];

    if (turn.crosstalk) {
      const [a, b] = turn.crosstalk;
      console.log(
        `   💥 CROSSTALK (turn ${t + 1}): ${NODES[a].name} and ${NODES[b].name} publish at the SAME instant`
      );
      // Fire both publishes concurrently from their respective nodes. This is
      // the only thing that differs from an orderly turn — and it's enough.
      await Promise.all([
        publishers[a].client.publishAsync(TOPIC, `${String(++line).padStart(2, "0")}│ ${turn.texts[0]}`, { qos: 1 }),
        publishers[b].client.publishAsync(TOPIC, `${String(++line).padStart(2, "0")}│ ${turn.texts[1]}`, { qos: 1 }),
      ]);
      await sleep(SETTLE_MS);
    } else {
      const speaker = NODES[turn.speaker];
      await publishers[turn.speaker].client.publishAsync(
        TOPIC,
        `${String(++line).padStart(2, "0")}│ ${speaker.name}: ${turn.text}`,
        { qos: 1 }
      );
      // Quiet gap → only one message in flight → no race → consistent order.
      await sleep(TURN_GAP_MS);
    }
  }

  console.log("\n📨 Conversation finished. Collecting each node's transcript...\n");

  const results = [];
  for (const collector of collectors) {
    const messages = await collector.promise;
    results.push({ name: collector.name, messages });
  }

  // ── Find the first line where the transcripts disagree ──────────────
  const reference = results[0].messages;
  const minLen = Math.min(...results.map((r) => r.messages.length));
  let divergeAt = -1;
  for (let i = 0; i < minLen; i++) {
    const baseline = reference[i];
    if (results.some((r) => r.messages[i] !== baseline)) {
      divergeAt = i;
      break;
    }
  }

  const allSameLength = results.every((r) => r.messages.length === TOTAL_MESSAGES);

  // ── Report ──────────────────────────────────────────────────────────
  console.log("─".repeat(72));
  console.log("  🔬 ANALYSIS");
  console.log("─".repeat(72));

  for (const r of results) {
    console.log(`   📡 ${r.name}'s node recorded ${r.messages.length}/${TOTAL_MESSAGES} lines`);
  }
  console.log();

  if (divergeAt === -1 && allSameLength) {
    console.log("  ✅ PASS: All 4 nodes recorded the EXACT same transcript, start to finish.");
    console.log("  🎉 Total ordering held even through crosstalk — the fix is working!\n");
  } else if (divergeAt === -1) {
    console.log(`  ⚠️  Transcripts agree so far but counts differ — cluster may not be fully ready.`);
    console.log("     Try giving consensus more time to stabilize before running.\n");
  } else {
    console.log(
      `  ✅ The first ${divergeAt} line(s) are IDENTICAL on all 4 nodes — the conversation`
    );
    console.log(`     was perfectly ordered through startup and well into the chat.`);
    console.log();
    console.log(
      `  🚨💀 GLORIOUS GLITCH: transcripts FORK at line ${divergeAt + 1} — mid-conversation,`
    );
    console.log(`     long after startup. From here, no two nodes fully agree.\n`);

    // Show the lines around the divergence point, node by node.
    const from = Math.max(0, divergeAt - 2);
    const to = Math.min(minLen, divergeAt + 4);
    for (let i = from; i < to; i++) {
      const baseline = reference[i];
      const agree = results.every((r) => r.messages[i] === baseline);
      const tag = i === divergeAt ? "  👈 FORK" : agree ? "  ✅" : "  💥";
      console.log(`  ${i < divergeAt ? "✅" : "  "} line ${String(i + 1).padStart(2)}${tag}`);
      for (const r of results) {
        console.log(`        ${r.name.padEnd(6)} ▸ ${r.messages[i] ?? "(missing)"}`);
      }
      console.log();
    }

    console.log("  🔥 In a real system this means:");
    console.log("    💀 'rollback' and 'done' applied in opposite orders on different nodes");
    console.log("    💀 Two operators reading the same channel see two different histories");
    console.log("    💀 Any state machine driven by this stream diverges permanently\n");
  }

  console.log("  🦊✨ " + "=".repeat(58) + " ✨🦊\n");

  for (const pub of publishers) await pub.client.endAsync();
  for (const sub of subscribers) await sub.client.endAsync();
}

main().catch((err) => {
  console.error("💥 Demo failed:", err.message);
  process.exit(1);
});
