/**
 * Test WebSocket Agent
 * Connects to the Go relay, registers, and handles dispatched tasks.
 *
 * Usage: node test-agent.js [name] [capabilities...]
 *   node test-agent.js Hermes bash,python,node
 *   node test-agent.js Alice windows,file-ops
 */

const WebSocket = require('ws');

const RELAY_URL = process.env.RELAY_URL || 'ws://localhost:8081/ws';
const agentName = process.argv[2] || 'TestAgent';
const capabilities = (process.argv[3] || 'bash,node').split(',');

console.log(`\n🤖 Agent: ${agentName}`);
console.log(`🔗 Connecting to: ${RELAY_URL}`);
console.log(`🎯 Capabilities: ${capabilities.join(', ')}`);
console.log('');

const ws = new WebSocket(RELAY_URL);

ws.on('open', () => {
  console.log('✅ WebSocket connected');

  // Register with the relay
  const registerMsg = {
    type: 'register',
    payload: {
      id: agentName.toLowerCase(),
      name: agentName,
      role: 'test-agent',
      capabilities: capabilities,
      endpoint: `http://localhost:${Math.floor(Math.random() * 10000 + 9000)}`,
      version: '0.1.0',
      metadata: {
        platform: process.platform,
        arch: process.arch,
      },
    },
  };

  ws.send(JSON.stringify(registerMsg));
  console.log('📤 Sent registration');
});

ws.on('message', (data) => {
  try {
    const msg = JSON.parse(data.toString());
    console.log(`\n📩 Received: ${msg.type}`);

    switch (msg.type) {
      case 'registered':
        console.log(`  ✅ Registered as: ${msg.payload?.status || 'ok'}`);
        console.log('  ⏳ Waiting for tasks...');
        break;

      case 'heartbeat_ack':
        // Normal heartbeat response
        break;

      case 'task_dispatch':
        handleTask(msg.payload);
        break;

      default:
        console.log(`  📦 Payload:`, JSON.stringify(msg.payload, null, 2).slice(0, 200));
    }
  } catch (err) {
    console.log('📩 Raw:', data.toString().slice(0, 100));
  }
});

ws.on('ping', () => {
  ws.pong();
});

ws.on('close', () => {
  console.log('\n❌ Connection closed');
  process.exit(0);
});

ws.on('error', (err) => {
  console.error('❌ Error:', err.message);
  process.exit(1);
});

function handleTask(task) {
  console.log(`  🎯 Task: ${task.title}`);
  console.log(`  🆔 ID: ${task.id}`);
  console.log(`  📝 Description: ${task.description || 'none'}`);
  console.log(`  ⭐ Priority: ${task.priority}`);
  console.log(`  🔧 Capability: ${task.capability}`);

  // Simulate processing
  console.log(`  ⚙️  Processing...`);
  setTimeout(() => {
    // Send task result back
    const resultMsg = {
      type: 'task_result',
      payload: {
        task_id: task.id,
        status: 'completed',
        result: `Task "${task.title}" completed successfully by ${agentName}`,
        completed_at: new Date().toISOString(),
      },
    };
    ws.send(JSON.stringify(resultMsg));
    console.log(`  ✅ Task completed!`);
    console.log(`  📤 Sent result back to relay`);
  }, 2000);
}

// Keep alive with periodic status updates
setInterval(() => {
  ws.send(JSON.stringify({
    type: 'heartbeat',
    payload: { timestamp: new Date().toISOString() },
  }));
}, 10000);

// Allow graceful shutdown
process.on('SIGINT', () => {
  console.log('\n👋 Disconnecting...');
  ws.close();
  process.exit(0);
});
