# **IPCMessenger (TypeScript)**

[![Node.js Version](https://img.shields.io/badge/node.js-%3E%3D16.0.0-green.svg)](https://nodejs.org/)
[![TypeScript](https://img.shields.io/badge/typescript-%3E%3D4.5.0-blue.svg)](https://www.typescriptlang.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A lightweight, high-performance asynchronous Inter-Process Communication (IPC) library written in TypeScript for Node.js microservices.

Rather than replacing message brokers, **IPCMessenger structures and standardizes how you use them**. It combines the distinct strengths of **RabbitMQ** and **Redis Pub/Sub** into a single, strongly-typed interface that delivers low-latency, point-to-point **Request-Response** messaging with zero boilerplate.

---

## 💡 **Why IPCMessenger?**

### **The Problem with Raw Message Brokers**
Implementing a robust **Request-Response (RPC)** pattern between decoupled microservices using native brokers requires substantial infrastructure boilerplate:
* **RabbitMQ Complexity:** To receive a response asynchronously, the requesting service must declare a temporary, exclusive callback queue, generate a unique correlation ID, attach headers, and maintain consumer listeners. If a process crashes, orphaned queues can leak resources on the broker.
* **Redis Pub/Sub Limitations:** While Redis Pub/Sub is extremely fast for message delivery, it lacks native queue persistence, worker load balancing, and competing consumer management out of the box.

### **The IPCMessenger Solution: A Best-of-Both-Worlds Architecture**
IPCMessenger abstracts these broker mechanics away by combining them into a standardized hybrid pattern:

1. **RabbitMQ for Workload Distribution & Durability:** Used for distributing incoming requests across competing consumer queues. If your responder microservices scale up or temporarily go offline, RabbitMQ reliably queues and load-balances work without dropping messages.
2. **Redis Pub/Sub for Low-Latency Responses:** Used exclusively for routing responses directly back to the specific requesting process in-memory. This delivers high-speed, point-to-point delivery without the overhead of creating, monitoring, and destroying temporary RabbitMQ queues.

---

## 🌟 **Key Advantages**

* 🛡️ **End-to-End Type Safety:** Leverage full TypeScript generics for strongly typed request payloads and response outputs across microservices.
* ⚡ **High Performance & Low Latency:** Bypasses RabbitMQ's disk/queue lifecycle overhead on the response path by utilizing Redis's ultra-fast in-memory Pub/Sub mechanism.
* 🔓 **Complete Microservice Decoupling:** Requesters and Responders operate independently without needing direct network visibility or explicit RPC queue setup.
* 🧹 **Zero Queue Leaks & Reduced Broker Load:** Eliminates the classic RabbitMQ RPC anti-pattern of creating and tearing down exclusive temporary reply queues for every request.
* 🚀 **Accelerated Developer Velocity:** Replaces 50+ lines of low-level `amqplib` connection, exchange, channel, correlation ID, and header handling with a clean async/await interface.
* ⚠️ **Transparent Error Propagation:** Exceptions raised inside remote worker callbacks are automatically captured, serialized, and re-raised locally as a `RemoteServiceError` on the requester side.
* 📈 **Effortless Horizontal Scaling:** Scale responder instances up or down seamlessly with built-in RabbitMQ competing-consumer load balancing.

---

## 📋 **Prerequisites & Dependencies**

### **Prerequisites**
* **Node.js**: v16.0.0 or higher
* **TypeScript**: v4.5.0 or higher
* **RabbitMQ**: An accessible RabbitMQ broker instance (e.g., `amqp://guest:guest@localhost:5672/`)
* **Redis**: An accessible Redis server instance (e.g., `redis://localhost:6379/0`)

### **Installation**

Install the required runtime dependencies and ambient TypeScript declaration types:

```bash
npm install amqplib ioredis uuid
npm install --save-dev typescript @types/node @types/amqplib @types/uuid
```

---

## 📂 **Project Structure**

When integrating `IPCMessenger` into a TypeScript project, structure your codebase as follows:

```text
your_project/
│
├── src/
│   ├── ipc_messenger/
│   │   ├── index.ts          # Main IPCMessenger orchestrator & custom errors
│   │   ├── mqHandler.ts      # RabbitMQ connection & consumer handler
│   │   └── cachePubSub.ts    # Redis Pub/Sub response listener
│   │
│   ├── serviceA.ts           # Requester Service (API Gateway / Client)
│   └── serviceB.ts           # Responder Service (Worker / Microservice)
│
├── tsconfig.json
└── package.json
```

---

## 💻 **Implementation Guide**

### **1. Shared Types (`types.ts`)**

Define strongly-typed interfaces for communication between services:

```typescript
export interface OrderPayload {
  orderId: number;
  action: 'validate' | 'cancel';
}

export interface OrderResult {
  status: 'verified' | 'rejected';
  timestamp: string;
}
```

---

### **2. The Requester (`serviceA.ts`)**

This service sends requests to a RabbitMQ task queue and awaits a typed reply via Redis Pub/Sub.

```typescript
import { IPCMessenger, RemoteServiceError, TimeoutError } from './ipc_messenger';
import { OrderPayload, OrderResult } from './types';

async function main(): Promise<void> {
  const ipc = new IPCMessenger(
    'amqp://guest:guest@localhost/',
    'redis://localhost:6379/0'
  );

  await ipc.connect();

  try {
    console.log("Sending request to 'order_processing'...");
    
    const payload: OrderPayload = { orderId: 123, action: 'validate' };

    // Pass target response & request payload types as generics
    const response = await ipc.sendRequest<OrderResult, OrderPayload>(
      'order_processing',
      payload,
      10 // timeout in seconds
    );

    console.log('Server Response:', response.status, 'at', response.timestamp);

  } catch (err: unknown) {
    if (err instanceof RemoteServiceError) {
      console.error(`Remote service failed: ${err.message}`);
    } else if (err instanceof TimeoutError) {
      console.error('Request timed out.');
    } else {
      console.error('Unexpected error:', err);
    }
  } finally {
    await ipc.close();
  }
}

main();
```

---

### **3. The Responder (`serviceB.ts`)**

This service listens on a RabbitMQ queue, processes the typed payload, and publishes the result back through Redis.

```typescript
import { IPCMessenger } from './ipc_messenger';
import { OrderPayload, OrderResult } from './types';

async function processTask(data: OrderPayload): Promise<OrderResult> {
  console.log('Processing order:', data.orderId);

  if (data.orderId === 0) {
    throw new Error('Invalid order ID!');
  }

  // Simulate async operation
  await new Promise((resolve) => setTimeout(resolve, 500));

  return {
    status: 'verified',
    timestamp: new Date().toISOString()
  };
}

async function main(): Promise<void> {
  const ipc = new IPCMessenger(
    'amqp://guest:guest@localhost/',
    'redis://localhost:6379/0'
  );

  await ipc.connect();

  console.log("Worker listening on queue 'order_processing'...");
  
  // Strongly-typed message consumer
  await ipc.startListening<OrderPayload, OrderResult>(
    'order_processing',
    processTask
  );
}

main();
```

---

## 🔄 **Technical Flow**

```text
[ Requester Service ]                                    [ Responder Worker ]
         │                                                        │
         ├── 1. Generate unique channel UUID ─────────────────────┤
         │     (e.g., response_channel:xyz)                       │
         │                                                        │
         ├── 2. Subscribe to Redis channel: response_channel:xyz  │
         │                                                        │
         ├── 3. Publish payload + channel UUID to RabbitMQ ──────►│
         │                                                        ├── 4. Consume from queue
         │                                                        ├── 5. Execute processCallback(data)
         │                                                        │
         │◄── 6. Publish result/error to Redis ───────────────────┤
         │       (channel: response_channel:xyz)                  │
         │                                                        │
         ├── 7. Unsubscribe from Redis & resolve Promise ─────────┘
```

---

## ⚙️ **API Reference**

### **`new IPCMessenger(rabbitmqUrl: string, redisUrl: string)`**
Instantiates a new messenger instance with RabbitMQ and Redis connection parameters.

---

### **`await connect(): Promise<void>`**
Establishes connection pools to both RabbitMQ and Redis asynchronously.

---

### **`await sendRequest<TResponse, TPayload>(queueName, payload, timeout?): Promise<TResponse>`**
* **`queueName`** (`string`): Target RabbitMQ task queue name.
* **`payload`** (`TPayload`): JSON-serializable request payload.
* **`timeout`** (`number`, optional): Maximum wait time in seconds (default: `10`).
* **Returns**: `Promise<TResponse>` containing the typed worker response.

---

### **`await startListening<TInput, TOutput>(queueName, processCallback): Promise<void>`**
* **`queueName`** (`string`): Target RabbitMQ queue to consume from.
* **`processCallback`** (`(data: TInput) => Promise<TOutput> | TOutput`): Callback executing worker business logic. Can return a result or throw an error.

---

### **`await close(): Promise<void>`**
Gracefully tears down consumers, unsubscribes Redis listeners, and closes connection pools.

---

## ⚠️ **Error Handling & Exception Propagation**

Uncaught exceptions thrown inside the responder callback `processCallback` are captured automatically, serialized into a structured payload, and routed back to the requester where they are re-thrown as a **`RemoteServiceError`**:

```typescript
try {
  const result = await ipc.sendRequest('order_processing', payload);
} catch (err) {
  if (err instanceof RemoteServiceError) {
    // Contains original remote error name and message
    console.error(`Remote worker error: ${err.message}`);
  }
}
```

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.