# SimpleKafka: Distributed Commit-Log Messaging Engine

A distributed, partitioned, append-only commit-log system built from scratch in Java using Java NIO, a custom binary wire protocol, and Apache ZooKeeper for cluster consensus and leader failover.

---

## Architecture Overview

## Core System Components

### 1. Custom Binary Wire Protocol
- Replaces high-overhead text formats (JSON/HTTP) with length-prefixed, big-endian `ByteBuffer` serialization.
- Packet framing uses a 1-byte magic identifier for request/response dispatching:
    - `0x01`: `PRODUCE`
    - `0x02`: `FETCH`
    - `0x03`: `METADATA`
    - `0x04`: `CREATE_TOPIC`
    - `0x05`: `REPLICATE`

### 2. High-Performance Segmented Storage
- **Append-Only Log Segments:** Messages are committed sequentially using `FileChannel` to maximize sequential disk I/O throughput.
- **Sparse Indexing:** Fixed 16-byte index entries (`[8B Offset][8B File Position]`) enable $O(\log N)$ binary search lookup without loading entire logs into memory.
- **Durability & Rollover:** Enforces disk flushes (`FileChannel.force(true)`) and automatically rolls over active logs when segments exceed size boundaries (e.g., 1MB).

### 3. Cluster Coordination & High Availability (ZooKeeper)
- **Broker Registration & Liveness:** Brokers maintain ephemeral znodes (`/brokers/ids/{id}`); sudden network drops or crashes trigger automated node deletion.
- **Dynamic Controller Election:** Brokers race to acquire the `/controller` ephemeral lock; when the active controller dies, followers re-elect a new cluster coordinator within milliseconds.
- **Partition Leadership & Rebalancing:** The controller assigns partition leaders/followers and automatically reassigns leadership on node dropouts.

### 4. Client APIs
- **SimpleKafkaProducer:** Dispatches records across topic partitions using round-robin and key-based distribution.
- **SimpleKafkaConsumer:** Continuous polling daemon with explicit offset seeking (`seek(offset)`) and local offset tracking.

---

## Directory Structure

```text
build-your-own-kafka/
├── data/                                # Local broker segment directories (ignored by git)
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── example/
│   │               └── SimpleKafka/
│   │                   ├── EmbeddedZooKeeper.java      # In-memory ZK runner for testing
│   │                   ├── Broker/
│   │                   │   ├── BrokerInfo.java
│   │                   │   ├── Partition.java          # Segmented log & sparse index engine
│   │                   │   ├── Protocol.java           # Binary serialization / frame parser
│   │                   │   ├── SimpleKafkaBroker.java  # NIO TCP server & controller logic
│   │                   │   └── ZookeeperClient.java    # Cluster topology watcher
│   │                   └── Client/
│   │                       ├── SimpleKafkaClient.java
│   │                       ├── SimpleKafkaProducer.java
│   │                       └── SimpleKafkaConsumer.java
│   └── test/
└── pom.xml
```

---

## Getting Started

### Prerequisites
- **Java 17+**
- **Maven 3.8+**

### 1. Build the Project
```bash
mvn clean package -DskipTests
```
### 2. Start the Cluster
Open separate terminal windows for each process:

Terminal 1: Start ZooKeeper

````
Bash
mvn exec:java -Dexec.mainClass="com.example.SimpleKafka.EmbeddedZooKeeper"
````

Terminals 2, 3, 4: Start 3 Broker Instances
````
Bash
# Broker 1
mvn exec:java -Dexec.mainClass="com.example.SimpleKafka.Broker.SimpleKafkaBroker" -Dexec.args="1 localhost 9091 2181"

# Broker 2
mvn exec:java -Dexec.mainClass="com.example.SimpleKafka.Broker.SimpleKafkaBroker" -Dexec.args="2 localhost 9092 2181"

# Broker 3
mvn exec:java -Dexec.mainClass="com.example.SimpleKafka.Broker.SimpleKafkaBroker" -Dexec.args="3 localhost 9093 2181"
````

### 3. Produce and Consume Messages
Terminal 5: Start Consumer
````
Bash
mvn exec:java -Dexec.mainClass="com.example.SimpleKafka.Client.SimpleKafkaConsumer" -Dexec.args="localhost 9091 test-topic 0"
Terminal 6: Produce Messages

Bash
mvn exec:java -Dexec.mainClass="com.example.SimpleKafka.Client.SimpleKafkaProducer" -Dexec.args="localhost 9091
````

---

## Fault Tolerance & Verification
To verify leader failover:

### 1. Terminate Broker 1 (Ctrl + C).

### 2. Observe ZooKeeper triggering a controller election on Broker 2.

### 3. Partition 0 leadership reassigns dynamically without interrupting the consumer.