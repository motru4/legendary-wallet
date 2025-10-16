# 💸 Legendary Wallet 

High-load wallet system: a REST API for receiving operations and a background worker for asynchronous processing via Kafka, with transactional persistence in PostgreSQL and caching in Redis.
Special focus - **correctness under 1000 RPS per wallet**, strict ordering, and idempotency.

## 🔧 Tech Stack

![Go](https://img.shields.io/badge/Go-00ADD8?logo=go\&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql\&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka\&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker\&logoColor=white)
![Swagger](https://img.shields.io/badge/Swagger-85EA2D?logo=swagger\&logoColor=black)

---

## 📁 Repository Structure

```
│  .env                 
│  docker-compose.yml   
│  LICENSE
│
├─ migrations/          # SQL initialization of DB (initdb or migrator)
│    001_init.sql
│
├─ operation-worker/
│   ├─ cmd/
│   └─ internal/
│       ├─ app/ broker/ cache/ config/ database/ models/
│       ├─ repositories/
│       │   ├─ postgresrepo/
│       │   └─ redisrepo/
│       ├─ services/
│       └─ worker/
│
└─ wallet-service/
    ├─ cmd/
    ├─ docs/            # Swagger
    └─ internal/
        ├─ app/ broker/ cache/ config/ database/ models/
        ├─ repositories/
        │   ├─ kafkarepo/
        │   ├─ postgresrepo/
        │   └─ redisrepo/
        ├─ services/
        └─ transport/http
```

---

## 🚀 Quick Start

### 1) Configure environment variables

Create a `.env` file in the project root based on `config.env`.

### 2) Launch the system

```bash
docker compose up --build
```

---
## 🔍 Useful Dev Tools

| Tool                                                                                                          | URL                                                            | Description                                       |
| ------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------- |
| ![Swagger](https://img.shields.io/badge/Swagger-85EA2D?logo=swagger\&logoColor=black)         | [http://localhost:8080/swagger](http://localhost:8080/swagger) | REST API documentation                            |
| ![Kafka](https://img.shields.io/badge/Kafdrop-231F20?logo=apachekafka\&logoColor=white)        | [http://localhost:9000](http://localhost:9000)                 | Web UI for Kafka topics, partitions, and messages |
| ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql\&logoColor=white) | `localhost:5432`                                               | Primary transactional database                    |
| ![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis\&logoColor=white)                      | `localhost:6379`                                               | Cache and fast lookup store                       |              |

---

## 🌐 API

### Routes

```go
POST /api/v1/wallets                                      // create a new wallet
GET  /api/v1/wallets/{walletId}                           // get wallet balance
POST /api/v1/wallet                                       // create operation (DEPOSIT/WITHDRAW)
GET  /api/v1/wallets/{walletId}/operations/{operationId}  // get operation status

GET  /swagger/index.html                                  // Swagger UI
```

---

## 🧵 Processing Flow (Kafka) and Concurrency

* The number of Kafka **partitions** defines the number of consumer goroutines.
* Every **100ms**, the batcher collects accumulated messages and **groups them by `walletId`**.
* For each wallet, the service layer:

  1. Locks the wallet row using a `SELECT ... FOR UPDATE` inside a transaction.

  2. Fetches the list of operations from DB and **skips already processed ones** (idempotency, restart safety).

  3. Applies new operations **in Kafka order**:

     * `DEPOSIT` — increases balance
     * `WITHDRAW` — checks funds; if insufficient → marks as `FAILED` with reason

  4. Bulk updates operation statuses, updates the wallet balance, and commits the transaction.

  5. Updates the Redis cache and **commits the Kafka offset**.
* Kafka delivery is **at-least-once**. Combined with idempotency and status checks, it provides **domain-level exactly-once** behavior.

---

## 🧭 Architecture Diagram (Mermaid)

```mermaid
flowchart LR

  subgraph API["wallet-service"]
    A["REST API"]
  end

  subgraph KAFKA["Kafka"]
    K["Topic: wallet-operations<br/>key = walletId"]
  end

  subgraph WORKER["operation-worker"]
    W1["Consumers per partition"]
    W2["Batcher (every 5s)"]
    W3["Service layer · idempotent · ordered"]
    W1 --> W2
    W2["Batcher (every n ms)"] --> W3
  end

  subgraph STORAGE["Storage"]
    R["Redis cache"]
    P["PostgreSQL"]
  end

  %% --- Client calls ---
  U
  A
  U -->|"GET/POST"| A
  U["Users"]
  A

  %% --- Write path ---
  A --> K
  K --> W1

  W3
  P
  W3
  P
  W3 --> P
  W3["Service layer"] --> R

  %% --- Read path ---
  A --> R
  R --> P

  %% ----- Styles -----
  classDef cache fill:#8b5cf6,stroke:#4c1d95,color:#fff;
  classDef db fill:#e0f2fe,stroke:#0284c7,color:#0f172a;
  class R cache;
  class P db;
```

---



