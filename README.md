# AetherVec

<p align="center">
  <i>A high-performance, embedded vector database written in Rust.</i>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Rust-1.75+-orange.svg" alt="Rust Version">
  <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/Status-Work_In_Progress-yellow.svg" alt="Status">
</p>

---

## Overview

AetherVec is a lightweight, embeddable vector database designed for applications requiring fast approximate nearest neighbor (ANN) search. Built entirely in Rust, it emphasizes:

-   **High Performance**: LSM-tree inspired architecture optimized for writes.
-   **Durability**: Write-Ahead Logging (WAL) ensures data safety.
-   **Embeddable**: No separate server process required; link directly into your application.
-   **Pure Rust**: No external dependencies like a C runtime for core functionality.

---

## Architecture

```mermaid
graph TD
    subgraph "Client Interface"
        A["AetherDB"]
    end

    subgraph "Collection Layer"
        B["CollectionManager"]
        C["Collection 1"]
        D["Collection N"]
    end

    subgraph "Memory Layer"
        E["Active MemTable<br/>(HNSW / IVF / Flat)"]
        F["Frozen MemTables"]
    end

    subgraph "Durability Layer"
        G["WalManager<br/>(Per-Collection WAL)"]
    end

    subgraph "Background Jobs"
        H["CompactionManager<br/>(Multi-Lane Executor)"]
        I["Worker Pool"]
    end

    subgraph "Disk Layer"
        J["Segments<br/>(SSTable-like Files)"]
    end

    A --> B
    B --> C
    B --> D
    C --> E
    C --> G
    E -- "freeze on full" --> F
    F -- "dispatch task" --> H
    H --> I
    I -- "flush to disk" --> J
    G -- "sync_data()" --> K["OS / Disk"]

    style A fill:#4A90D9,stroke:#333,color:#fff
    style H fill:#7B68EE,stroke:#333,color:#fff
    style J fill:#32CD32,stroke:#333,color:#fff
```

---

## Core Components

| Component | Description |
| :--- | :--- |
| **`AetherDB`** | Main entry point. Manages database lifecycle and collection access. |
| **`Collection`** | A named namespace for vectors. Holds a MemTable and a list of frozen MemTables. |
| **`MemTable`** | In-memory index (Flat, HNSW, or IVF). Mutates on writes. |
| **`WalManager`** | Per-collection Write-Ahead Log for crash recovery. Uses `BufWriter` + `sync_data`. |
| **`CompactionManager`** | Background flush orchestrator using a **Multi-Lane Executor** model. Guarantees per-collection task ordering while allowing cross-collection parallelism. |

---

## Quick Start

```rust
use aethervec::{AetherDB, IndexConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open or create a database
    let db = AetherDB::new("./my_data")?;

    // 2. Create a collection with HNSW index
    let config = IndexConfig::new_with_default_config("hnsw")?;
    let collection = db.create_collection("my_vectors", 128, "cosine", config)?;

    // 3. Upsert a document
    let mut coll = collection.write().unwrap();
    coll.upsert(Document::new(vec![0.1; 128], "my-doc".to_string()))?;

    // 4. Search
    let results = coll.search(&vec![0.1; 128], 10);
    
    Ok(())
}
```

---

## Roadmap

-   [x] Core `AetherDB`, `Collection`, `Document` API
-   [x] Write-Ahead Log (WAL) with `sync_data`
-   [x] Multi-Lane Executor for background compaction (GPM-inspired)
-   [ ] On-disk Segment format (SSTable-like)
-   [ ] HNSW / IVF index implementation
-   [ ] Disk-based ANN search (mmap / DiskANN-inspired)
-   [ ] Server mode (gRPC / REST)

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
