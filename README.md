<div align="center">

<img src="https://github.com/Litiaina/litiaina-admin-webpage/blob/master/images/litiaina_icon.png?raw=true" alt="Litiaina Icon" width="150" height="150" style="vertical-align: middle; margin-right: 10px;"/>

# Litiaina N1

**A high-performance single-node data engine optimized for structured storage, offering unified file system and object storage capabilities.**

</div>

---

**Litiaina N1** is a **structured data engine** designed for **performance**, **efficiency**, and **data integrity** on a **single server**.  
It bridges two storage paradigms — a **hierarchical file system** and an **object-based data layer** — providing flexibility to manage both structured and semi-structured data with unified access semantics.

---

## ✨ Core Features

> 🗂️ **Unified File & Object Storage**  
> - Combines **POSIX-style file system** semantics with **Custom object APIs**.  
> - Ideal for **application backends**, **local archives**, or **edge deployments** needing dual access patterns.  
>  
> ⚙️ **Optimized Structured Data Handling**  
> - Designed for **high-throughput reads/writes** and **transaction-safe metadata** updates.  
> - Perfect for databases, logs, analytics dumps, and machine-generated data.  
>  
> 💾 **Atomic Write and Metadata Operations**  
> - Ensures strong consistency and reliability across operations.  
> - Includes **journaling** and **safe checkpointing** to prevent corruption.  
>  
> 🗜️ **Transparent Compression Pipeline**  
> - Reduces disk usage while maintaining logical consistency.  
> - Supports inline and deferred compression modes.  
>  
> 🔐 **Authentication & Access Control**  
> - Supports **local credentials**, **API tokens**, and **integrated key stores**.  
>  
> 📊 **Storage Quotas & Resource Limits**  
> - Enforces per-user or per-directory capacity limits.  
> - Includes real-time usage tracking and reporting.  
>  
> 🌐 **WebDAV & REST Access Layers**  
> - Mount the N1 server as a **network drive** or interact via RESTful APIs.  
>  
> 🔗 **Local Object Gateway**  
> - Access stored objects via signed URLs or local tokenized links.  
>  
> 🧩 **Plugin-Ready Module System**  
> - Extend the engine with **custom analytics**, **data processors**, or **backup hooks**.  

---

## 🧠 Architecture Overview

> **Litiaina N1** runs entirely on a single node, prioritizing **structured storage**, **low latency**, and **system simplicity**.  
> It provides consistent performance without requiring clustering or distributed coordination.

### 🗂️ 1. Hierarchical File System Mode
- Traditional directory-based access  
- Optimized for **local workloads** and **low-latency file I/O**  
- Supports metadata caching and atomic updates  

### ☁️ 2. Local Object Storage Mode
- Exposes objects via **REST or S3-compatible APIs**  
- Ideal for **application binaries**, **data exports**, and **programmatic access**  
- Maintains metadata mappings internally for fast lookups  

🧩 *Both modes share authentication, quota, and compression pipelines for unified control.*

---

## 🧩 Data Flow Summary

1. **Ingestion Layer** — receives data via file writes, HTTP uploads, or API requests  
2. **Storage Layer** — manages structured data blocks with atomic write guarantees  
3. **Compression & Indexing** — applies transparent data compression and metadata tagging  
4. **Access Layer** — exposes unified FS + object interfaces  
5. **Policy Engine** — enforces quotas, user permissions, and retention rules  

---

## 🧩 Typical Use Cases

> - 🧠 Developer or research server for structured datasets  
> - 💼 On-premise application storage or internal file gateway  
> - 📁 Self-hosted NAS with object API compatibility  
> - 🧩 Local caching or pre-processing node in hybrid N2 deployments  

---

## 🔮 Planned Enhancements

> 🧠 Local deduplication for structured datasets  
> 📦 Snapshot and rollback support for versioned storage  
> 🔐 End-to-end encryption with optional HSM integration  
> 📊 Native metrics endpoint for performance and capacity tracking  

---

## 🚀 Why “Structured Data Engine”?

> **Litiaina N1** focuses on **data coherence** and **performance predictability** —  
> maintaining atomicity and structure without distributed overhead.  
>  
> Its dual-mode architecture (FS + Object) makes it the **foundation tier** of the Litiaina ecosystem,  
> serving as a **stable single-node engine** or as the **building block for N2 clusters**.

---
