# 🧠 Minecraft God World

A durability-aware, stateful multi-agent AI system for Minecraft.

This project powers autonomous AI agents (Mara, Eli, Nox, etc.) that:
- Respond to chat
- Maintain persistent world state
- Evolve trust/mood profiles
- Execute turns deterministically
- Survive crashes without corrupting memory

The system is built with production-level durability principles:
- Atomic persistence
- File locking
- Idempotent event processing
- Transactional state mutations
- Structured logging
- Defensive async handling

---

## 🏗 Architecture Overview

This project is intentionally **separate from the Minecraft server**.
