cd ~/task-queue-system
cat > README.md << 'EOF'
# 🚀 Distributed Task Queue System

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com)
[![Redis](https://img.shields.io/badge/Redis-7.2-red.svg)](https://redis.io)
[![WebSockets](https://img.shields.io/badge/WebSockets-12.0-purple.svg)](https://websockets.readthedocs.io)
[![Railway](https://img.shields.io/badge/Railway-Deployed-black.svg)](https://railway.app)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 📌 Live Demo

**Try it live:** https://task-system-api-production.up.railway.app/dashboard

> ⚡ Submit tasks, watch workers process them in real-time, and see failed tasks go to the Dead Letter Queue.

---

## 🎯 What Is This?

A **production-ready, fault-tolerant background job processing system** built for real-world applications. Think of it as your own lightweight Sidekiq/Celery/ Bull.

### Why This Matters

Most background job implementations are toy projects. This one is built like what **actual companies use**:

| Skill | Demonstrated |
|-------|---------------|
| System Design | Queue-based architecture with Redis |
| Fault Tolerance | Automatic retries + Dead Letter Queue |
| Horizontal Scaling | Multiple workers processing in parallel |
| Production Patterns | Health checks, graceful shutdown |
| Real-time Communication | WebSockets for live dashboard updates |
| Containerization | Docker & Docker Compose |
| Cloud Deployment | Live on Railway (free tier) |
| Monitoring | Prometheus metrics endpoint |

---

## ✨ Features

| Feature | Status |
|---------|--------|
| Async task processing | ✅ |
| Redis-backed queue | ✅ |
| Automatic retries with backoff | ✅ |
| Dead letter queue for failures | ✅ |
| Failed task inspection & retry | ✅ |
| Real image processing (Pillow) | ✅ |
| Email task simulation | ✅ |
| Prometheus metrics | ✅ |
| Professional web dashboard | ✅ |
| Dark/Light mode UI | ✅ |
| Real-time WebSocket updates | ✅ |
| Multiple workers (horizontal scaling) | ✅ |
| Docker & Docker Compose | ✅ |
| Cloud deployment (Railway) | ✅ |

---

## 🏗️ Architecture
