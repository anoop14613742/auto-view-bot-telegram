# 🚀 View Bot + Reaction

> **Advanced Telegram automation system for channel views with intelligent session management**

A sophisticated Telegram bot that automates channel view generation using multiple user accounts. Features enterprise-grade session management, intelligent frozen account detection, and robust error handling for production deployment.

## ✨ Key Features

### 🔥 Core Functionality
- **Automated View Generation** - Intelligent distribution of views across multiple Telegram accounts
- **Real-time Event Processing** - Event-driven architecture for instant view generation
- **Smart Budget Management** - Per-account daily limits and per-chat hourly quotas
- **Flood Protection** - Automatic rate limiting and flood wait handling
- **Quarantine System** - Intelligent detection and isolation of problematic accounts

### 🛡️ Advanced Session Management
- **Multi-format Support** - Handles both SQLite and String session formats
- **Intelligent Health Monitoring** - Comprehensive session validation and frozen account detection
- **Grace Period System** - 72-hour grace period for temporarily frozen accounts
- **Automatic Recovery** - Smart recovery of falsely flagged sessions
- **Metadata Preservation** - Consistent device parameters across session reloads

### 🎯 Bot Interface Features
- **Role-based Access Control** - Owner, Co-owner, and Admin roles with granular permissions
- **Interactive Chat Management** - Easy setup of auto-view channels with count configuration
- **Real-time Statistics** - Live view counts, session health, and performance metrics
- **Login Helper System** - Secure storage of 2FA passwords and F2F data
- **Account Testing Tools** - Comprehensive frozen account detection and debugging

## 🏗️ Architecture Overview

### Core Components
```
hh.py                 → Main bot application (5,140+ lines)
├── Session Management → Loading, validation, health monitoring
├── Auto-view Handler  → Event-driven view generation
├── Bot Commands      → Interactive management interface
├── Account Testing   → Frozen detection algorithms
└── State Persistence → Auto-save configurations

views_core.py         → View generation engine
├── Budget System     → Daily/hourly quota management
├── Flood Protection  → Rate limiting and backoff
├── Account Selection → Round-robin with LRU cooldown
└── Error Handling    → Quarantine and recovery

session_cleanup.py    → Advanced session management
├── Health Detection  → Multi-layer frozen account detection
├── File Management   → Bundle cleanup with metadata
├── Grace Period      → Temporary quarantine system
└── Recovery Logic    → False positive correction

auto_views.py         → Legacy polling system (deprecated)
```

## 📦 Dependencies

### Required Libraries
```bash
# Core Telegram libraries
aiogram>=3.0.0        # Modern async Telegram Bot API
telethon>=1.24.0      # Telegram client library
python-dotenv         # Environment configuration

# System utilities
asyncio               # Async/await support
pathlib              # Modern path handling
logging              # Comprehensive logging
```

### Environment Requirements
- **Python 3.8+** - Modern Python with async/await support
- **Telegram API** - API ID/Hash from my.telegram.org
- **Bot Token** - From @BotFather
- **Storage** - Persistent storage for sessions and state

## ⚙️ Configuration

### Environment Variables (`.env`)
```bash
# 🔑 Required Credentials
API_ID=12345678                    # From my.telegram.org
API_HASH=abcd1234567890abcd        # From my.telegram.org  
BOT_TOKEN=123456:ABC-DEF123        # From @BotFather
OWNER_IDS=123456789,987654321      # Comma-separated admin IDs

# ⚡ Performance Tuning
SKIP_CLEANUP=false                 # Skip startup cleanup for speed
LENIENT_MODE=true                  # Less aggressive frozen detection
JOIN_SECONDS=1                     # Delay between joins
LEAVE_SECONDS=1                    # Delay between leaves
AUTO_VIEW_SECONDS=0.5              # Auto-view delay
STATE_TIMEOUT=300                  # FSM state timeout

# 📞 Optional
ADMIN_CONTACT_USERNAME=admin       # Contact info for users
```

### Production Recommendations
```bash
# Balanced performance & stability
SKIP_CLEANUP=false
LENIENT_MODE=true
AUTO_VIEW_SECONDS=0.5

# High-performance (risk: false positives)
SKIP_CLEANUP=true
LENIENT_MODE=false
AUTO_VIEW_SECONDS=0.1

# Maximum stability (slower startup)
SKIP_CLEANUP=false
LENIENT_MODE=true
AUTO_VIEW_SECONDS=1.0
```

## 🚀 Quick Start

### 1. Initial Setup
```bash
# Clone and navigate
cd "view bot + reaction"

# Install dependencies
pip install aiogram telethon python-dotenv

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

### 2. First Run
```bash
# Start the bot
python hh.py

# The system will automatically:
# ✅ Validate environment configuration
# ✅ Run comprehensive session cleanup
# ✅ Load and validate existing sessions
# ✅ Start the bot interface
```

### 3. Add Sessions
```text
/login          → Add new Telegram accounts
/sessions       → View loaded session status
/health         → Check account health
/stats          → View system statistics
```

### 4. Configure Auto-Views
```text
/auto_view      → Set up channels for auto-views
/mega_auto      → Bulk configuration
/view_stats     → Monitor performance
```

## 📊 System Monitoring

### Health Indicators
- **Active Sessions** - Currently connected and healthy accounts
- **Frozen Accounts** - Temporarily restricted accounts (72h grace period)
- **Budget Status** - Daily quotas and hourly limits per account/chat
- **Quarantine Status** - Accounts with repeated errors
- **View Statistics** - Success rates and performance metrics

### Directory Structure
```
sessions/                    → Active session files
├── *.session               → SQLite session files
├── *.json                  → Session metadata
├── sudo_users.json         → Admin configuration
├── auto_view_chats.json    → Auto-view configuration
└── passwords.json          → Encrypted 2FA storage

sessions_frozen/             → Temporarily restricted accounts
sessions_corrupted/          → Invalid sessions (auto-moved)
sessions_artifacts/          → SQLite artifacts (.wal, .shm)
state/                      → System state files
├── health_state.json       → Session health tracking
└── *.json                  → Various state files
```

## � Advanced Features

### Session Management
- **Multi-format Detection** - Automatic SQLite vs String session detection
- **Consistent Parameters** - Device fingerprint preservation across restarts
- **Health Monitoring** - Multi-layer frozen account detection including:
  - User object flags (`restricted`, `deleted`, `scam`, `fake`)
  - Restriction reasons parsing
  - API operation testing (getAppConfig, authorizations)
  - SpamBot validation checks
  - Method freeze detection (`FROZEN_METHOD_INVALID`)

### Budget System
```python
# Per-account daily limits
PER_ACCOUNT_DAILY_CAP = 1000

# Per-chat hourly limits  
PER_CHAT_HOURLY_CAP = 200

# Automatic rollover and tracking
```

### Quarantine Logic
```python
# Progressive penalties
First error  → Warning (no penalty)
Second error → 72-hour quarantine
Repeated     → Extended quarantine
```

## �️ Maintenance

### Regular Tasks
```bash
# View system health
/health

# Check budget status
/budget_status

# Clean up old artifacts
# (Automatic - no manual intervention needed)
```

### Troubleshooting
```bash
# Test specific account
/test_frozen +1234567890

# Force session reload
# Restart the bot - sessions auto-reload

# Reset quarantine
# Delete state/quarantine.json (if needed)
```

## 🚨 Important Notes

### Security
- **2FA passwords** are encrypted using bot token-derived keys
- **Session strings** are securely stored with metadata
- **Admin access** is restricted to configured owner IDs

### Rate Limiting
- Automatic flood wait handling with exponential backoff
- Per-account and per-chat quota enforcement
- Intelligent account rotation to prevent detection

### Account Safety
- Grace period system prevents permanent loss of temporarily frozen accounts
- Lenient mode reduces false positives in frozen detection
- Comprehensive validation before account removal

## 📈 Performance Stats

- **Session Loading** - Supports 1000+ concurrent sessions
- **View Generation** - Sub-second response time for auto-views
- **Error Recovery** - Automatic retry with exponential backoff
- **Memory Efficiency** - Optimized state management and cleanup

---

**⚡ Built for scale, designed for reliability, optimized for safety.**
