import asyncio
import random
import time
import json
from pathlib import Path
from telethon import functions, errors, types
from typing import Any, Dict

# Global state dictionary - ensure defined before any save_* calls
state: dict = {}

FLOOD_UNTIL = {}  # phone -> unix_ts

async def tg_call(phone: str, coro_fn, *, label: str, max_retries: int = 3) -> Dict[str, Any]:
    # respect flood window
    now = int(time.time())
    until = FLOOD_UNTIL.get(phone, 0)
    if now < until:
        return {"ok": False, "error": f"flood_wait_active:{until-now}s", "retry_at": until}

    delay = 0.75
    for attempt in range(max_retries + 1):
        try:
            res = await coro_fn()
            # random human-like jitter after successful API call
            await asyncio.sleep(random.uniform(0.2, 1.4))
            return {"ok": True, "result": res, "error": None, "retry_at": None}
        except errors.FloodWaitError as e:
            FLOOD_UNTIL[phone] = int(time.time()) + int(e.seconds)
            return {"ok": False, "error": f"flood:{e.seconds}s", "retry_at": FLOOD_UNTIL[phone]}
        except (errors.InternalServerError, errors.RpcCallFailError, asyncio.TimeoutError, ConnectionError) as e:
            if attempt >= max_retries:
                return {"ok": False, "error": f"transient:{type(e).__name__}", "retry_at": None}
            await asyncio.sleep(delay + random.uniform(0.2, 0.9))
            delay = min(delay * 2.0, 8.0)
        except errors.AuthKeyError:
            return {"ok": False, "error": "authkey_error", "retry_at": None}
        except errors.UserDeactivatedBanError:
            return {"ok": False, "error": "banned", "retry_at": None}
        except Exception as e:
            return {"ok": False, "error": f"unknown:{type(e).__name__}", "retry_at": None}

# Quotas & state (persist these dicts to JSON on tick/exit)
PER_ACCOUNT_DAILY_CAP = 0     # 0 = No limit for daily per account
PER_CHAT_HOURLY_CAP = 0       # 0 = No limit for hourly per chat
PER_CHAT_MONTHLY_CAP = {}     # chat_id -> monthly_limit (0 = no limit)
CHAT_VIEWS_PER_POST = {}      # chat_id -> views_per_post (number of views per new post)

account_daily_usage = {}   # phone -> {"date":"YYYY-MM-DD","count":int}
chat_hour_usage = {}       # chat_id -> {"hour":"YYYYMMDDHH","count":int}
chat_monthly_usage = {}    # chat_id -> {"month":"YYYY-MM","count":int}

def _rollover_account_cap(phone: str, today: str):
    rec = account_daily_usage.get(phone)
    if not rec or rec["date"] != today:
        account_daily_usage[phone] = {"date": today, "count": 0}

def _rollover_chat_cap(chat_id: int, hour_key: str):
    rec = chat_hour_usage.get(chat_id)
    if not rec or rec["hour"] != hour_key:
        chat_hour_usage[chat_id] = {"hour": hour_key, "count": 0}

def _rollover_chat_monthly_cap(chat_id: int, month_key: str):
    rec = chat_monthly_usage.get(chat_id)
    if not rec or rec["month"] != month_key:
        chat_monthly_usage[chat_id] = {"month": month_key, "count": 0}

def has_budget(phone: str, chat_id: int, today: str, hour_key: str, month_key: str = None) -> bool:
    """Check if account has budget remaining for views."""
    from datetime import datetime
    if month_key is None:
        month_key = datetime.now().strftime("%Y-%m")
        
    _rollover_account_cap(phone, today)
    _rollover_chat_cap(chat_id, hour_key)
    _rollover_chat_monthly_cap(chat_id, month_key)
    
    account_usage = account_daily_usage[phone]["count"]
    chat_usage = chat_hour_usage[chat_id]["count"]
    monthly_usage = chat_monthly_usage[chat_id]["count"]
    
    # Check daily account limit (0 = no limit)
    if PER_ACCOUNT_DAILY_CAP > 0 and account_usage >= PER_ACCOUNT_DAILY_CAP:
        # Log when account hits daily limit for visibility
        if account_usage == PER_ACCOUNT_DAILY_CAP:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"📊 Account {phone} hit daily limit ({PER_ACCOUNT_DAILY_CAP})")
        return False
    
    # Check hourly chat limit (0 = no limit)    
    if PER_CHAT_HOURLY_CAP > 0 and chat_usage >= PER_CHAT_HOURLY_CAP:
        # Log when chat hits hourly limit for visibility
        if chat_usage == PER_CHAT_HOURLY_CAP:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"📊 Chat {chat_id} hit hourly limit ({PER_CHAT_HOURLY_CAP})")
        return False
    
    # Check monthly chat limit (chat-specific limit)
    monthly_limit = PER_CHAT_MONTHLY_CAP.get(chat_id, 0)
    if monthly_limit > 0 and monthly_usage >= monthly_limit:
        # Log when chat hits monthly limit for visibility
        if monthly_usage == monthly_limit:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"📊 Chat {chat_id} hit monthly limit ({monthly_limit})")
        return False
        
    return True

def get_budget_status(phone: str, chat_id: int, today: str, hour_key: str, month_key: str = None) -> Dict[str, Any]:
    """Get detailed budget status for observability."""
    from datetime import datetime
    if month_key is None:
        month_key = datetime.now().strftime("%Y-%m")
        
    _rollover_account_cap(phone, today)
    _rollover_chat_cap(chat_id, hour_key)
    _rollover_chat_monthly_cap(chat_id, month_key)
    
    account_usage = account_daily_usage[phone]["count"]
    chat_usage = chat_hour_usage[chat_id]["count"]
    monthly_usage = chat_monthly_usage[chat_id]["count"]
    monthly_limit = PER_CHAT_MONTHLY_CAP.get(chat_id, 0)
    
    # Calculate percentages safely
    account_percentage = (account_usage / PER_ACCOUNT_DAILY_CAP) * 100 if PER_ACCOUNT_DAILY_CAP > 0 else 0
    chat_percentage = (chat_usage / PER_CHAT_HOURLY_CAP) * 100 if PER_CHAT_HOURLY_CAP > 0 else 0
    monthly_percentage = (monthly_usage / monthly_limit) * 100 if monthly_limit > 0 else 0
    
    return {
        "account": {
            "used": account_usage,
            "limit": PER_ACCOUNT_DAILY_CAP,
            "remaining": max(0, PER_ACCOUNT_DAILY_CAP - account_usage) if PER_ACCOUNT_DAILY_CAP > 0 else "unlimited",
            "percentage": account_percentage
        },
        "chat": {
            "used": chat_usage,
            "limit": PER_CHAT_HOURLY_CAP,
            "remaining": max(0, PER_CHAT_HOURLY_CAP - chat_usage) if PER_CHAT_HOURLY_CAP > 0 else "unlimited",
            "percentage": chat_percentage
        },
        "monthly": {
            "used": monthly_usage,
            "limit": monthly_limit,
            "remaining": max(0, monthly_limit - monthly_usage) if monthly_limit > 0 else "unlimited",
            "percentage": monthly_percentage
        },
        "has_budget": (PER_ACCOUNT_DAILY_CAP == 0 or account_usage < PER_ACCOUNT_DAILY_CAP) and 
                     (PER_CHAT_HOURLY_CAP == 0 or chat_usage < PER_CHAT_HOURLY_CAP) and
                     (monthly_limit == 0 or monthly_usage < monthly_limit)
    }

def consume_budget(phone: str, chat_id: int, n: int = 1, *, today: str = None, hour_key: str = None, month_key: str = None):
    from datetime import datetime
    if today is None:
        today = datetime.now().strftime("%Y-%m-%d")
    if hour_key is None:
        hour_key = datetime.now().strftime("%Y%m%d%H")
    if month_key is None:
        month_key = datetime.now().strftime("%Y-%m")
    
    # Defensive: ensure rollover has been called first
    _rollover_account_cap(phone, today)
    _rollover_chat_cap(chat_id, hour_key)
    _rollover_chat_monthly_cap(chat_id, month_key)
    
    account_daily_usage[phone]["count"] += n
    chat_hour_usage[chat_id]["count"] += n
    chat_monthly_usage[chat_id]["count"] += n

def set_chat_monthly_limit(chat_id: int, limit: int):
    """Set monthly limit for a specific chat."""
    PER_CHAT_MONTHLY_CAP[chat_id] = limit

def get_chat_monthly_limit(chat_id: int) -> int:
    """Get monthly limit for a specific chat."""
    return PER_CHAT_MONTHLY_CAP.get(chat_id, 0)

def set_chat_views_per_post(chat_id: int, views: int):
    """Set views per post for a specific chat."""
    CHAT_VIEWS_PER_POST[chat_id] = views

def get_chat_views_per_post(chat_id: int) -> int:
    """Get views per post for a specific chat."""
    return CHAT_VIEWS_PER_POST.get(chat_id, 1)  # Default to 1 view per post

def get_usage_stats(phone: str, today: str, hour_key: str) -> Dict[str, Any]:
    """Get usage statistics for a phone number."""
    _rollover_account_cap(phone, today)
    
    account_usage = account_daily_usage[phone]["count"]
    
    return {
        "account": {
            "usage": account_usage,
            "limit": PER_ACCOUNT_DAILY_CAP,
            "remaining": max(0, PER_ACCOUNT_DAILY_CAP - account_usage),
            "percentage": (account_usage / PER_ACCOUNT_DAILY_CAP) * 100 if PER_ACCOUNT_DAILY_CAP > 0 else 0
        }
    }

def select_accounts_for_views(phones: list[str], chat_id: int, need: int, *, state) -> list[str]:
    """
    phones: all eligible phones
    state: provides rotating offset & LRU cooldown per chat (persist this state)
    """
    # rotate window
    offset = state.setdefault(chat_id, {"offset": 0, "recent": []})
    window = phones[offset["offset"]:] + phones[:offset["offset"]]
    chosen = []
    for p in window:
        if p in offset["recent"]:
            continue
        chosen.append(p)
        if len(chosen) >= need:
            break
    # update round-robin offset & LRU
    offset["offset"] = (offset["offset"] + 1) % max(len(phones), 1)
    offset["recent"] = (offset["recent"] + chosen)[-min(50, len(phones)):]
    return chosen

async def add_views(client, phone: str, chat, message_ids: list[int], *, label: str):
    async def do_call():
        return await client(functions.messages.GetMessagesViewsRequest(
            peer=chat, id=message_ids, increment=True
        ))
    return await tg_call(phone, do_call, label=label)

# --- Persistence helpers ---

DATA_DIR = Path("sessions")
DATA_DIR.mkdir(exist_ok=True)
FLOOD_UNTIL_FILE = DATA_DIR / "flood_until.json"
ACCOUNT_USAGE_FILE = DATA_DIR / "account_daily_usage.json"
CHAT_USAGE_FILE = DATA_DIR / "chat_hour_usage.json"
CHAT_MONTHLY_USAGE_FILE = DATA_DIR / "chat_monthly_usage.json"
CHAT_MONTHLY_LIMITS_FILE = DATA_DIR / "chat_monthly_limits.json"
CHAT_VIEWS_PER_POST_FILE = DATA_DIR / "chat_views_per_post.json"
STATE_FILE = DATA_DIR / "views_state.json"

def save_json(data, file):
    try:
        with open(file, "w") as f:
            json.dump(data, f)
    except Exception:
        pass

def load_json(file, default):
    if Path(file).exists():
        try:
            with open(file, "r") as f:
                return json.load(f)
        except Exception:
            return default
    return default

def save_all_state():
    save_json(FLOOD_UNTIL, FLOOD_UNTIL_FILE)
    save_json(account_daily_usage, ACCOUNT_USAGE_FILE)
    save_json(chat_hour_usage, CHAT_USAGE_FILE)
    save_json(chat_monthly_usage, CHAT_MONTHLY_USAGE_FILE)
    save_json(PER_CHAT_MONTHLY_CAP, CHAT_MONTHLY_LIMITS_FILE)
    save_json(CHAT_VIEWS_PER_POST, CHAT_VIEWS_PER_POST_FILE)
    save_json(state, STATE_FILE)

def load_all_state():
    global FLOOD_UNTIL, account_daily_usage, chat_hour_usage, chat_monthly_usage, PER_CHAT_MONTHLY_CAP, CHAT_VIEWS_PER_POST, state
    FLOOD_UNTIL.update(load_json(FLOOD_UNTIL_FILE, {}))
    account_daily_usage.update(load_json(ACCOUNT_USAGE_FILE, {}))
    chat_hour_usage.update(load_json(CHAT_USAGE_FILE, {}))
    chat_monthly_usage.update(load_json(CHAT_MONTHLY_USAGE_FILE, {}))
    PER_CHAT_MONTHLY_CAP.update(load_json(CHAT_MONTHLY_LIMITS_FILE, {}))
    CHAT_VIEWS_PER_POST.update(load_json(CHAT_VIEWS_PER_POST_FILE, {}))
    state = load_json(STATE_FILE, {})
    prune_old_usage()

def prune_old_usage():
    # Prune quarantine entries older than 7 days
    now = int(time.time())
    for phone in list(quarantine.keys()):
        until = quarantine[phone].get("until", 0)
        if until and now - until > 7*24*3600:
            del quarantine[phone]
    
    # Limit size of quarantine dict to prevent unbounded growth
    if len(quarantine) > 10000:  # Keep only most recent 10k entries
        sorted_quarantine = sorted(quarantine.items(), 
                                 key=lambda x: x[1].get("last_hit", 0), 
                                 reverse=True)
        quarantine.clear()
        quarantine.update(dict(sorted_quarantine[:10000]))
    
    # Remove old dates/hours from usage dicts
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    hour_key = datetime.now().strftime("%Y%m%d%H")
    
    # Prune account_daily_usage
    for phone in list(account_daily_usage.keys()):
        if account_daily_usage[phone].get("date") != today:
            account_daily_usage[phone] = {"date": today, "count": 0}
    
    # Prune chat_hour_usage
    for chat_id in list(chat_hour_usage.keys()):
        if chat_hour_usage[chat_id].get("hour") != hour_key:
            chat_hour_usage[chat_id] = {"hour": hour_key, "count": 0}
    
    # Limit account_daily_usage size
    if len(account_daily_usage) > 50000:
        # Keep only entries from today
        today_entries = {k: v for k, v in account_daily_usage.items() 
                        if v.get("date") == today}
        account_daily_usage.clear()
        account_daily_usage.update(today_entries)
    
    # Limit chat_hour_usage size
    if len(chat_hour_usage) > 10000:
        # Keep only entries from current hour
        current_entries = {k: v for k, v in chat_hour_usage.items() 
                          if v.get("hour") == hour_key}
        chat_hour_usage.clear()
        chat_hour_usage.update(current_entries)
    
    # Prune FLOOD_UNTIL entries
    for phone in list(FLOOD_UNTIL.keys()):
        if now > FLOOD_UNTIL[phone]:
            del FLOOD_UNTIL[phone]

# --- Health monitor & quarantine ---
quarantine = {}  # phone -> {"until": unix_ts, "hits": int, "last_hit": unix_ts}
QUARANTINE_FILE = DATA_DIR / "quarantine.json"

def save_quarantine():
    save_json(quarantine, QUARANTINE_FILE)

def load_quarantine():
    global quarantine
    quarantine.update(load_json(QUARANTINE_FILE, {}))
    prune_old_usage()

def mark_restriction(phone: str, penalty_hours=72):
    now = int(time.time())
    q = quarantine.get(phone, {"hits": 0, "until": 0, "last_hit": 0})
    q["hits"] += 1
    q["last_hit"] = now
    if q["hits"] >= 2:
        q["until"] = now + penalty_hours*3600
    quarantine[phone] = q
    save_quarantine()

def is_quarantined(phone: str) -> bool:
    return int(time.time()) < quarantine.get(phone, {}).get("until", 0)
