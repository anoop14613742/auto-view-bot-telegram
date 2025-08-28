def upsert_health_frozen(phone: str, reason: str, source: str = "runtime"):
    state = _load_health_state()
    state[phone] = {
        "status": "frozen",
        "reason": reason,
        "since_ts": int(time.time()),
        "detected_during": source,
    }
    _save_health_state(state)
import os, glob, time, asyncio, json
import re
import shutil
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.sessions import StringSession
from typing import Dict, Tuple, Optional

SESSION_DIR = "sessions"
ARTIFACT_EXTS = (".session-journal", ".session-wal", ".session-shm")
ARTIFACT_DIR = "sessions_artifacts"
os.makedirs(ARTIFACT_DIR, exist_ok=True)
CORRUPTED_DIR = "sessions_corrupted"
FROZEN_DIR = "sessions_frozen"

STATE_DIR = "state"
JSON_SIDECAR_PATTERNS = [
    "{phone}.json",
    "{phone}_meta.json",
    "{phone}_device.json",
    "{phone}_proxy.json",
    "{phone}_*settings.json",
    "{phone}_*.json",
    "+{phone}.json",  # Handle phone numbers with + prefix
    "+{phone}_meta.json",
    "+{phone}_device.json", 
    "+{phone}_proxy.json",
    "+{phone}_*settings.json",
    "+{phone}_*.json",
]

FROZEN_GRACE_HOURS = 72
HARD_DELETE = False

os.makedirs(CORRUPTED_DIR, exist_ok=True)
os.makedirs(FROZEN_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)

HEALTH_STATE_FILE = "state/health_state.json"
os.makedirs(os.path.dirname(HEALTH_STATE_FILE), exist_ok=True)

async def _classify_session(file_path: str, api_id: int, api_hash: str, proxy=None) -> Tuple[str, Optional[str]]:
    """Classify session health, properly handling both file-based and string sessions with timeout protection."""
    phone = os.path.splitext(os.path.basename(file_path))[0]
    
    # Detect if this is a string session vs file-based session
    try:
        with open(file_path, "rb") as f:
            content = f.read()
        
        # Try to detect if it's a string session (base64-like text) vs SQLite file
        # Better detection: check for SQLite header first
        is_sqlite = content.startswith(b"SQLite format 3\x00")
        
        if is_sqlite:
            # This is a SQLite database file
            is_string_session = False
        else:
            # Try to decode as string session with better detection
            is_string_session = False
            try:
                # String sessions are typically base64-encoded text
                text_content = content.decode('utf-8').strip()
                
                # Better validation: check for Telethon string session prefix
                # Telethon string sessions often start with "1A" or similar patterns
                if (len(text_content) > 100 and 
                    len(text_content) < 4096 and
                    text_content.startswith(('1A', '1B', '1C', '1D', '1E', '1F'))):
                    # This looks like a Telethon string session
                    is_string_session = True
                    session_string = text_content
                else:
                    # More conservative check - only accept if it's clearly base64-like
                    # and has the right length characteristics
                    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-")
                    flat = text_content.replace('\n','').replace('\r','')
                    if (100 < len(flat) < 4096 and 
                        all(c in allowed for c in flat) and
                        flat.count('=') <= 4):  # Base64 padding check
                        is_string_session = True
                        session_string = text_content
            except UnicodeDecodeError:
                # Binary content that's not SQLite, treat as file-based session
                is_string_session = False
    except Exception as e:
        return "invalid", f"read_error:{type(e).__name__}"

    # Create appropriate client based on session type
    try:
        if is_string_session:
            client = TelegramClient(StringSession(session_string), api_id, api_hash, proxy=proxy, timeout=8)
        else:
            # Use file-based session for SQLite files
            client = TelegramClient(file_path, api_id, api_hash, proxy=proxy, timeout=8)
        
        # Set flood sleep threshold to prevent long waits
        client.flood_sleep_threshold = 10
        
    except Exception as e:
        return "invalid", f"client_creation_error:{type(e).__name__}"

    try:
        # Add timeout protection for the entire session check
        async def check_session():
            await client.connect()
            if not await client.is_user_authorized():
                return "invalid", "not_authorized"

            me = await client.get_me()
            if not me:
                return "invalid", "no_me"

            # Quick dialog check with minimal limit
            await client.get_dialogs(limit=1)
            
            # Additional check for frozen sessions: try to get full user info
            # This often triggers FROZEN_METHOD_INVALID for frozen accounts
            try:
                await client.get_entity("me")
            except Exception as e:
                error_msg = str(e)
                error_type = type(e).__name__
                if ("FROZEN_METHOD_INVALID" in error_msg or 
                    "FrozenAuthKeyError" in error_type or
                    "method is frozen" in error_msg.lower()):
                    return "frozen", f"frozen_detected:{error_type}"
                # If it's not a frozen error, continue with normal flow
            
            return "ok", None
        
        # Use asyncio.wait_for with timeout
        result = await asyncio.wait_for(check_session(), timeout=12.0)
        return result

    except asyncio.TimeoutError:
        return "error", "timeout"
    except asyncio.CancelledError:
        return "error", "cancelled"
    except errors.UserDeactivatedBanError:
        return "banned", "UserDeactivatedBanError"
    except (errors.AuthKeyError, errors.AuthKeyUnregisteredError):
        return "invalid", "AuthKeyError"
    except errors.UserDeactivatedError:
        return "invalid", "UserDeactivatedError"
    except errors.PhoneNumberBannedError:
        return "banned", "PhoneNumberBannedError"
    except errors.FloodWaitError as e:
        return "error", f"FloodWait_{e.seconds}s"
    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        
        # Check for frozen session indicators
        if ("FROZEN_METHOD_INVALID" in error_msg or 
            "FrozenAuthKeyError" in error_type or
            "method is frozen" in error_msg.lower()):
            return "frozen", f"frozen:{error_type}"
        
        return "error", error_type
    finally:
        try:
            if client.is_connected():
                await asyncio.wait_for(client.disconnect(), timeout=3.0)
        except Exception:
            pass

def _remove_session_file(path: str, target_dir: Optional[str] = None):
    base = os.path.basename(path)
    if HARD_DELETE or target_dir is None:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        return
    
    # Ensure target directory exists before moving
    os.makedirs(target_dir, exist_ok=True)
    
    dest = os.path.join(target_dir, base)
    if os.path.exists(dest):
        root, ext = os.path.splitext(dest)
        i = 1
        while os.path.exists(f"{root}.{i}{ext}"):
            i += 1
        dest = f"{root}.{i}{ext}"
    shutil.move(path, dest)

# Define exact sidecar files we control to avoid sweeping unrelated JSON files
CONTROLLED_JSON_PATTERNS = [
    "{phone}.json",           # Main metadata file
    "{phone}_meta.json",      # Additional metadata  
    "{phone}_device.json",    # Device information
    "{phone}_proxy.json",     # Proxy settings
    "{phone}_auth.json",      # Auth-related data
    "+{phone}.json",          # Main metadata with + prefix
    "+{phone}_meta.json",     # Additional metadata with + prefix  
    "+{phone}_device.json",   # Device information with + prefix
    "+{phone}_proxy.json",    # Proxy settings with + prefix
    "+{phone}_auth.json",     # Auth-related data with + prefix
]

def _sidecar_json_paths_for_phone(phone: str):
    """Find controlled JSON sidecar files for a phone number, using safe patterns."""
    paths = []
    
    # Generate phone number variants
    phone_variants = [phone]
    if phone.startswith('+'):
        phone_variants.append(phone[1:])
    else:
        phone_variants.append('+' + phone)
    
    # For each phone variant, try only controlled patterns
    for phone_variant in phone_variants:
        for pat in CONTROLLED_JSON_PATTERNS:
            if pat.startswith("+{phone}"):
                if phone_variant.startswith('+'):
                    # Use the phone number as-is for + patterns
                    file_path = os.path.join(SESSION_DIR, pat.format(phone=phone_variant[1:]))
                else:
                    # Add + for patterns that expect it
                    file_path = os.path.join(SESSION_DIR, pat.format(phone=phone_variant))
            else:
                # Regular patterns - use phone without + if present
                clean_phone = phone_variant[1:] if phone_variant.startswith('+') else phone_variant
                file_path = os.path.join(SESSION_DIR, pat.format(phone=clean_phone))
            
            # Only add if file actually exists (no globbing)
            if os.path.exists(file_path):
                paths.append(file_path)
    
    # Create a sidecar index for future reference if we have files
    if paths:
        _update_sidecar_index(phone, paths)
    
    return paths

def _update_sidecar_index(phone: str, paths: list):
    """Update a sidecar index to track which files we created."""
    index_file = os.path.join(SESSION_DIR, ".sidecar_index.json")
    index = {}
    
    if os.path.exists(index_file):
        try:
            with open(index_file, 'r') as f:
                index = json.load(f)
        except Exception:
            pass
    
    index[phone] = [os.path.basename(p) for p in paths]
    
    try:
        with open(index_file, 'w') as f:
            json.dump(index, f, indent=2)
    except Exception as e:
        # If we can't write the index, continue anyway
        pass

def _load_sidecar_index():
    """Load the sidecar index if it exists."""
    index_file = os.path.join(SESSION_DIR, ".sidecar_index.json")
    if os.path.exists(index_file):
        try:
            with open(index_file, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}
    
    return list(dict.fromkeys(paths))  # Remove duplicates

def _artifact_paths_for_phone(phone: str):
    """Return all SQLite artifacts for this phone's session."""
    base = os.path.join(SESSION_DIR, phone)
    paths = []
    for ext in ARTIFACT_EXTS:
        paths.extend(glob.glob(base + ext))
    return paths

def _remove_artifacts(paths, target_dir: Optional[str] = None):
    """Delete/move wal/shm/journal files."""
    for p in paths:
        if not os.path.exists(p):
            continue
        if HARD_DELETE or target_dir is None:
            try: os.remove(p)
            except FileNotFoundError: pass
        else:
            # Ensure target directory exists before moving
            os.makedirs(target_dir, exist_ok=True)
            
            dest = os.path.join(target_dir, os.path.basename(p))
            if os.path.exists(dest):
                root, ext = os.path.splitext(dest)
                i = 1
                while os.path.exists(f"{root}.{i}{ext}"):
                    i += 1
                dest = f"{root}.{i}{ext}"
            shutil.move(p, dest)

def _remove_or_archive(paths, target_dir: Optional[str] = None):
    for p in paths:
        if not os.path.exists(p):
            continue
        if HARD_DELETE or target_dir is None:
            try: os.remove(p)
            except FileNotFoundError: pass
        else:
            # Ensure target directory exists before moving
            os.makedirs(target_dir, exist_ok=True)
            
            base = os.path.basename(p)
            dest = os.path.join(target_dir, base)
            if os.path.exists(dest):
                root, ext = os.path.splitext(dest)
                i = 1
                while os.path.exists(f"{root}.{i}{ext}"):
                    i += 1
                dest = f"{root}.{i}{ext}"
            shutil.move(p, dest)

def _remove_session_bundle(phone: str, reason_dir: str):
    """Remove/move session files and all related JSON metadata files."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Generate phone variants to handle different formats
    phone_variants = [phone]
    if phone.startswith('+'):
        phone_variants.append(phone[1:])
    else:
        phone_variants.append('+' + phone)
    
    # Find session files - handle both .session and .sessions variants
    session_paths = []
    for phone_variant in phone_variants:
        candidates = [
            os.path.join(SESSION_DIR, f"{phone_variant}.session"),
            os.path.join(SESSION_DIR, f"{phone_variant}.sessions"),
        ]
        session_paths.extend([p for p in candidates if os.path.exists(p)])
    
    # Find all JSON sidecar files for this phone
    json_paths = _sidecar_json_paths_for_phone(phone)
    
    # Combine all files to move
    all_files = session_paths + json_paths
    
    if all_files:
        logger.debug(f"   📦 Moving {len(all_files)} files for {phone}: {len(session_paths)} session files, {len(json_paths)} JSON files")
        for file_path in all_files:
            logger.debug(f"      📄 {os.path.basename(file_path)}")
    
    # Move/remove all files
    _remove_or_archive(all_files, target_dir=reason_dir)
    
    # Handle artifacts separately
    artifacts = []
    for phone_variant in phone_variants:
        artifacts.extend(_artifact_paths_for_phone(phone_variant))
    
    if artifacts:
        _remove_artifacts(artifacts, target_dir=ARTIFACT_DIR if reason_dir else None)

def _load_health_state() -> Dict[str, dict]:
    try:
        with open(HEALTH_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_health_state(state: Dict[str, dict]):
    with open(HEALTH_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def _is_frozen_expired(rec: dict) -> bool:
    if rec.get("status") != "frozen":
        return False
    since = int(rec.get("since_ts", 0))
    if since == 0:
        return False
    return (time.time() - since) >= FROZEN_GRACE_HOURS * 3600

async def cleanup_invalid_and_banned(api_id: int, api_hash: str, proxy=None) -> Tuple[int, int]:
    """Clean up invalid and banned sessions with detailed logging and improved error handling."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Support both .session and .sessions files
    files = glob.glob(os.path.join(SESSION_DIR, "*.session")) + glob.glob(os.path.join(SESSION_DIR, "*.sessions"))
    
    if not files:
        logger.info("🔍 No session files found for cleanup")
        return 0, 0
    
    logger.info(f"🔍 Checking {len(files)} session files for validity...")
    
    kept, removed = 0, 0
    invalid_sessions = []
    banned_sessions = []
    frozen_sessions = []
    error_sessions = []
    
    # Load existing health state to track frozen sessions
    health_state = _load_health_state()
    
    # Process files with concurrency limit to prevent overwhelming Telegram servers
    semaphore = asyncio.Semaphore(2)  # Only 2 concurrent sessions
    
    async def process_session_file(f):
        nonlocal kept, removed
        phone = os.path.splitext(os.path.basename(f))[0]
        logger.info(f"   🔍 Checking session: {phone}")
        
        async with semaphore:
            try:
                # Add overall timeout per session
                status, reason = await asyncio.wait_for(
                    _classify_session(f, api_id, api_hash, proxy=proxy),
                    timeout=20.0  # 20 second timeout per session
                )
                
                if status == "invalid":
                    logger.warning(f"   ❌ {phone}: Invalid session - {reason}")
                    invalid_sessions.append(phone)
                    dir_target = CORRUPTED_DIR
                    os.makedirs(dir_target, exist_ok=True)  # Ensure directory exists
                    _remove_session_bundle(phone, dir_target)
                    removed += 1
                elif status == "banned":
                    logger.warning(f"   🚫 {phone}: Banned session - {reason}")
                    banned_sessions.append(phone)
                    dir_target = CORRUPTED_DIR
                    os.makedirs(dir_target, exist_ok=True)  # Ensure directory exists
                    _remove_session_bundle(phone, dir_target)
                    removed += 1
                elif status == "frozen":
                    logger.warning(f"   ❄️ {phone}: Frozen session detected - {reason}")
                    frozen_sessions.append(phone)
                    # Mark as frozen in health state with current timestamp
                    health_state[phone] = {
                        "status": "frozen",
                        "reason": reason,
                        "since_ts": int(time.time()),
                        "detected_during": "cleanup"
                    }
                    # Move to frozen directory
                    os.makedirs(FROZEN_DIR, exist_ok=True)  # Ensure directory exists
                    _remove_session_bundle(phone, FROZEN_DIR)
                    removed += 1
                elif status == "ok":
                    logger.info(f"   ✅ {phone}: Session is healthy")
                    # Update health state to mark as healthy if it was previously frozen
                    if health_state.get(phone, {}).get("status") == "frozen":
                        health_state[phone] = {
                            "status": "healthy",
                            "last_check_ts": int(time.time()),
                            "recovered_from": "frozen"
                        }
                    kept += 1
                else:
                    logger.warning(f"   ⚠️ {phone}: Session has errors - {reason}")
                    error_sessions.append(phone)
                    # For errors (like timeout, FloodWait), keep the session
                    kept += 1
                    
            except asyncio.TimeoutError:
                logger.warning(f"   ⏱️ {phone}: Session check timed out - keeping")
                error_sessions.append(phone)
                kept += 1
            except asyncio.CancelledError:
                logger.warning(f"   🚫 {phone}: Session check cancelled - keeping")
                error_sessions.append(phone)
                kept += 1
                raise  # Re-raise to handle gracefully
            except Exception as e:
                logger.error(f"   💥 {phone}: Cleanup error - {e}")
                error_sessions.append(phone)
                kept += 1
    
    # Process all files with error handling
    tasks = [process_session_file(f) for f in files]
    
    try:
        # Use asyncio.gather with return_exceptions=True to handle individual failures
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check for any CancelledError in results
        cancelled_count = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
        if cancelled_count > 0:
            logger.warning(f"⚠️ {cancelled_count} session checks were cancelled")
            
    except asyncio.CancelledError:
        logger.warning("⚠️ Session cleanup was cancelled")
        # Save health state before returning
        _save_health_state(health_state)
        return kept, removed
    except Exception as e:
        logger.error(f"💥 Unexpected error during batch session cleanup: {e}")
    
    # Save updated health state
    _save_health_state(health_state)
    
    # Summary logging
    logger.info(f"🧹 Session cleanup completed:")
    logger.info(f"   ✅ Healthy sessions kept: {kept}")
    logger.info(f"   ❌ Invalid/banned/frozen sessions removed: {removed}")
    
    if invalid_sessions:
        logger.info(f"   📋 Invalid sessions removed: {', '.join(invalid_sessions)}")
    if banned_sessions:
        logger.info(f"   🚫 Banned sessions removed: {', '.join(banned_sessions)}")
    if frozen_sessions:
        logger.info(f"   ❄️ Frozen sessions removed: {', '.join(frozen_sessions)}")
    if error_sessions:
        logger.info(f"   ⚠️ Error sessions (kept): {', '.join(error_sessions[:10])}{'...' if len(error_sessions) > 10 else ''}")
    
    return kept, removed

def cleanup_frozen_sessions_from_health() -> Tuple[int, int]:
    """Clean up frozen sessions based on health state with detailed logging."""
    import logging
    logger = logging.getLogger(__name__)
    
    state = _load_health_state()

    # Scan both SESSION_DIR and FROZEN_DIR for .session/.sessions files
    files_main = glob.glob(os.path.join(SESSION_DIR, "*.session")) + glob.glob(os.path.join(SESSION_DIR, "*.sessions"))
    files_frozen = glob.glob(os.path.join(FROZEN_DIR, "*.session")) + glob.glob(os.path.join(FROZEN_DIR, "*.sessions"))
    phones_on_disk = {os.path.splitext(os.path.basename(p))[0] for p in files_main}
    phones_in_frozen = {os.path.splitext(os.path.basename(p))[0] for p in files_frozen}

    if not phones_on_disk and not phones_in_frozen:
        logger.info("🔍 No session files found for frozen cleanup")
        return 0, 0

    logger.info(f"🔍 Checking {len(phones_on_disk)} sessions and {len(phones_in_frozen)} frozen for frozen state cleanup...")

    kept, removed = 0, 0
    now = time.time()
    frozen_expired = []
    frozen_active = []

    # Check sessions in SESSION_DIR
    for phone in list(phones_on_disk):
        rec = state.get(phone) or {}
        if rec.get("status") == "frozen":
            since = int(rec.get("since_ts", 0))
            if since and (now - since) >= FROZEN_GRACE_HOURS * 3600:
                hours_frozen = (now - since) / 3600
                logger.warning(f"   ❄️ {phone}: Frozen for {hours_frozen:.1f}h (>{FROZEN_GRACE_HOURS}h) - removing")
                frozen_expired.append(phone)
                os.makedirs(FROZEN_DIR, exist_ok=True)  # Ensure directory exists
                _remove_session_bundle(phone, FROZEN_DIR)
                rec["status"] = "removed_frozen"
                rec["removed_ts"] = int(now)
                state[phone] = rec
                removed += 1
            else:
                hours_frozen = (now - since) / 3600 if since else 0
                logger.info(f"   ❄️ {phone}: Frozen for {hours_frozen:.1f}h (<{FROZEN_GRACE_HOURS}h) - keeping")
                frozen_active.append(phone)
                kept += 1
        else:
            logger.debug(f"   ✅ {phone}: Not frozen - keeping")
            kept += 1

    # Check sessions in FROZEN_DIR for expiry
    for phone in list(phones_in_frozen):
        rec = state.get(phone) or {}
        if rec.get("status") == "frozen":
            since = int(rec.get("since_ts", 0))
            if since and (now - since) >= FROZEN_GRACE_HOURS * 3600:
                hours_frozen = (now - since) / 3600
                logger.warning(f"   ❄️ {phone}: Frozen in FROZEN_DIR for {hours_frozen:.1f}h (>{FROZEN_GRACE_HOURS}h) - purging")
                # Optionally, move to an archive or delete
                _remove_session_bundle(phone, None)  # None = delete
                rec["status"] = "purged_frozen"
                rec["purged_ts"] = int(now)
                state[phone] = rec
                removed += 1
            else:
                hours_frozen = (now - since) / 3600 if since else 0
                logger.info(f"   ❄️ {phone}: Still within grace period in FROZEN_DIR ({hours_frozen:.1f}h < {FROZEN_GRACE_HOURS}h) - keeping")
                frozen_active.append(phone)
                kept += 1
        else:
            logger.debug(f"   ✅ {phone}: Not frozen in FROZEN_DIR - keeping")
            kept += 1
    
    _save_health_state(state)
    
    # Summary logging
    logger.info(f"🧹 Frozen session cleanup completed:")
    logger.info(f"   ✅ Sessions kept: {kept}")
    logger.info(f"   ❄️ Expired frozen sessions removed: {removed}")
    
    if frozen_expired:
        logger.info(f"   📋 Expired frozen sessions: {', '.join(frozen_expired)}")
    if frozen_active:
        logger.info(f"   ⏳ Active frozen sessions (kept): {', '.join(frozen_active)}")
    
    return kept, removed

async def detect_and_cleanup_frozen_sessions(api_id: int, api_hash: str, proxy=None) -> Tuple[int, int]:
    """Actively detect frozen sessions by testing operations that trigger FROZEN_METHOD_INVALID."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Support both .session and .sessions files
    files = glob.glob(os.path.join(SESSION_DIR, "*.session")) + glob.glob(os.path.join(SESSION_DIR, "*.sessions"))
    
    if not files:
        logger.info("🔍 No session files found for frozen detection")
        return 0, 0
    
    logger.info(f"🔍 Actively testing {len(files)} sessions for frozen state...")
    
    kept, removed = 0, 0
    frozen_sessions = []
    healthy_sessions = []
    error_sessions = []
    
    health_state = _load_health_state()
    semaphore = asyncio.Semaphore(3)  # Limit concurrent checks
    
    async def test_session_for_frozen(file_path: str):
        nonlocal kept, removed
        phone = os.path.splitext(os.path.basename(file_path))[0]
        
        async with semaphore:
            try:
                # Create client
                try:
                    with open(file_path, "rb") as f:
                        content = f.read()
                    
                    is_sqlite = content.startswith(b"SQLite format 3\x00")
                    
                    if not is_sqlite:
                        # Try string session
                        try:
                            text_content = content.decode('utf-8').strip()
                            allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-")
                            flat = text_content.replace('\n','').replace('\r','')
                            if 100 < len(flat) < 4096 and all(c in allowed for c in flat):
                                client = TelegramClient(StringSession(text_content), api_id, api_hash, proxy=proxy, timeout=10)
                            else:
                                client = TelegramClient(file_path, api_id, api_hash, proxy=proxy, timeout=10)
                        except UnicodeDecodeError:
                            client = TelegramClient(file_path, api_id, api_hash, proxy=proxy, timeout=10)
                    else:
                        client = TelegramClient(file_path, api_id, api_hash, proxy=proxy, timeout=10)
                    
                    client.flood_sleep_threshold = 5
                    
                except Exception as e:
                    logger.warning(f"   ⚠️ {phone}: Client creation failed - {e}")
                    error_sessions.append(phone)
                    kept += 1
                    return
                
                # Test for frozen status
                try:
                    async def frozen_test():
                        await client.connect()
                        if not await client.is_user_authorized():
                            return "not_auth"
                        
                        # Test operations that commonly trigger FROZEN_METHOD_INVALID
                        try:
                            # 1. Get user info
                            await client.get_entity("me")
                            
                            # 2. Try to get dialogs
                            await client.get_dialogs(limit=5)
                            
                            # 3. Probe that tends to fail on frozen accounts (read-only)
                            await client(GetParticipantRequest('telegram', 'me'))
                            return "ok"
                            
                        except Exception as e:
                            error_msg = str(e)
                            error_type = type(e).__name__
                            
                            # Check for specific frozen indicators
                            if ("FROZEN_METHOD_INVALID" in error_msg or 
                                "FrozenAuthKeyError" in error_type or
                                "method is frozen" in error_msg.lower() or
                                "RPCError 420: FROZEN_METHOD_INVALID" in error_msg):
                                return "frozen"
                            
                            # Other errors are not necessarily frozen
                            return "error"
                    
                    result = await asyncio.wait_for(frozen_test(), timeout=15.0)
                    
                    if result == "frozen":
                        logger.warning(f"   ❄️ {phone}: FROZEN session detected!")
                        frozen_sessions.append(phone)
                        
                        # Mark as frozen in health state
                        health_state[phone] = {
                            "status": "frozen",
                            "reason": "FROZEN_METHOD_INVALID_detected",
                            "since_ts": int(time.time()),
                            "detected_during": "active_scan"
                        }
                        
                        # Move to frozen directory
                        os.makedirs(FROZEN_DIR, exist_ok=True)  # Ensure directory exists
                        _remove_session_bundle(phone, FROZEN_DIR)
                        removed += 1
                        
                    elif result == "ok":
                        logger.info(f"   ✅ {phone}: Session is healthy")
                        healthy_sessions.append(phone)
                        
                        # Update health state if previously frozen
                        if health_state.get(phone, {}).get("status") == "frozen":
                            health_state[phone] = {
                                "status": "healthy",
                                "last_check_ts": int(time.time()),
                                "recovered_from": "frozen"
                            }
                        kept += 1
                        
                    else:
                        logger.debug(f"   ⚠️ {phone}: Error during test (not frozen)")
                        error_sessions.append(phone)
                        kept += 1
                        
                except asyncio.TimeoutError:
                    logger.warning(f"   ⏱️ {phone}: Frozen test timed out")
                    error_sessions.append(phone)
                    kept += 1
                except Exception as e:
                    error_msg = str(e)
                    if ("FROZEN_METHOD_INVALID" in error_msg or "method is frozen" in error_msg.lower()):
                        logger.warning(f"   ❄️ {phone}: FROZEN session detected via exception - {e}")
                        frozen_sessions.append(phone)
                        
                        health_state[phone] = {
                            "status": "frozen",
                            "reason": f"exception:{type(e).__name__}",
                            "since_ts": int(time.time()),
                            "detected_during": "active_scan"
                        }
                        
                        os.makedirs(FROZEN_DIR, exist_ok=True)  # Ensure directory exists
                        _remove_session_bundle(phone, FROZEN_DIR)
                        removed += 1
                    else:
                        logger.debug(f"   ⚠️ {phone}: Other error - {e}")
                        error_sessions.append(phone)
                        kept += 1
                
                finally:
                    try:
                        if client.is_connected():
                            await asyncio.wait_for(client.disconnect(), timeout=3.0)
                    except Exception:
                        pass
                        
            except Exception as e:
                logger.error(f"   💥 {phone}: Unexpected error during frozen test - {e}")
                error_sessions.append(phone)
                kept += 1
    
    # Process all files
    tasks = [test_session_for_frozen(f) for f in files]
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"💥 Error during batch frozen detection: {e}")
    
    # Save health state
    _save_health_state(health_state)
    
    # Summary logging
    logger.info(f"🧹 Frozen detection completed:")
    logger.info(f"   ✅ Healthy sessions: {len(healthy_sessions)}")
    logger.info(f"   ❄️ Frozen sessions detected and removed: {len(frozen_sessions)}")
    logger.info(f"   ⚠️ Error sessions (kept): {len(error_sessions)}")
    
    if frozen_sessions:
        logger.info(f"   📋 Newly detected frozen sessions: {', '.join(frozen_sessions[:10])}{'...' if len(frozen_sessions) > 10 else ''}")
    
    return kept, removed

# --------- INLINE UTIL FOR FATAL LOAD FAILS ---------
def remove_corrupted_during_load(session_file_path: str):
    """Remove corrupted session during loading with detailed logging."""
    import logging
    logger = logging.getLogger(__name__)
    
    phone = os.path.splitext(os.path.basename(session_file_path))[0]
    logger.warning(f"💥 {phone}: Critical session error during load - moving to corrupted folder")
    
    try:
        # Ensure corrupted directory exists before moving files
        os.makedirs(CORRUPTED_DIR, exist_ok=True)
        
        _remove_session_bundle(phone, CORRUPTED_DIR)
        # also clean artifacts immediately
        artifacts = _artifact_paths_for_phone(phone)
        if artifacts:
            logger.info(f"   🗑️ {phone}: Cleaning {len(artifacts)} associated artifacts")
            _remove_artifacts(artifacts, target_dir=ARTIFACT_DIR)
        logger.info(f"   ✅ {phone}: Successfully moved to corrupted folder")
    except Exception as e:
        logger.error(f"   ❌ {phone}: Failed to move corrupted session - {e}")

def sweep_orphan_artifacts(max_age_hours: int = 1) -> Tuple[int, int]:
    """
    Remove/move SQLite artifacts that have no corresponding .session file with detailed logging.
    max_age_hours: only touch artifacts older than this (avoid fighting live locks).
    Returns: (kept, removed)
    """
    import logging
    logger = logging.getLogger(__name__)
    
    now = time.time()
    kept = removed = 0
    orphan_artifacts = []
    active_artifacts = []
    
    logger.info(f"🔍 Sweeping orphan SQLite artifacts (older than {max_age_hours}h)...")
    
    # all artifacts in SESSION_DIR
    total_artifacts = 0
    for ext in ARTIFACT_EXTS:
        artifacts = glob.glob(os.path.join(SESSION_DIR, f"*{ext}"))
        total_artifacts += len(artifacts)
        
        for p in artifacts:
            base_session = p.rsplit(ext, 1)[0] + ".session"
            artifact_name = os.path.basename(p)
            
            # skip very new files (possible active client), or if .session exists
            if os.path.exists(base_session):
                logger.debug(f"   ✅ {artifact_name}: Has corresponding session file - keeping")
                active_artifacts.append(artifact_name)
                kept += 1
                continue
                
            age_hours = (now - os.path.getmtime(p)) / 3600.0
            if age_hours < max_age_hours:
                logger.debug(f"   ⏳ {artifact_name}: Too recent ({age_hours:.1f}h) - keeping")
                active_artifacts.append(artifact_name)
                kept += 1
                continue
                
            # archive/delete
            logger.info(f"   🗑️ {artifact_name}: Orphaned for {age_hours:.1f}h - removing")
            orphan_artifacts.append(artifact_name)
            os.makedirs(ARTIFACT_DIR, exist_ok=True)  # Ensure directory exists
            _remove_artifacts([p], target_dir=ARTIFACT_DIR)
            removed += 1
    
    # Summary logging
    if total_artifacts == 0:
        logger.info("🔍 No SQLite artifacts found")
    else:
        logger.info(f"🧹 Artifact cleanup completed:")
        logger.info(f"   ✅ Active artifacts kept: {kept}")
        logger.info(f"   🗑️ Orphan artifacts removed: {removed}")
        
        if orphan_artifacts:
            logger.info(f"   📋 Removed artifacts: {', '.join(orphan_artifacts)}")
        
    return kept, removed
