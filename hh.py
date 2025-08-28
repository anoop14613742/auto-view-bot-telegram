import os
from session_cleanup import cleanup_invalid_and_banned, cleanup_frozen_sessions_from_health, detect_and_cleanup_frozen_sessions, remove_corrupted_during_load, sweep_orphan_artifacts, CORRUPTED_DIR
import re
import json
import random
import asyncio
import logging
import time
import signal
import atexit
import hashlib
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Set, Tuple, List, Optional, Union
import views_core

# Environment variables
from dotenv import load_dotenv

# Aiogram imports (version 3.x)
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError, TelegramNotFound,
    TelegramRetryAfter, TelegramAPIError
)
from aiogram import BaseMiddleware

# Telethon for user client operations
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, UserNotParticipantError,
    InviteHashExpiredError, UserAlreadyParticipantError, InviteHashInvalidError,
    ChannelsTooMuchError, ChannelInvalidError, ChannelPrivateError,
    AuthKeyError, UserDeactivatedBanError, UserDeactivatedError, PhoneNumberBannedError
)
from telethon.tl.functions.messages import GetMessagesViewsRequest
from telethon.tl.functions.channels import GetParticipantRequest, JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.account import GetAuthorizationsRequest
from telethon.tl.functions import InitConnectionRequest
from telethon.tl.functions.help import GetConfigRequest, GetAppConfigRequest
from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator
from telethon.tl.types import MessageEmpty
from telethon.tl.types import JsonObject, JsonObjectValue, JsonString, JsonNumber, JsonBool, JsonArray

# Configure logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Set specific log levels for different components
logging.getLogger('telethon.network.mtprotosender').setLevel(logging.WARNING)
logging.getLogger('telethon.client.telegramclient').setLevel(logging.INFO)
logging.getLogger('aiogram').setLevel(logging.INFO)

# Load environment variables
load_dotenv()

# Check for required environment variables
required_vars = {
    "API_ID": "Telegram API ID (get from my.telegram.org)",
    "API_HASH": "Telegram API hash (get from my.telegram.org)",
    "BOT_TOKEN": "Bot token (get from @BotFather)",
    "OWNER_IDS": "Your Telegram user ID (get from @userinfobot)"
}

missing_vars = [var for var, desc in required_vars.items() if not os.getenv(var)]

# Load the username for admin contact (for non-admin block message)
ADMIN_CONTACT_USERNAME = os.getenv("ADMIN_CONTACT_USERNAME", "admin")

if missing_vars:
    error_msg = "❌ Missing required environment variables in .env file:\n"
    for var in missing_vars:
        error_msg += f"- {var}: {required_vars[var]}\n"
    error_msg += "Please create or update the .env file with these values and try again."
    logger.error(error_msg)
    exit(1)

try:
    API_ID = int(os.getenv("API_ID"))
except ValueError:
    logger.error("API_ID must be a valid integer. Check your .env file.")
    exit(1)

API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Validate bot token format
if not re.match(r'^\d+:[A-Za-z0-9_-]+$', BOT_TOKEN):
    logger.error("Bot token format appears invalid. Please check your .env file.")
    logger.error("Bot tokens should look like: 123456789:ABCDefGhIJKlmNoPQRsTUVwxyZ")
    logger.error("Get a new token from @BotFather on Telegram")
    exit(1)

try:
    OWNER_IDS = set(map(int, os.getenv("OWNER_IDS").split(',')))
except (ValueError, AttributeError):
    logger.error("OWNER_IDS must be a comma-separated list of integers. Check your .env file.")
    exit(1)

# Validate delay settings
for var in ["JOIN_SECONDS", "LEAVE_SECONDS", "STATE_TIMEOUT"]:
    try:
        value = float(os.getenv(var, "1"))
        if value < 0:
            raise ValueError(f"{var} must be non-negative")
    except ValueError:
        logger.error(f"{var} must be a valid non-negative number")
        exit(1)

# Directories for session, sudo user, and users files
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)
SUDO_USERS_FILE = SESSIONS_DIR / "sudo_users.json"
USERS_FILE = SESSIONS_DIR / "users.json"

# Delay settings with defaults
JOIN_DELAY = float(os.getenv("JOIN_SECONDS", "1"))
LEAVE_DELAY = float(os.getenv("LEAVE_SECONDS", "1"))
STATE_TIMEOUT = float(os.getenv("STATE_TIMEOUT", "300"))  # 5 minutes timeout for states

# Auto-reaction and auto-view settings
AUTO_VIEW_FILE = SESSIONS_DIR / "auto_view_chats.json"

# Files for count configurations
AUTO_VIEW_COUNTS_FILE = SESSIONS_DIR / "auto_view_counts.json"

# Login Helper files
F2F_DATA_FILE = SESSIONS_DIR / "f2f_data.json"  # New file for storing F2F data
PASSWORDS_FILE = SESSIONS_DIR / "passwords.json"  # For storing 2FA passwords

# Delay settings for auto features
AUTO_VIEW_DELAY = float(os.getenv("AUTO_VIEW_SECONDS", "0.5"))

# Initialize bot and dispatcher (aiogram 3.x style)
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Global variables
user_clients = {}  # {phone: (client, session_string)}
cancellation_events = {}
user_roles = {}  # {user_id: {'role': 'co-owner'/'admin', 'promoted_by': user_id, 'promoted_at': timestamp}}
known_users = set()  # Set of user IDs who have interacted with the bot
auto_features_enabled = True  # Global switch to enable/disable auto features
f2f_data = {}  # {phone: f2f_data} - Dictionary for storing F2F data
passwords = {}  # {phone: password} - Dictionary for storing 2FA passwords
frozen_accounts = set()  # Set of phone numbers that are frozen/restricted
account_status = {}  # {phone: {'status': 'active'/'frozen', 'last_check': timestamp, 'reason': 'reason'}}

# Statistics counters
view_count = 0
post_count = 0

# Auto features global variables
AUTO_VIEW_CHATS = set()  # Set of auto-view chats
AUTO_VIEW_COUNTS = {}  # Dict to store count of accounts for auto-view
AUTO_REACT_CHATS = set()  # Set of auto-react chats
AUTO_REACT_COUNTS = {}  # Dict to store count of accounts for auto-react
AUTO_DUAL_CHATS = set()  # Set of auto-dual chats
AUTO_DUAL_COUNTS = {}  # Dict to store count of accounts for auto-dual

# Define states for FSM
class AutoFeatureStates(StatesGroup):
    chat_id = State()
    count = State()  # Added for count-based operations

class MegaAutoFeatureStates(StatesGroup):
    chat_ids = State()
    confirm = State()

class AccountStates(StatesGroup):
    phone = State()
    otp = State()
    password = State()

class RemoveAccountStates(StatesGroup):
    phone = State()

class MemberManagementStates(StatesGroup):
    count = State()
    invite_link = State()

class SudoStates(StatesGroup):
    user_id = State()
    role = State()
    confirm_remove = State()

class LoginHelperStates(StatesGroup):
    select_account = State()
    waiting_for_otp = State()
    waiting_for_2fa = State()

class PasswordManagementStates(StatesGroup):
    phone = State()
    password = State()

# Helper functions
async def is_account_frozen_by_app_config(client):
    """Check if account is frozen using help.getAppConfig and improved restriction detection."""
    try:
        # Method 1: Check via getAppConfig (as advisory only, not primary)
        result = await client(GetAppConfigRequest())
        logger.debug(f"getAppConfig result type: {type(result)}")
        
        # Parse the config JSON structure correctly
        config_data = {}
        
        # Handle the proper JsonObject structure from GetAppConfigRequest
        if hasattr(result, 'config') and isinstance(result.config, JsonObject):
            logger.debug(f"Parsing JsonObject config with {len(result.config.data)} items")
            for json_obj_value in result.config.data:
                if hasattr(json_obj_value, 'key') and hasattr(json_obj_value, 'value'):
                    key = json_obj_value.key
                    value_obj = json_obj_value.value
                    
                    # Extract value based on JSON type
                    if isinstance(value_obj, JsonString):
                        config_data[key] = value_obj.value
                    elif isinstance(value_obj, JsonNumber):
                        config_data[key] = value_obj.value
                    elif isinstance(value_obj, JsonBool):
                        config_data[key] = value_obj.value
                    elif isinstance(value_obj, JsonArray):
                        # Handle array values
                        array_values = []
                        for item in value_obj.value:
                            if isinstance(item, JsonString):
                                array_values.append(item.value)
                            elif isinstance(item, JsonNumber):
                                array_values.append(item.value)
                            elif isinstance(item, JsonBool):
                                array_values.append(item.value)
                        config_data[key] = array_values
                    else:
                        config_data[key] = str(value_obj)
                        
        logger.debug(f"Parsed config_data: {config_data}")
        
        # Only check confirmed, specific keys that we know indicate restrictions
        # More conservative approach - whitelist known restriction keys
        confirmed_freeze_indicators = [
            'account_frozen', 'account_restricted', 'is_frozen', 'is_restricted',
            'restriction_type', 'restriction_status', 'frozen_status'
        ]

        found_indicator = False
        for indicator in confirmed_freeze_indicators:
            if indicator in config_data:
                value = config_data[indicator]
                if value is not None and value != 0 and str(value).lower() not in ['false', 'null', 'none', '']:
                    logger.warning(f"Account restriction detected via confirmed getAppConfig key: {indicator}={value}")
                    found_indicator = True
                    return True, f"{indicator}={value}"

        # Log other suspicious keys at DEBUG level only (don't act on them)
        for key, value in config_data.items():
            key_lower = str(key).lower()
            if any(keyword in key_lower for keyword in ['freeze', 'restrict', 'limit', 'suspend', 'ban', 'block', 'disable']):
                if value and str(value).lower() not in ['false', '0', 'null', 'none', '']:
                    logger.debug(f"Potentially suspicious config key (advisory only): {key}={value}")
                        
    except Exception as e:
        logger.debug(f"Error checking freeze status via getAppConfig: {e}")
        # Don't return error as frozen - this method is for additional verification
    
    return False, None
# Security: Simple encryption helpers for sensitive data
def _get_encryption_key():
    """Get or create a simple encryption key based on bot token."""
    # Use bot token hash as encryption key (simple but better than plaintext)
    key_material = BOT_TOKEN + "meta_bot_encryption_salt"
    return hashlib.sha256(key_material.encode()).digest()[:16]  # 16-byte key

def _simple_encrypt(data: str) -> str:
    """Simple XOR-based encryption for sensitive data."""
    if not data:
        return data
    key = _get_encryption_key()
    data_bytes = data.encode('utf-8')
    encrypted = bytearray()
    for i, byte in enumerate(data_bytes):
        encrypted.append(byte ^ key[i % len(key)])
    return base64.b64encode(encrypted).decode('utf-8')

def _simple_decrypt(encrypted_data: str) -> str:
    """Simple XOR-based decryption for sensitive data."""
    if not encrypted_data:
        return encrypted_data
    try:
        key = _get_encryption_key()
        encrypted_bytes = base64.b64decode(encrypted_data.encode('utf-8'))
        decrypted = bytearray()
        for i, byte in enumerate(encrypted_bytes):
            decrypted.append(byte ^ key[i % len(key)])
        return decrypted.decode('utf-8')
    except Exception as e:
        logger.warning(f"Failed to decrypt data, returning as-is: {e}")
        return encrypted_data

def _sanitize_log_data(data):
    """Sanitize sensitive data from logs."""
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if any(sensitive in str(key).lower() for sensitive in ['password', '2fa', 'secret', 'token', 'key']):
                sanitized[key] = "***REDACTED***"
            else:
                sanitized[key] = _sanitize_log_data(value) if isinstance(value, (dict, list)) else value
        return sanitized
    elif isinstance(data, list):
        return [_sanitize_log_data(item) for item in data]
    else:
        return data

def normalize_phone(phone: str) -> str:
    """Normalize phone number format."""
    phone = phone.strip()
    if phone.startswith('+'):
        return '+' + ''.join(c for c in phone[1:] if c.isdigit())
    return '+' + ''.join(c for c in phone if c.isdigit())
    """Normalize phone number format."""
    phone = phone.strip()
    if phone.startswith('+'):
        return '+' + ''.join(c for c in phone[1:] if c.isdigit())
    return '+' + ''.join(c for c in phone if c.isdigit())

def is_auto_react_chat(chat_id: int) -> bool:
    return False

def is_auto_view_chat(chat_id: int) -> bool:
    """Check if chat has auto-view enabled."""
    return chat_id in AUTO_VIEW_CHATS or chat_id in AUTO_DUAL_CHATS

def has_sudo_access(user_id: int) -> bool:
    """Check if user has sudo access."""
    if user_id in OWNER_IDS:
        return True
    role = user_roles.get(user_id, {}).get('role')
    return role in ['co-owner', 'admin']

def get_auto_react_count(chat_id):
    """Get the number of accounts configured for auto-react in a chat."""
    return AUTO_REACT_COUNTS.get(chat_id, len(user_clients) if chat_id in AUTO_REACT_CHATS else 0)

def set_auto_react_count(chat_id, count):
    """Set the number of accounts for auto-react in a chat."""
    AUTO_REACT_COUNTS[chat_id] = min(count, len(user_clients))



def get_auto_view_count(chat_id):
    """Get the number of accounts configured for auto-view in a chat."""
    return AUTO_VIEW_COUNTS.get(chat_id, len(user_clients) if chat_id in AUTO_VIEW_CHATS else 0)

def set_auto_view_count(chat_id, count):
    """Set the number of accounts for auto-view in a chat."""
    AUTO_VIEW_COUNTS[chat_id] = min(count, len(user_clients))

def remove_auto_view_count(chat_id):
    """Remove count tracking for a chat."""
    if chat_id in AUTO_VIEW_COUNTS:
        del AUTO_VIEW_COUNTS[chat_id]

def get_auto_dual_count(chat_id):
    """Get the number of accounts configured for auto-dual in a chat."""
    return AUTO_DUAL_COUNTS.get(chat_id, len(user_clients) if chat_id in AUTO_DUAL_CHATS else 0)

def set_auto_dual_count(chat_id, count):
    """Set the number of accounts for auto-dual in a chat."""
    AUTO_DUAL_COUNTS[chat_id] = min(count, len(user_clients))

def remove_auto_dual_count(chat_id):
    """Remove count tracking for a chat."""
    if chat_id in AUTO_DUAL_COUNTS:
        del AUTO_DUAL_COUNTS[chat_id]

async def ensure_connected(client, phone, retries=3, delay=5):
    """Ensure client is connected to Telegram."""
    for attempt in range(retries):
        try:
            if not client.is_connected():
                await client.connect()
                
            # Verify connection by making a simple request
            me = await client.get_me()
            if me:
                logger.info(f"Client {phone} successfully connected")
                return True
            else:
                logger.error(f"Client {phone} connected but couldn't get user info")
                
        except FloodWaitError as e:
            logger.warning(f"FloodWaitError for {phone}: waiting {e.seconds} seconds")
            views_core.FLOOD_UNTIL[phone] = int(time.time()) + e.seconds
            if attempt < retries - 1:
                await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Connection attempt {attempt + 1}/{retries} failed for {phone}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                
    logger.error(f"Failed to connect client {phone} after {retries} attempts")
    return False

async def validate_session_health(client, phone):
    """Validate if a session is healthy and working properly."""
    # Always check frozen_accounts set first
    if phone in frozen_accounts:
        return False, "Account marked as frozen in system"

    try:
        # Check if client is connected
        if not client.is_connected():
            return False, "Not connected"

        # Try to get user info with timeout
        me = await asyncio.wait_for(client.get_me(), timeout=10)
        if not me:
            return False, "Cannot get user info"

        # Check for deleted account
        if getattr(me, 'deleted', False):
            mark_account_frozen(phone, "Account deleted")
            return False, "Account deleted"

        # Primary check: user.restricted attribute (most reliable)
        if hasattr(me, 'restricted') and me.restricted:
            mark_account_frozen(phone, "User object shows restricted=True")
            return False, "Account restricted (user.restricted=True)"
            
        # Check restriction_reason attribute
        if hasattr(me, 'restriction_reason') and me.restriction_reason:
            restriction_reasons = []
            if isinstance(me.restriction_reason, list):
                for reason in me.restriction_reason:
                    if hasattr(reason, 'text'):
                        restriction_reasons.append(reason.text)
                    else:
                        restriction_reasons.append(str(reason))
            else:
                restriction_reasons.append(str(me.restriction_reason))
            
            if restriction_reasons:
                reason_text = "; ".join(restriction_reasons)
                mark_account_frozen(phone, f"Restriction reason: {reason_text}")
                return False, f"Account restricted: {reason_text}"

        # Check for other user status flags
        problematic_flags = ['scam', 'fake']
        for flag in problematic_flags:
            if hasattr(me, flag) and getattr(me, flag):
                mark_account_frozen(phone, f"Account flagged as {flag}")
                return False, f"Account flagged as {flag}"

        # Enhanced frozen account detection using help.getAppConfig
        try:
            is_frozen, freeze_info = await is_account_frozen_by_app_config(client)
            if is_frozen:
                mark_account_frozen(phone, f"getAppConfig detected freeze: {freeze_info}")
                return False, f"Account frozen (getAppConfig: {freeze_info})"
        except Exception as e:
            logger.debug(f"getAppConfig check failed for {phone}: {e}")

        # Test account functionality with actual API calls
        try:
            # Test 1: Try to get authorizations (detects many types of restrictions)
            await asyncio.wait_for(client(GetAuthorizationsRequest()), timeout=8)
        except (UserDeactivatedBanError, UserDeactivatedError, PhoneNumberBannedError) as e:
            mark_account_frozen(phone, str(e))
            return False, f"Account banned/deactivated: {str(e)}"
        except FloodWaitError as e:
            # FloodWait is not a frozen state, just rate limiting
            return False, f"FloodWait: {e.seconds}s"
        except Exception as e:
            error_str = str(e).lower()
            # Check for various restriction indicators in error messages
            restriction_keywords = [
                'restricted', 'frozen', 'banned', 'suspended', 'limited',
                'auth_key_duplicated', 'user_deactivated', 'phone_number_banned',
                'account_expired', 'session_expired', 'access_token_invalid',
                'blocked', 'violation', 'terms', 'service'
            ]
            if any(keyword in error_str for keyword in restriction_keywords):
                mark_account_frozen(phone, str(e))
                return False, f"Account frozen/restricted: {str(e)}"
            
            # For other authorization errors, log but don't mark as frozen
            logger.debug(f"Authorization check error for {phone}: {e}")

        # Test 2: Try to get user entity (additional validation)
        try:
            full_user = await asyncio.wait_for(client.get_entity('me'), timeout=5)
            if hasattr(full_user, 'restriction_reason') and full_user.restriction_reason:
                mark_account_frozen(phone, f"Entity restriction reason: {full_user.restriction_reason}")
                return False, f"Account restricted: {full_user.restriction_reason}"
                
            # Double-check restricted flag on full entity
            if hasattr(full_user, 'restricted') and full_user.restricted:
                mark_account_frozen(phone, "Entity shows restricted=True")
                return False, "Account restricted (entity.restricted=True)"
                
        except Exception as e:
            logger.debug(f"Entity check error for {phone}: {e}")
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['restricted', 'frozen', 'banned', 'blocked', 'violation']):
                mark_account_frozen(phone, str(e))
                return False, f"Account frozen/restricted: {str(e)}"

        # Test 3: Try to send a test message to SpamBot to verify account status (more targeted)
        try:
            spam_bot = await client.get_entity('@SpamBot')
            # Try to send /start to SpamBot - frozen accounts will get specific responses
            start_time = datetime.now(timezone.utc)
            result = await client.send_message(spam_bot, '/start')
            await asyncio.sleep(5)  # Wait for response
            
            # Get recent messages from SpamBot, but only check fresh replies
            messages = await client.get_messages(spam_bot, limit=5)
            for msg in messages:
                if (msg.text and msg.date and 
                    (start_time - msg.date).total_seconds() <= 10 and  # Within 10 seconds of our /start
                    not msg.out):  # Message from SpamBot, not from us
                    
                    msg_text = msg.text.lower()
                    # Check for frozen/blocked indicators in SpamBot response (more specific)
                    specific_frozen_indicators = [
                        'your account is frozen', 'account was blocked', 'violations of the telegram terms',
                        'account is limited', 'account has been restricted', 'breaking telegram\'s terms',
                        'blocked for violations', 'violation of terms', 'read-only mode',
                        'your account was frozen', 'account will be deleted'
                    ]
                    if any(indicator in msg_text for indicator in specific_frozen_indicators):
                        mark_account_frozen(phone, f"SpamBot detected restriction: {msg.text[:100]}...")
                        return False, f"Account frozen (SpamBot: {msg.text[:50]}...)"
                        
        except Exception as e:
            logger.debug(f"SpamBot check failed for {phone}: {e}")
            # If SpamBot check fails, don't mark as frozen - this is just an additional check

        # Test 4: Tripwire call using a stable public channel
        try:
            # Use a well-known stable public channel (Telegram's official channel)
            # Instead of string 'telegram', use the official @telegram channel
            try:
                # Try to get participant info for a stable public channel
                await asyncio.wait_for(
                    client(GetParticipantRequest('@telegram', 'me')),
                    timeout=8
                )
            except UserNotParticipantError:
                # Not being a participant is fine for this test
                pass
            except ChannelPrivateError:
                # This is also fine - channel exists but is private
                pass
        except Exception as e:
            msg = str(e).lower()
            frozen_keywords = [
                'frozen', 'method is frozen', 'frozen_method_invalid', 'frozenauthkey',
                'blocked', 'restricted', 'banned', 'violation', 'terms'
            ]
            if any(k in msg for k in frozen_keywords):
                mark_account_frozen(phone, str(e))
                return False, f"Account frozen/restricted: {str(e)}"

        return True, "Healthy"

    except FloodWaitError as e:
        return False, f"FloodWait: {e.seconds}s"
    except (UserDeactivatedBanError, UserDeactivatedError, PhoneNumberBannedError) as e:
        mark_account_frozen(phone, str(e))
        return False, f"Account banned/deactivated: {str(e)}"
    except asyncio.TimeoutError:
        return False, "Timeout during health check"
    except Exception as e:
        error_str = str(e).lower()
        # Check for restriction indicators in any exception
        restriction_keywords = [
            'restricted', 'frozen', 'banned', 'suspended', 'limited', 'blocked', 
            'violation', 'terms', 'service', 'deactivated'
        ]
        if any(keyword in error_str for keyword in restriction_keywords):
            mark_account_frozen(phone, str(e))
            return False, f"Account frozen/restricted: {str(e)}"
        return False, f"Error: {str(e)}"

def is_account_frozen(phone):
    """Check if an account is frozen/restricted."""
    return phone in frozen_accounts

def within_active_hours(now, tz=None):
    """Centralized check for active hours to avoid processing during quiet times."""
    ACTIVE_HOURS = (8, 23)  # 8 AM to 11 PM local time
    if tz is None:
        # Use local time if no timezone provided
        current_hour = now.hour
    else:
        current_hour = now.astimezone(tz).hour
    return ACTIVE_HOURS[0] <= current_hour < ACTIVE_HOURS[1]

async def test_account_frozen_status(phone):
    """Test a specific account for frozen status - useful for debugging."""
    if phone not in user_clients:
        return f"❌ Phone {phone} not found in user_clients"
    
    client, _ = user_clients[phone]
    
    try:
        # Test basic connection
        if not client.is_connected():
            await client.connect()
        
        # Test get_me and examine all relevant attributes
        me = await client.get_me()
        logger.info(f"🔍 Testing {phone}: User object - {me}")
        
        # Check specific user attributes
        user_info = {}
        important_attrs = ['id', 'phone', 'username', 'first_name', 'restricted', 'deleted', 'scam', 'fake', 'restriction_reason']
        for attr in important_attrs:
            if hasattr(me, attr):
                value = getattr(me, attr)
                user_info[attr] = value
                if attr in ['restricted', 'deleted', 'scam', 'fake'] and value:
                    logger.warning(f"🚨 {phone}: RESTRICTION FLAG - {attr}={value}")
                elif attr == 'restriction_reason' and value:
                    logger.warning(f"🚨 {phone}: RESTRICTION REASON - {value}")
        
        logger.info(f"📋 {phone} user attributes: {user_info}")
        
        # Test getAppConfig in detail
        logger.info(f"🔍 Testing {phone}: Running getAppConfig...")
        is_frozen, freeze_info = await is_account_frozen_by_app_config(client)
        logger.info(f"🔍 Testing {phone}: getAppConfig result - frozen={is_frozen}, info={freeze_info}")
        if not is_frozen:
            # Try to catch new error types from Telethon
            try:
                await client.get_entity("me")
            except Exception as e:
                error_msg = str(e)
                error_type = type(e).__name__
                if ("FROZEN_METHOD_INVALID" in error_msg or
                    "FrozenAuthKeyError" in error_type or
                    "method is frozen" in error_msg.lower() or
                    "frozen" in error_msg.lower() or
                    "frozen" in error_type.lower()):
                    logger.warning(f"Account {phone} detected as frozen via exception: {error_type} - {error_msg}")
                    is_frozen = True
                    freeze_info = f"exception:{error_type}:{error_msg}"
        
        # Test authorization request
        logger.info(f"🔍 Testing {phone}: Checking authorizations...")
        try:
            auth_result = await asyncio.wait_for(client(GetAuthorizationsRequest()), timeout=8)
            logger.info(f"✅ {phone}: Authorization check passed")
        except Exception as auth_error:
            logger.warning(f"⚠️ {phone}: Authorization check failed - {auth_error}")
        
        # Test full session health
        logger.info(f"🔍 Testing {phone}: Running full session health check...")
        is_healthy, reason = await validate_session_health(client, phone)
        logger.info(f"🔍 Testing {phone}: Session health - healthy={is_healthy}, reason={reason}")
        
        # Summary of findings
        findings = []
        if user_info.get('restricted'):
            findings.append("❌ User restricted flag is True")
        if user_info.get('deleted'):
            findings.append("❌ User deleted flag is True")
        if user_info.get('scam'):
            findings.append("❌ User scam flag is True")
        if user_info.get('fake'):
            findings.append("❌ User fake flag is True")
        if user_info.get('restriction_reason'):
            findings.append(f"❌ Has restriction reason: {user_info['restriction_reason']}")
        if is_frozen:
            findings.append(f"❌ Frozen detected: {freeze_info}")
        
        if not findings:
            findings.append("✅ No obvious restriction indicators found")
        
        result = f"🔍 Detailed Test Results for {phone}:\n"
        result += f"📊 Final Health: {'✅ Healthy' if is_healthy else '❌ Unhealthy'} - {reason}\n"
        result += f"🔍 Findings:\n" + "\n".join(f"   {finding}" for finding in findings)
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error testing {phone}: {e}")
        return f"❌ Error testing {phone}: {e}"

def mark_account_frozen(phone, reason="Unknown"):
    """Mark an account as frozen/restricted."""
    frozen_accounts.add(phone)
    account_status[phone] = {
        'status': 'frozen',
        'last_check': asyncio.get_event_loop().time(),
        'reason': reason
    }
    logger.warning(f"Account {phone} marked as frozen: {reason}")
    # Persist to health state for later grace-based cleanup
    try:
        from session_cleanup import upsert_health_frozen
        upsert_health_frozen(phone, reason, source="validate_session_health")
    except Exception as e:
        logger.debug(f"Failed to persist frozen state for {phone}: {e}")

def mark_account_active(phone):
    """Mark an account as active."""
    if phone in frozen_accounts:
        frozen_accounts.remove(phone)
    account_status[phone] = {
        'status': 'active',
        'last_check': asyncio.get_event_loop().time(),
        'reason': 'Healthy'
    }

def get_active_clients():
    """Get only active (non-frozen) clients."""
    active_clients = {}
    for phone, (client, session_string) in user_clients.items():
        if not is_account_frozen(phone) and client.is_connected():
            active_clients[phone] = (client, session_string)
    return active_clients

def get_account_statistics():
    """Get statistics about active vs frozen accounts."""
    total_accounts = len(user_clients)
    active_accounts = len([phone for phone in user_clients.keys() if not is_account_frozen(phone)])
    frozen_count = len(frozen_accounts)
    connected_active = len([phone for phone, (client, _) in user_clients.items()
                           if not is_account_frozen(phone) and client.is_connected()])
    
    return {
        'total': total_accounts,
        'active': active_accounts,
        'frozen': frozen_count,
        'connected_active': connected_active
    }

async def cleanup_dead_sessions():
    """Remove sessions that are no longer working and mark frozen accounts."""
    dead_sessions = []
    frozen_sessions = []
    
    for phone, (client, session_string) in list(user_clients.items()):
        is_healthy, reason = await validate_session_health(client, phone)
        if not is_healthy:
            logger.warning(f"Session {phone} is unhealthy: {reason}")
            
            # Check if account is frozen/restricted
            if any(keyword in reason.lower() for keyword in ['restricted', 'frozen', 'banned', 'suspended']):
                mark_account_frozen(phone, reason)
                frozen_sessions.append(phone)
            elif "deleted" in reason.lower():
                dead_sessions.append(phone)
        else:
            # Mark as active if it was previously frozen but now healthy
            if is_account_frozen(phone):
                mark_account_active(phone)
                logger.info(f"Account {phone} recovered from frozen state")
                
    from session_cleanup import _remove_session_bundle, CORRUPTED_DIR
    for phone in dead_sessions:
        try:
            client, _ = user_clients.pop(phone)
            if client.is_connected():
                await client.disconnect()
            # Move the session files so they don’t linger
            _remove_session_bundle(phone, CORRUPTED_DIR)
            logger.info(f"Removed dead session: {phone}")
        except Exception as e:
            logger.error(f"Error removing dead session {phone}: {e}")
            
    return len(dead_sessions), len(frozen_sessions)

async def respect_flood_limit(phone, delay=0):
    """Respect flood wait limits for a phone number using views_core."""
    if phone in views_core.FLOOD_UNTIL:
        wait_until = views_core.FLOOD_UNTIL[phone]
        current_time = int(time.time())
        if current_time < wait_until:
            await asyncio.sleep(wait_until - current_time)
    if delay > 0:
        await asyncio.sleep(delay)

def save_auto_chat_lists():
    """Save auto-reaction and auto-view chat lists to JSON files."""
    try:
        with AUTO_VIEW_FILE.open('w') as f:
            json.dump(list(AUTO_VIEW_CHATS), f)
        with AUTO_VIEW_COUNTS_FILE.open('w') as f:
            json.dump({str(k): v for k, v in AUTO_VIEW_COUNTS.items()}, f)
        logger.info(f"Saved auto-view chat lists and counts")
    except Exception as e:
        logger.error(f"Failed to save auto chat lists: {e}")

def load_auto_chat_lists():
    """Load auto-reaction and auto-view chat lists from JSON files."""
    global AUTO_VIEW_CHATS, AUTO_VIEW_COUNTS
    if AUTO_VIEW_FILE.exists():
        try:
            with AUTO_VIEW_FILE.open('r') as f:
                AUTO_VIEW_CHATS = set(json.load(f))
            logger.info(f"Loaded {len(AUTO_VIEW_CHATS)} auto-view chats")
        except Exception as e:
            logger.error(f"Failed to load auto-view chats: {e}")
    if AUTO_VIEW_COUNTS_FILE.exists():
        try:
            with AUTO_VIEW_COUNTS_FILE.open('r') as f:
                AUTO_VIEW_COUNTS = {int(k): v for k, v in json.load(f).items()}
            logger.info(f"Loaded auto-view counts")
        except Exception as e:
            logger.error(f"Failed to load auto-view counts: {e}")

def load_views_core_state():
    """Load views_core state (flood, quotas, quarantine)."""
    try:
        views_core.load_all_state()
        logger.info("📊 Loaded views_core state")
    except Exception as e:
        logger.error(f"❌ Failed to load views_core state: {e}")
    
    try:
        views_core.load_quarantine()
        logger.info("🔒 Loaded views_core quarantine")
    except Exception as e:
        logger.error(f"❌ Failed to load views_core quarantine: {e}")

def save_views_core_state():
    """Save views_core state (flood, quotas, quarantine)."""
    try:
        views_core.save_all_state()
        logger.info("💾 Saved views_core state")
    except Exception as e:
        logger.error(f"❌ Failed to save views_core state: {e}")
    
    try:
        views_core.save_quarantine()
        logger.info("💾 Saved views_core quarantine")
    except Exception as e:
        logger.error(f"❌ Failed to save views_core quarantine: {e}")

def save_sudo_users():
    """Save sudo users to JSON file."""
    try:
        with SUDO_USERS_FILE.open('w') as f:
            json.dump(user_roles, f, default=str)
        logger.info(f"Saved sudo users to {SUDO_USERS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save sudo users: {e}")

def load_sudo_users():
    """Load sudo users from JSON file."""
    global user_roles
    if SUDO_USERS_FILE.exists():
        try:
            with SUDO_USERS_FILE.open('r') as f:
                loaded_data = json.load(f)
            user_roles.update({
                int(k): {
                    'role': v['role'],
                    'promoted_by': int(v['promoted_by']),
                    'promoted_at': v['promoted_at']
                } for k, v in loaded_data.items()
            })
            logger.info(f"Loaded {len(user_roles)} sudo users from {SUDO_USERS_FILE}")
        except Exception as e:
            logger.error(f"Failed to load sudo users: {e}")

def save_known_users():
    """Save known users to JSON file."""
    try:
        with USERS_FILE.open('w') as f:
            json.dump(list(known_users), f)
        logger.info(f"Saved {len(known_users)} known users to {USERS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save known users: {e}")

def load_known_users():
    """Load known users from JSON file."""
    global known_users
    if USERS_FILE.exists():
        try:
            with USERS_FILE.open('r') as f:
                loaded_users = json.load(f)
            known_users.update(map(int, loaded_users))
            logger.info(f"Loaded {len(known_users)} known users from {USERS_FILE}")
        except Exception as e:
            logger.error(f"Failed to load known users: {e}")

# Login Helper functions
def save_f2f_data():
    """Save F2F data to JSON file with encryption for sensitive fields."""
    try:
        # Encrypt sensitive fields before saving
        encrypted_f2f_data = {}
        for phone, data in f2f_data.items():
            encrypted_data = {}
            for key, value in data.items():
                if any(sensitive in key.lower() for sensitive in ['password', '2fa', 'secret']):
                    try:
                        encrypted_data[key] = _simple_encrypt(str(value))
                    except Exception as e:
                        logger.warning(f"Failed to encrypt {key} for {phone}: {e}")
                        encrypted_data[key] = value
                else:
                    encrypted_data[key] = value
            encrypted_f2f_data[phone] = encrypted_data
            
        with F2F_DATA_FILE.open('w') as f:
            json.dump(encrypted_f2f_data, f)
        logger.info(f"Saved encrypted F2F data for {len(f2f_data)} accounts")
    except Exception as e:
        logger.error(f"Failed to save F2F data: {e}")

def load_f2f_data():
    """Load F2F data from JSON file with decryption for sensitive fields."""
    if F2F_DATA_FILE.exists():
        try:
            with F2F_DATA_FILE.open('r') as f:
                encrypted_f2f_data = json.load(f)
            
            # Decrypt sensitive fields
            decrypted_f2f_data = {}
            for phone, data in encrypted_f2f_data.items():
                decrypted_data = {}
                for key, value in data.items():
                    if any(sensitive in key.lower() for sensitive in ['password', '2fa', 'secret']) and isinstance(value, str):
                        try:
                            decrypted_data[key] = _simple_decrypt(value)
                        except Exception as e:
                            logger.warning(f"Failed to decrypt {key} for {phone}, treating as plaintext: {e}")
                            decrypted_data[key] = value  # Fallback for migration
                    else:
                        decrypted_data[key] = value
                decrypted_f2f_data[phone] = decrypted_data
                
            logger.info(f"Loaded F2F data for {len(decrypted_f2f_data)} accounts")
            return decrypted_f2f_data
        except Exception as e:
            logger.error(f"Failed to load F2F data: {e}")
    return {}

def store_f2f_data(phone, data):
    """Store F2F data for a phone number with automatic encryption of sensitive fields."""
    if phone not in f2f_data:
        f2f_data[phone] = {}
    
    # Sanitize data for logging
    sanitized_data = _sanitize_log_data(data)
    
    # Update with new data
    f2f_data[phone].update(data)
    
    # Save to file (encryption handled in save_f2f_data)
    save_f2f_data()
    logger.info(f"Updated F2F data for {phone}: {sanitized_data}")

def get_f2f_data(phone):
    """Get F2F data for a specific phone number."""
    return f2f_data.get(phone, {})

def load_passwords():
    """Load 2FA passwords from JSON file with decryption."""
    global passwords
    if PASSWORDS_FILE.exists():
        try:
            with PASSWORDS_FILE.open('r') as f:
                encrypted_passwords = json.load(f)
            # Decrypt passwords
            passwords = {}
            for phone, encrypted_password in encrypted_passwords.items():
                try:
                    passwords[phone] = _simple_decrypt(encrypted_password)
                except Exception as e:
                    logger.warning(f"Failed to decrypt password for {phone}, treating as plaintext: {e}")
                    passwords[phone] = encrypted_password  # Fallback for migration
            logger.info(f"Loaded 2FA passwords for {len(passwords)} accounts")
            return passwords
        except Exception as e:
            logger.error(f"Failed to load 2FA passwords: {e}")
    return {}

def get_2fa_password(phone):
    """Get the 2FA password for a specific phone number."""
    if PASSWORDS_FILE.exists():
        try:
            with PASSWORDS_FILE.open('r') as f:
                encrypted_passwords = json.load(f)
            encrypted_password = encrypted_passwords.get(phone)
            if encrypted_password:
                try:
                    return _simple_decrypt(encrypted_password)
                except Exception as e:
                    logger.warning(f"Failed to decrypt password for {phone}, treating as plaintext: {e}")
                    return encrypted_password  # Fallback for migration
        except Exception as e:
            logger.error(f"Failed to retrieve 2FA password for {phone}: {e}")
    return None

def save_2fa_password(phone, password):
    """Save a 2FA password for a phone number with encryption."""
    encrypted_passwords = {}
    if PASSWORDS_FILE.exists():
        try:
            with PASSWORDS_FILE.open('r') as f:
                encrypted_passwords = json.load(f)
        except Exception as e:
            logger.error(f"Error loading passwords file: {e}")
    
    # Encrypt the password before saving
    try:
        encrypted_password = _simple_encrypt(password)
        encrypted_passwords[phone] = encrypted_password
    except Exception as e:
        logger.error(f"Error encrypting password: {e}")
        return False
    
    # Save the updated encrypted passwords
    try:
        with PASSWORDS_FILE.open('w') as f:
            json.dump(encrypted_passwords, f)
        logger.info(f"Saved encrypted 2FA password for {phone}")
        
        # Also store in F2F data (encrypted)
        try:
            encrypted_2fa = _simple_encrypt(password)
            store_f2f_data(phone, {"2fa_password_encrypted": encrypted_2fa})
        except Exception as e:
            logger.warning(f"Failed to store encrypted password in F2F data: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error saving encrypted password: {e}")
        return False




def remove_auto_react_count(chat_id):
    """Remove count tracking for a chat (reactions feature removed)."""
    pass  # Reactions feature has been removed

async def auto_react_handler(event):
    """Handle new messages for auto-react (feature removed)."""
    pass  # Reactions feature has been removed

# Module-level set for tracking entity errors to avoid log spam (with size cap)
_entity_error_logged = set()
_MAX_ENTITY_ERROR_LOG_SIZE = 1000  # Cap to prevent memory growth

def _add_entity_error_log(error_key):
    """Add error to logged set with size limiting."""
    global _entity_error_logged
    if len(_entity_error_logged) >= _MAX_ENTITY_ERROR_LOG_SIZE:
        # Remove oldest half of entries (crude but effective)
        _entity_error_logged = set(list(_entity_error_logged)[_MAX_ENTITY_ERROR_LOG_SIZE//2:])
    _entity_error_logged.add(error_key)

async def auto_view_handler(event):
    """Handle new messages for auto-view with comprehensive safety checks."""
    global view_count, _entity_error_logged
    
    if not auto_features_enabled:
        return
    
    # FIRST: Quick check if this chat is configured for auto-views (before any expensive operations)
    chat_id = event.chat_id
    if not is_auto_view_chat(chat_id):
        return  # Silently skip non-configured channels
    
    # Only log and process if this is a configured auto-view channel
    message_type = "Unknown"
    if hasattr(event, 'message'):
        if hasattr(event.message, 'post') and event.message.post:
            message_type = "Channel Post"
        elif hasattr(event, 'is_channel') and event.is_channel:
            message_type = "Channel Message"
        elif hasattr(event, 'is_group') and event.is_group:
            message_type = "Group Message"
        else:
            message_type = "Private Message"
    
    logger.info(f"🔍 AUTO-VIEW: {message_type} in configured channel - Chat: {chat_id}, Message: {event.message.id}")
    
    # Pre-check active hours for configured channels only
    now = datetime.now()
    current_hour = now.hour
    # TEMPORARILY DISABLED: Remove time restriction for testing
    # if not within_active_hours(now):
    #     logger.info(f"🔍 Outside active hours ({current_hour}h), skipping auto-view")
    #     return
    logger.info(f"🔍 Time check passed - Hour: {current_hour}, processing auto-view")
        
    try:
        chat = await event.get_chat()
        message_id = event.message.id
    except Exception as e:
        logger.error(f"Failed to resolve chat entity: {e}")
        return
        
    # FAST-TRACK: Already confirmed this is a configured auto-view channel
    logger.info(f"🔍 Processing auto-view for configured chat {chat_id}")

    # Pre-check membership using event properties to avoid expensive API calls
    if hasattr(event, 'is_channel') and event.is_channel:
        # This is a channel, proceed with views
        pass

    num_accounts_to_view = get_auto_view_count(chat_id)
    logger.info(f"🔍 Auto-view count for chat {chat_id}: {num_accounts_to_view}")
    if num_accounts_to_view == 0:
        logger.info(f"🔍 No accounts configured for auto-view in chat {chat_id}")
        return

    # Filter clients with enhanced safety checks
    eligible_clients = []
    for phone, (client, _) in user_clients.items():
        # Check connection state
        if not client.is_connected():
            continue
        # Check frozen state  
        if is_account_frozen(phone):
            continue
        # Check quarantine state from views_core
        if views_core.is_quarantined(phone):
            continue
        # Check flood state (quick check)
        if phone in views_core.FLOOD_UNTIL:
            if int(time.time()) < views_core.FLOOD_UNTIL[phone]:
                continue
        eligible_clients.append((phone, client))
    
    if not eligible_clients:
        logger.warning(f"No eligible clients for auto-view in chat {chat_id}")
        return

    eligible_clients.sort(key=lambda x: x[0])
    selected_clients = eligible_clients[:num_accounts_to_view]

    # Check budget constraints before processing
    today = now.strftime("%Y-%m-%d")
    hour_key = now.strftime("%Y%m%d%H")
    
    budget_filtered_clients = []
    for phone, client_obj in selected_clients:
        if views_core.has_budget(phone, chat_id, today, hour_key):
            budget_filtered_clients.append((phone, client_obj))
        else:
            logger.debug(f"Phone {phone} has no budget for chat {chat_id}, skipping")
    
    if not budget_filtered_clients:
        logger.debug(f"No clients with budget for auto-view in chat {chat_id}")
        return

    async def process_view(phone, client_obj):
        global view_count  # Declare access to global view_count
        delay = random.uniform(0.1, 0.3)  # Much faster delays for instant views
        await respect_flood_limit(phone, delay)
        try:
            # Fast entity resolution for auto-view channels (skip participant check for speed)
            try:
                entity = await client_obj.get_entity(chat_id)
                # For configured auto-view channels, assume we can view (faster processing)
                is_participant = True
            except Exception as e:
                logger.error(f"Entity resolution error for {phone} in chat {chat_id}: {e}")
                return

            if is_participant:
                try:
                    result = await views_core.add_views(client_obj, phone, chat_id, [message_id], label=f"views:{chat_id}")
                    if result.get("ok"):
                        # Consume budget on successful view
                        views_core.consume_budget(phone, chat_id, n=1, today=today, hour_key=hour_key)
                        logger.info(f"View sent from {phone} in chat {chat_id}")
                        # Increment global counter
                        view_count += 1
                    else:
                        error_msg = result.get("error", "")
                        logger.warning(f"View failed from {phone} in chat {chat_id}: {error_msg}")
                        # Check for errors that should trigger quarantine
                        if any(x in error_msg for x in ["flood", "banned", "authkey_error", "restricted"]):
                            logger.warning(f"Marking {phone} for quarantine due to: {error_msg}")
                            views_core.mark_restriction(phone)
                except Exception as e:
                    logger.error(f"Error sending view from {phone} in chat {chat_id}: {e}")
                    error_str = str(e).lower()
                    if any(keyword in error_str for keyword in ['restricted', 'frozen', 'banned', 'flood']):
                        logger.warning(f"Marking {phone} for quarantine due to exception: {e}")
                        views_core.mark_restriction(phone)
            else:
                logger.info(f"Client {phone} is not in chat {chat_id}, skipping auto-view.")
        except Exception as e:
            logger.error(f"Error in process_view for {phone} in chat {chat_id}: {e}")

    await asyncio.gather(*(process_view(phone, client_obj) for phone, client_obj in budget_filtered_clients), return_exceptions=True)

# IMPROVED ACCOUNT MANAGEMENT FUNCTIONS
def load_session_metadata(phone):
    """Load session metadata from JSON file with comprehensive parameter extraction."""
    json_file = SESSIONS_DIR / f"{phone}.json"
    if json_file.exists():
        try:
            with open(json_file, 'r') as f:
                metadata = json.load(f)
            logger.info(f"Loaded metadata for {phone}")
            return metadata
        except Exception as e:
            logger.error(f"Error loading metadata for {phone}: {e}")
    return None

def get_consistent_account_parameters(phone, metadata=None):
    """Get consistent parameters for an account, ensuring same parameters are always used."""
    if metadata is None:
        metadata = load_session_metadata(phone)
    
    if metadata:
        # Use parameters from stored metadata to ensure consistency
        return {
            'app_id': metadata.get('app_id', API_ID),
            'app_hash': metadata.get('app_hash', API_HASH),
            'device_model': metadata.get('device', f'Desktop-{phone[-4:]}'),
            'system_version': metadata.get('sdk', 'Windows 10'),
            'app_version': metadata.get('app_version', '5.14.2 x64'),
            'lang_code': metadata.get('lang_code', 'en'),
            'system_lang_code': metadata.get('system_lang_code', 'en-us'),
            'lang_pack': metadata.get('lang_pack', 'tdesktop'),
            'twoFA': metadata.get('twoFA'),
            'user_id': metadata.get('user_id'),
            'first_name': metadata.get('first_name', ''),
            'last_name': metadata.get('last_name', ''),
            'username': metadata.get('username', ''),
            'proxy': metadata.get('proxy'),
            'ipv6': metadata.get('ipv6', False)
        }
    else:
        # Default parameters if no metadata exists
        return {
            'app_id': API_ID,
            'app_hash': API_HASH,
            'device_model': f'Desktop-{phone[-4:]}',
            'system_version': 'Windows 10',
            'app_version': '5.14.2 x64',
            'lang_code': 'en',
            'system_lang_code': 'en-us',
            'lang_pack': 'tdesktop',
            'twoFA': None,
            'user_id': None,
            'first_name': '',
            'last_name': '',
            'username': '',
            'proxy': None,
            'ipv6': False
        }

def create_telegram_client_with_init_request(session, phone, metadata=None, proxy=None, timeout=7):
    """Create TelegramClient with InitConnectionRequest configuration."""
    
    # Get consistent parameters for this account
    account_params = get_consistent_account_parameters(phone, metadata)
    
    # Extract parameters
    app_id = account_params['app_id']
    app_hash = account_params['app_hash']
    device_model = account_params['device_model']
    system_version = account_params['system_version']
    app_version = account_params['app_version']
    lang_code = account_params['lang_code']
    system_lang_code = account_params['system_lang_code']
    lang_pack = account_params['lang_pack']
    
    # Create the client with device parameters (safer than private _init_request)
    client = TelegramClient(
        session, 
        app_id, 
        app_hash, 
        proxy=proxy, 
        timeout=timeout,
        device_model=device_model,
        system_version=system_version,
        app_version=app_version,
        lang_code=lang_code,
        system_lang_code=system_lang_code
    )
    
    # Set flood sleep threshold according to Telethon best practices
    # Automatically sleep for FloodWaitError less than 60 seconds
    client.flood_sleep_threshold = 60
    
    # Only set _init_request if it's safe to do so (with try/except guard)
    try:
        # Create timezone offset (in seconds)
        tz = int(time.timezone)
        
        client._init_request = InitConnectionRequest(
            api_id=app_id,
            device_model=device_model,
            system_version=system_version,
            app_version=app_version,
            system_lang_code=system_lang_code,
            lang_pack=lang_pack,
            lang_code=lang_code,
            query=GetConfigRequest(),
            proxy=None,
            params=JsonObject([
                JsonObjectValue("desktop_app", JsonString("Telegram Desktop")),
                JsonObjectValue("system", JsonString("windows")),
                JsonObjectValue("version", JsonString("5.14.2")),
                JsonObjectValue("tz_offset", JsonNumber(tz))
            ])
        )
    except Exception as e:
        # If setting private attribute fails, log warning but continue
        logger.warning(f"Could not set _init_request for {phone}: {e}")
    
    return client

def save_session_metadata(phone, metadata):
    """Save session metadata to JSON file, preserving existing data."""
    json_file = SESSIONS_DIR / f"{phone}.json"
    existing_metadata = {}
    
    # Load existing metadata if it exists
    if json_file.exists():
        try:
            with open(json_file, 'r') as f:
                existing_metadata = json.load(f)
        except Exception as e:
            logger.error(f"Error loading existing metadata for {phone}: {e}")
    
    # Merge with new metadata, preserving existing values
    existing_metadata.update(metadata)
    
    try:
        with open(json_file, 'w') as f:
            json.dump(existing_metadata, f, indent=2)
        logger.info(f"Saved metadata for {phone}")
        return True
    except Exception as e:
        logger.error(f"Error saving metadata for {phone}: {e}")
        return False

async def comprehensive_session_cleanup():
    """Run comprehensive cleanup of all session-related issues with improved error handling."""
    lenient_mode = os.getenv("LENIENT_MODE", "").lower() in ('true', '1', 'yes')
    
    if lenient_mode:
        logger.info("🕊️ Running cleanup in LENIENT MODE - less aggressive detection")
    else:
        logger.info("🧹 Running comprehensive session cleanup before loading sessions...")
    logger.info("=" * 80)
    
    total_kept = 0
    total_removed = 0
    cleanup_results = {}
    
    # 1. Clean up invalid and banned sessions
    logger.info("🔍 Step 1: Checking for invalid and banned sessions...")
    try:
        # Add timeout to prevent hanging
        kept_invalid, removed_invalid = await asyncio.wait_for(
            cleanup_invalid_and_banned(API_ID, API_HASH),
            timeout=300.0  # 5 minute timeout for this step
        )
        cleanup_results['invalid_banned'] = {'kept': kept_invalid, 'removed': removed_invalid}
        total_kept += kept_invalid
        total_removed += removed_invalid
        logger.info(f"✅ Invalid/banned cleanup: {kept_invalid} kept, {removed_invalid} removed")
    except asyncio.TimeoutError:
        logger.warning("⏱️ Invalid/banned cleanup timed out - continuing with other cleanup steps")
        cleanup_results['invalid_banned'] = {'kept': 0, 'removed': 0, 'error': 'timeout'}
    except asyncio.CancelledError:
        logger.warning("🚫 Invalid/banned cleanup was cancelled - continuing with other cleanup steps")
        cleanup_results['invalid_banned'] = {'kept': 0, 'removed': 0, 'error': 'cancelled'}
    except Exception as e:
        logger.error(f"❌ Error during invalid/banned cleanup: {e}")
        cleanup_results['invalid_banned'] = {'kept': 0, 'removed': 0, 'error': str(e)}
    
    # 2. Clean up frozen sessions from health state
    logger.info("❄️ Step 2a: Checking for expired frozen sessions from health state...")
    try:
        kept_frozen, removed_frozen = cleanup_frozen_sessions_from_health()
        cleanup_results['frozen_health'] = {'kept': kept_frozen, 'removed': removed_frozen}
        total_kept += kept_frozen
        total_removed += removed_frozen
        logger.info(f"✅ Frozen health cleanup: {kept_frozen} kept, {removed_frozen} removed")
    except Exception as e:
        logger.error(f"❌ Error during frozen health cleanup: {e}")
        cleanup_results['frozen_health'] = {'kept': 0, 'removed': 0, 'error': str(e)}
    
    # 2b. Actively detect frozen sessions - ENHANCED FOR THOROUGH CLEANUP
    logger.info("❄️ Step 2b: Actively detecting frozen sessions with enhanced detection...")
    try:
        # Use less aggressive mode if lenient mode is enabled
        aggressive_mode = not lenient_mode
        kept_frozen_active, removed_frozen_active = await asyncio.wait_for(
            detect_and_cleanup_frozen_sessions(API_ID, API_HASH, aggressive_mode=aggressive_mode),
            timeout=900.0  # 15 minute timeout for thorough frozen detection
        )
        cleanup_results['frozen_active'] = {'kept': kept_frozen_active, 'removed': removed_frozen_active}
        total_kept += kept_frozen_active
        total_removed += removed_frozen_active
        logger.info(f"✅ Active frozen detection: {kept_frozen_active} kept, {removed_frozen_active} frozen detected and removed")
    except asyncio.TimeoutError:
        logger.warning("⏱️ Active frozen detection timed out - continuing with other cleanup steps")
        cleanup_results['frozen_active'] = {'kept': 0, 'removed': 0, 'error': 'timeout'}
    except asyncio.CancelledError:
        logger.warning("🚫 Active frozen detection was cancelled - continuing with other cleanup steps")
        cleanup_results['frozen_active'] = {'kept': 0, 'removed': 0, 'error': 'cancelled'}
    except Exception as e:
        logger.error(f"❌ Error during active frozen detection: {e}")
        cleanup_results['frozen_active'] = {'kept': 0, 'removed': 0, 'error': str(e)}
    
    # 2c. ADDITIONAL: Pre-scan session files for obvious frozen indicators
    logger.info("❄️ Step 2c: Pre-scanning session files for frozen indicators...")
    try:
        pre_frozen_count = await pre_scan_frozen_sessions()
        cleanup_results['pre_scan_frozen'] = {'removed': pre_frozen_count}
        total_removed += pre_frozen_count
        logger.info(f"✅ Pre-scan frozen cleanup: {pre_frozen_count} potentially frozen sessions moved")
    except Exception as e:
        logger.error(f"❌ Error during pre-scan frozen cleanup: {e}")
        cleanup_results['pre_scan_frozen'] = {'removed': 0, 'error': str(e)}
    
    # 3. Clean up dead sessions from loaded clients (if any exist)
    logger.info("💀 Step 3: Checking for dead loaded sessions...")
    dead_count = 0
    frozen_count = 0
    try:
        if user_clients:  # Only if we have loaded clients
            dead_count, frozen_count = await asyncio.wait_for(
                cleanup_dead_sessions(),
                timeout=60.0  # 1 minute timeout
            )
            cleanup_results['dead_sessions'] = {'dead': dead_count, 'frozen': frozen_count}
            total_removed += dead_count
            logger.info(f"✅ Dead session cleanup: {dead_count} dead removed, {frozen_count} frozen marked")
        else:
            logger.info("ℹ️ No loaded sessions to check for dead sessions")
            cleanup_results['dead_sessions'] = {'dead': 0, 'frozen': 0}
    except asyncio.TimeoutError:
        logger.warning("⏱️ Dead session cleanup timed out")
        cleanup_results['dead_sessions'] = {'dead': 0, 'frozen': 0, 'error': 'timeout'}
    except Exception as e:
        logger.error(f"❌ Error during dead session cleanup: {e}")
        cleanup_results['dead_sessions'] = {'dead': 0, 'frozen': 0, 'error': str(e)}
    
    # 4. Sweep orphan artifacts
    logger.info("🗑️ Step 4: Sweeping orphan SQLite artifacts...")
    try:
        kept_artifacts, removed_artifacts = sweep_orphan_artifacts(max_age_hours=1)
        cleanup_results['artifacts'] = {'kept': kept_artifacts, 'removed': removed_artifacts}
        total_kept += kept_artifacts
        total_removed += removed_artifacts
        logger.info(f"✅ Artifact cleanup: {kept_artifacts} kept, {removed_artifacts} removed")
    except Exception as e:
        logger.error(f"❌ Error during artifact cleanup: {e}")
        cleanup_results['artifacts'] = {'kept': 0, 'removed': 0, 'error': str(e)}
    
    # 5. Additional cleanup: validate and clean corrupted metadata files
    logger.info("📝 Step 5: Checking for corrupted metadata files...")
    try:
        metadata_kept, metadata_removed = cleanup_corrupted_metadata()
        cleanup_results['metadata'] = {'kept': metadata_kept, 'removed': metadata_removed}
        total_kept += metadata_kept
        total_removed += metadata_removed
        logger.info(f"✅ Metadata cleanup: {metadata_kept} kept, {metadata_removed} removed")
    except Exception as e:
        logger.error(f"❌ Error during metadata cleanup: {e}")
        cleanup_results['metadata'] = {'kept': 0, 'removed': 0, 'error': str(e)}
    
    # Summary of comprehensive cleanup
    logger.info("=" * 80)
    logger.info(f"🎯 Comprehensive Cleanup Summary:")
    logger.info(f"   📊 Total Sessions Kept: {total_kept}")
    logger.info(f"   🗑️ Total Items Removed: {total_removed}")
    logger.info(f"   💀 Dead Sessions: {cleanup_results.get('dead_sessions', {}).get('dead', 0)}")
    logger.info(f"   ❄️ Frozen Sessions (from health): {cleanup_results.get('frozen_health', {}).get('removed', 0)}")
    logger.info(f"   ❄️ Frozen Sessions (actively detected): {cleanup_results.get('frozen_active', {}).get('removed', 0)}")
    logger.info(f"   ❄️ Frozen Sessions (pre-scan): {cleanup_results.get('pre_scan_frozen', {}).get('removed', 0)}")
    
    # Detailed breakdown
    for category, results in cleanup_results.items():
        if 'error' in results:
            logger.warning(f"   ⚠️ {category}: ERROR - {results['error']}")
        else:
            kept = results.get('kept', results.get('dead', 0))
            removed = results.get('removed', results.get('frozen', 0))
            logger.info(f"   📋 {category}: {kept} kept, {removed} processed")
    
    logger.info("=" * 80)
    return cleanup_results

async def pre_scan_frozen_sessions():
    """Pre-scan session files for obvious frozen indicators without connecting."""
    moved_count = 0
    
    # Look for sessions that might be frozen based on metadata or file patterns
    session_files = list(SESSIONS_DIR.glob("*.session")) + list(SESSIONS_DIR.glob("*.sessions"))
    
    for session_file in session_files:
        phone = session_file.stem
        try:
            # Check if already in frozen accounts list
            if is_account_frozen(phone):
                logger.info(f"❄️ Pre-scan: {phone} already marked as frozen, moving...")
                from session_cleanup import _remove_session_bundle, FROZEN_DIR
                _remove_session_bundle(phone, FROZEN_DIR)
                moved_count += 1
                continue
                
            # Check metadata for frozen indicators
            metadata = load_session_metadata(phone)
            if metadata:
                # Look for frozen indicators in metadata
                metadata_str = str(metadata).lower()
                frozen_indicators = [
                    'frozen', 'restricted', 'banned', 'blocked', 'violation', 
                    'terms', 'suspended', 'deactivated', 'limited'
                ]
                
                if any(indicator in metadata_str for indicator in frozen_indicators):
                    logger.info(f"❄️ Pre-scan: {phone} has frozen indicators in metadata, moving...")
                    from session_cleanup import _remove_session_bundle, FROZEN_DIR
                    mark_account_frozen(phone, "Pre-scan detected frozen indicators in metadata")
                    _remove_session_bundle(phone, FROZEN_DIR)
                    moved_count += 1
                    continue
            
            # Check if session file is suspiciously small (might be corrupted/frozen)
            file_size = session_file.stat().st_size
            if file_size < 100:  # Very small session files are usually corrupted
                logger.info(f"❄️ Pre-scan: {phone} has suspiciously small session file ({file_size} bytes), moving...")
                from session_cleanup import _remove_session_bundle, FROZEN_DIR
                mark_account_frozen(phone, f"Pre-scan: Session file too small ({file_size} bytes)")
                _remove_session_bundle(phone, FROZEN_DIR)
                moved_count += 1
                continue
                
        except Exception as e:
            logger.debug(f"Pre-scan error for {phone}: {e}")
            
    return moved_count

def cleanup_corrupted_metadata():
    """Clean up corrupted or orphaned metadata JSON files."""
    kept = 0
    removed = 0
    
    # Get all JSON metadata files
    json_files = list(SESSIONS_DIR.glob("*.json"))
    
    for json_file in json_files:
        phone = json_file.stem
        
        # Skip special files that aren't session metadata
        if phone in ['sudo_users', 'users', 'auto_view_chats', 'auto_view_counts', 'f2f_data', 'passwords']:
            kept += 1
            continue
            
        # Check if corresponding session file exists
        session_file = SESSIONS_DIR / f"{phone}.session"
        sessions_file = SESSIONS_DIR / f"{phone}.sessions"
        
        if not session_file.exists() and not sessions_file.exists():
            logger.info(f"   🗑️ Orphaned metadata: {phone}.json (no session file)")
            try:
                json_file.unlink()
                removed += 1
            except Exception as e:
                logger.error(f"   ❌ Failed to remove {phone}.json: {e}")
                kept += 1
        else:
            # Validate JSON structure
            try:
                with json_file.open('r') as f:
                    data = json.load(f)
                    # Basic validation - should have phone number
                    if not isinstance(data, dict) or 'phone' not in data:
                        logger.warning(f"   ⚠️ Invalid metadata structure: {phone}.json")
                kept += 1
            except json.JSONDecodeError:
                logger.warning(f"   🗑️ Corrupted JSON: {phone}.json")
                try:
                    corrupted_path = json_file.with_suffix('.json.corrupted')
                    json_file.rename(corrupted_path)
                    removed += 1
                except Exception as e:
                    logger.error(f"   ❌ Failed to move corrupted {phone}.json: {e}")
                    kept += 1
            except Exception as e:
                logger.error(f"   ❌ Error validating {phone}.json: {e}")
                kept += 1
    
    return kept, removed

def force_cleanup_frozen_accounts():
    """Force cleanup of all known frozen accounts from memory and move their files."""
    cleaned_count = 0
    
    # Clean up from user_clients
    frozen_phones = list(frozen_accounts)
    for phone in frozen_phones:
        if phone in user_clients:
            try:
                client, _ = user_clients.pop(phone)
                if client and client.is_connected():
                    asyncio.create_task(client.disconnect())
                logger.info(f"❄️ Force cleanup: Removed {phone} from active clients")
                cleaned_count += 1
            except Exception as e:
                logger.debug(f"Error cleaning {phone} from user_clients: {e}")
    
    # Move session files for frozen accounts
    for phone in frozen_phones:
        try:
            from session_cleanup import _remove_session_bundle, FROZEN_DIR
            _remove_session_bundle(phone, FROZEN_DIR)
            logger.info(f"❄️ Force cleanup: Moved {phone} session files to frozen directory")
        except Exception as e:
            logger.debug(f"Error moving {phone} session files: {e}")
    
    logger.info(f"❄️ Force cleanup completed: {cleaned_count} frozen accounts cleaned from memory, {len(frozen_phones)} files moved")
    return cleaned_count

async def load_sessions():
    """Load existing sessions with comprehensive cleanup and improved error handling."""
    # FIRST: Force cleanup any known frozen accounts from previous runs
    logger.info("❄️ Starting with force cleanup of known frozen accounts...")
    force_cleanup_frozen_accounts()
    
    # Check if cleanup should be skipped for faster startup
    skip_cleanup = os.getenv("SKIP_CLEANUP", "").lower() in ('true', '1', 'yes')
    lenient_mode = os.getenv("LENIENT_MODE", "").lower() in ('true', '1', 'yes')
    
    if skip_cleanup:
        logger.info("⚡ Skipping comprehensive cleanup for faster startup (SKIP_CLEANUP=true)")
        # Even if skipping comprehensive cleanup, do a quick frozen cleanup
        try:
            cleanup_frozen_sessions_from_health()
        except Exception as _:
            logger.debug("Light frozen sweep failed; continuing.")
    else:
        # Run comprehensive cleanup before loading sessions
        try:
            cleanup_results = await comprehensive_session_cleanup()
        except asyncio.CancelledError:
            logger.warning("🚫 Comprehensive cleanup was cancelled - continuing with session loading")
        except Exception as e:
            logger.error(f"💥 Comprehensive cleanup failed: {e} - continuing with session loading")
    
    logger.info("📱 Starting session loading process...")
    # Always do a quick sweep that only relies on saved health state
    try:
        cleanup_frozen_sessions_from_health()
    except Exception as _:
        logger.debug("Light sweep failed; continuing.")
    corrupted_files = []
    
    # Support both .session and .sessions files
    session_files = list(SESSIONS_DIR.glob("*.session")) + list(SESSIONS_DIR.glob("*.sessions"))

    # More conservative batch size and delay to prevent API limits
    BATCH_SIZE = 3  # Reduced from 8 to 3
    BATCH_DELAY = 10  # Increased from 5 to 10 seconds
    CONNECTION_TIMEOUT = 15  # Increased timeout

    semaphore = asyncio.Semaphore(BATCH_SIZE)  # Limit concurrency to batch size

    async def process_session_file(session_file):
        """Process a single session file with proper error handling."""
        phone = session_file.stem
        try:
            async with semaphore:
                logger.info(f"🔄 Processing session: {phone}")
                
                # 1. Load session metadata from JSON file and get consistent parameters
                metadata = load_session_metadata(phone)
                if not metadata:
                    logger.warning(f"⚠️ No metadata found for {phone}, creating default parameters")
                    # Create default metadata for consistency
                    account_params = get_consistent_account_parameters(phone, None)
                    metadata = {
                        'app_id': account_params['app_id'],
                        'app_hash': account_params['app_hash'],
                        'sdk': account_params['system_version'],
                        'app_version': account_params['app_version'],
                        'device': account_params['device_model'],
                        'phone': phone
                    }
                    # Save the default metadata for future consistency
                    save_session_metadata(phone, metadata)
                
                # 2. Detect session type and read appropriately with improved detection
                is_string_session = False
                session_content = None
                try:
                    logger.debug(f"🔍 Detecting session type for {phone}")
                    with open(session_file, 'rb') as f:
                        raw_bytes = f.read()
                    
                    # Get file size for validation
                    file_size = len(raw_bytes)
                    logger.debug(f"📏 {phone}: Session file size: {file_size} bytes")
                    
                    # Skip completely empty files
                    if file_size == 0:
                        logger.error(f"❌ {phone}: Session file is completely empty")
                        corrupted_files.append(session_file)
                        return
                    
                    # Better detection: check for SQLite header first (Telethon's SQLiteSession format)
                    is_sqlite = raw_bytes.startswith(b"SQLite format 3\x00")
                    
                    if is_sqlite:
                        # This is a SQLite database file (Telethon's default format)
                        is_string_session = False
                        session_content = str(session_file)  # Use file path for SQLite sessions
                        logger.info(f"📄 {phone}: SQLite session file detected (size: {file_size} bytes)")
                        
                        # More lenient validation for SQLite session files
                        if file_size < 100:
                            logger.warning(f"⚠️ {phone}: SQLite session file very small ({file_size} bytes) - will try anyway")
                    else:
                        # Try to decode as string session - be more lenient
                        try:
                            session_content = raw_bytes.decode('utf-8').strip()
                            if session_content and len(session_content) > 10:  # More lenient length check
                                is_string_session = True
                                logger.info(f"📄 {phone}: String session detected (length: {len(session_content)})")
                            else:
                                # Try as binary/other format - maybe it's a different session format
                                logger.warning(f"⚠️ {phone}: Not a clear string session, treating as SQLite format")
                                is_string_session = False
                                session_content = str(session_file)
                        except UnicodeDecodeError:
                            # If can't decode as UTF-8, treat as binary session file
                            logger.info(f"📄 {phone}: Binary session file detected, treating as SQLite")
                            is_string_session = False
                            session_content = str(session_file)
                        
                except Exception as e:
                    logger.warning(f"⚠️ {phone}: Error reading session file - {e}, will try anyway")
                    # Be more lenient - try to load it as SQLite
                    is_string_session = False
                    session_content = str(session_file)

                # 3. Create client with appropriate session type
                user_client = None
                creation_error = None

                # Get consistent parameters for this account
                account_params = get_consistent_account_parameters(phone, metadata)

                # Create client based on detected session type
                try:
                    logger.info(f"🔧 {phone}: Creating Telegram client...")
                    if is_string_session:
                        user_client = create_telegram_client_with_init_request(
                            StringSession(session_content), phone, metadata
                        )
                    else:
                        user_client = create_telegram_client_with_init_request(
                            session_content, phone, metadata  # session_content is file path for SQLite
                        )
                    
                    # Connect to Telegram with retries
                    logger.info(f"🌐 {phone}: Connecting to Telegram...")
                    max_retries = 3
                    connected = False
                    
                    for attempt in range(max_retries):
                        try:
                            connection_task = asyncio.create_task(user_client.connect())
                            await asyncio.wait_for(connection_task, timeout=CONNECTION_TIMEOUT)
                            connected = True
                            logger.info(f"✅ {phone}: Connected successfully on attempt {attempt + 1}")
                            break
                        except asyncio.TimeoutError:
                            logger.warning(f"⏰ {phone}: Connection timeout on attempt {attempt + 1}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(3)
                        except Exception as conn_error:
                            logger.warning(f"⚠️ {phone}: Connection error on attempt {attempt + 1}: {conn_error}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(3)
                    
                    if not connected:
                        logger.warning(f"⚠️ {phone}: Failed to connect after {max_retries} attempts, but will try authorization anyway")
                        # Don't immediately fail - try to proceed
                    
                    # IMMEDIATE frozen check right after connection - but be more lenient
                    logger.info(f"🔍 {phone}: Running immediate frozen check after connection...")
                    try:
                        # Quick check for basic connectivity and authorization
                        is_authorized = await user_client.is_user_authorized()
                        if not is_authorized:
                            logger.warning(f"⚠️ {phone}: Not authorized after connection - will mark as potentially corrupted")
                            creation_error = "Not authorized"
                            # Don't immediately set user_client to None - continue to see if we can get more info
                        else:
                            # Get user info to check for immediate red flags
                            try:
                                me_quick = await asyncio.wait_for(user_client.get_me(), timeout=10)
                                if not me_quick:
                                    logger.warning(f"⚠️ {phone}: Cannot get user info after connection")
                                    creation_error = "Cannot get user info"
                                else:
                                    # Check for obvious restriction flags
                                    if hasattr(me_quick, 'restricted') and me_quick.restricted:
                                        logger.warning(f"❄️ {phone}: Account shows restricted=True immediately after connection")
                                        creation_error = "Account restricted"
                                    elif getattr(me_quick, 'deleted', False):
                                        logger.warning(f"❄️ {phone}: Account is deleted")
                                        creation_error = "Account deleted" 
                                    else:
                                        logger.info(f"✅ {phone}: Passed immediate frozen check")
                            except Exception as me_error:
                                logger.warning(f"⚠️ {phone}: Error getting user info: {me_error}")
                                creation_error = f"User info error: {me_error}"
                        
                    except Exception as quick_check_error:
                        logger.warning(f"⚠️ {phone}: Quick frozen check failed: {quick_check_error}")
                        error_str = str(quick_check_error).lower()
                        if any(keyword in error_str for keyword in ['restricted', 'frozen', 'banned', 'blocked', 'violation']):
                            logger.warning(f"❄️ {phone}: Quick check detected restriction: {quick_check_error}")
                            creation_error = f"Restriction detected: {quick_check_error}"
                        else:
                            # For other errors, be more lenient
                            logger.warning(f"⚠️ {phone}: Non-critical error in quick check: {quick_check_error}")
                            creation_error = f"Quick check error: {quick_check_error}"
                    
                except FloodWaitError as e:
                    logger.warning(f"⏳ {phone}: FloodWait {e.seconds}s - retrying...")
                    await asyncio.sleep(e.seconds)
                    try:
                        await user_client.connect()
                        logger.info(f"✅ {phone}: Connected successfully after FloodWait")
                    except Exception as e2:
                        logger.error(f"❌ {phone}: Failed to connect after FloodWait: {e2}")
                        creation_error = e2
                        user_client = None
                except (AuthKeyError, UserDeactivatedBanError, UserDeactivatedError, PhoneNumberBannedError) as e:
                    logger.error(f"🚫 {phone}: Critical session error - {e}. Marking as corrupted.")
                    remove_corrupted_during_load(str(session_file))
                    corrupted_files.append(session_file)
                    return
                except Exception as e:
                    creation_error = e
                    # Check for specific Telethon errors and provide better logging
                    error_str = str(e).lower()
                    if "authkey" in error_str or "auth key" in error_str:
                        logger.error(f"🔑 {phone}: Critical AuthKey error - {e}. Marking as corrupted.")
                        remove_corrupted_during_load(str(session_file))
                        corrupted_files.append(session_file)
                        return
                    elif "flood" in error_str:
                        logger.warning(f"⏳ {phone}: Flood error - {e}, will retry later")
                        creation_error = f"Flood error: {e}"
                        user_client = None
                    elif "network" in error_str or "connection" in error_str:
                        logger.warning(f"🌐 {phone}: Network error - {e}, session may be valid")
                        creation_error = f"Network error: {e}"
                        user_client = None
                    elif "timeout" in error_str:
                        logger.warning(f"⏰ {phone}: Timeout error - {e}, session may be valid")
                        creation_error = f"Timeout error: {e}"
                        user_client = None
                    else:
                        logger.warning(f"⚠️ {phone}: Client creation error - {e}, will check if session is actually corrupted")
                        creation_error = e
                        user_client = None

                # Instead of immediately failing if user_client is None, try to analyze the error
                if user_client is None and creation_error:
                    error_str = str(creation_error).lower()
                    
                    # Only mark as corrupted for truly critical errors
                    critical_errors = [
                        'authkey', 'auth key', 'session file is corrupted', 
                        'database is locked', 'no such table', 'sql error'
                    ]
                    
                    is_critical = any(critical in error_str for critical in critical_errors)
                    
                    if is_critical:
                        logger.error(f"❌ {phone}: Critical error detected, marking as corrupted: {creation_error}")
                        corrupted_files.append(session_file)
                        return
                    else:
                        # For non-critical errors, log but don't mark as corrupted
                        logger.warning(f"⚠️ {phone}: Non-critical error, session might be recoverable: {creation_error}")
                        logger.info(f"📝 {phone}: Skipping this session for now, it may work in future runs")
                        return

                # Verify authorization
                try:
                    logger.info(f"🔐 {phone}: Checking authorization...")
                    if await user_client.is_user_authorized():
                        # Get user info and validate session
                        me = await user_client.get_me()
                        if me:
                            # IMMEDIATELY and THOROUGHLY check for frozen status after connection
                            logger.info(f"❄️ {phone}: Running comprehensive frozen detection...")
                            try:
                                is_healthy, reason = await validate_session_health(user_client, phone)
                                if not is_healthy:
                                    from session_cleanup import _remove_session_bundle, FROZEN_DIR, upsert_health_frozen
                                    logger.warning(f"❄️ {phone}: Account detected as frozen/restricted during connection - {reason}")
                                    
                                    # Mark as frozen in system
                                    mark_account_frozen(phone, reason)
                                    upsert_health_frozen(phone, reason, source="connection_check")
                                    
                                    # Disconnect the client immediately
                                    try:
                                        if user_client.is_connected():
                                            await user_client.disconnect()
                                            logger.info(f"❄️ {phone}: Disconnected frozen client")
                                    except Exception as disconnect_error:
                                        logger.debug(f"❄️ {phone}: Error disconnecting: {disconnect_error}")
                                    
                                    # Move session files to frozen directory for complete cleanup
                                    try:
                                        _remove_session_bundle(phone, FROZEN_DIR)
                                        logger.info(f"❄️ {phone}: Session bundle moved to frozen directory")
                                    except Exception as move_error:
                                        logger.error(f"❄️ {phone}: Error moving session bundle: {move_error}")
                                    
                                    # Remove from any in-memory tracking
                                    if phone in user_clients:
                                        del user_clients[phone]
                                        logger.info(f"❄️ {phone}: Removed from active clients")
                                    
                                    # Mark as corrupted for this load session too
                                    corrupted_files.append(session_file)
                                    logger.info(f"❄️ {phone}: Complete frozen account cleanup finished")
                                    return
                                else:
                                    logger.info(f"✅ {phone}: Account health check passed - {reason}")
                            except Exception as health_error:
                                # If health check fails, treat as potentially frozen and clean up
                                logger.warning(f"⚠️ {phone}: Health check failed, treating as frozen - {health_error}")
                                
                                from session_cleanup import _remove_session_bundle, FROZEN_DIR, upsert_health_frozen
                                mark_account_frozen(phone, f"Health check failed: {health_error}")
                                upsert_health_frozen(phone, f"Health check failed: {health_error}", source="health_check_failure")
                                
                                try:
                                    if user_client.is_connected():
                                        await user_client.disconnect()
                                except Exception:
                                    pass
                                
                                try:
                                    _remove_session_bundle(phone, FROZEN_DIR)
                                except Exception:
                                    pass
                                
                                corrupted_files.append(session_file)
                                return

                            # Store the session successfully only if health check passed completely
                            if is_string_session:
                                session_string = session_content
                            else:
                                # For SQLite sessions, we need to convert to string for storage
                                session_string = user_client.session.save()
                            
                            user_clients[phone] = (user_client, session_string)
                            
                            # Attach event handlers for auto-view functionality
                            user_client.add_event_handler(auto_view_handler, events.NewMessage())
                            logger.info(f"🔗 {phone}: Event handlers attached for auto-view")
                            
                            logger.info(f"✅ {phone}: Session loaded successfully - {me.first_name} {me.last_name or ''}")

                            # Update metadata with user info if not already present
                            if not metadata.get('user_id'):
                                metadata.update({
                                    'user_id': me.id,
                                    'first_name': me.first_name or '',
                                    'last_name': me.last_name or '',
                                    'username': me.username or ''
                                })
                                save_session_metadata(phone, metadata)
                        else:
                            logger.error(f"❌ {phone}: Could not get user info despite authorization")
                            await user_client.disconnect()
                            corrupted_files.append(session_file)
                    else:
                        logger.error(f"🚫 {phone}: Session not authorized")
                        await user_client.disconnect()
                        corrupted_files.append(session_file)
                except FloodWaitError as e:
                    logger.warning(f"⏳ {phone}: FloodWait during authorization check: {e.seconds}s")
                    # Don't mark as corrupted for FloodWait, but disconnect for now
                    await user_client.disconnect()
                except (AuthKeyError, UserDeactivatedBanError, UserDeactivatedError, PhoneNumberBannedError) as e:
                    logger.error(f"🚫 {phone}: Critical authorization error - {e}")
                    await user_client.disconnect()
                    remove_corrupted_during_load(str(session_file))
                    corrupted_files.append(session_file)
                except Exception as e:
                    logger.error(f"❌ {phone}: Authorization check error - {e}")
                    await user_client.disconnect()
                    corrupted_files.append(session_file)
        except Exception as e:
            logger.exception(f"❌ Unexpected error processing session {session_file.stem}: {e}")
            try:
                if 'user_client' in locals() and user_client:
                    await user_client.disconnect()
            except Exception as disconnect_error:
                logger.error(f"Error disconnecting client for {session_file.stem}: {disconnect_error}")
            corrupted_files.append(session_file)
            return

    # Batch processing for session files with progress tracking
    async def process_in_batches(files, batch_size, batch_delay):
        total_batches = (len(files) + batch_size - 1) // batch_size
        successful_loads = 0

        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} sessions)")
            
            # Process batch concurrently
            tasks = [process_session_file(session_file) for session_file in batch]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful loads
            for session_file in batch:
                if session_file.stem in user_clients:
                    successful_loads += 1
            
            # Delay between batches to avoid overwhelming the API
            if batch_num < total_batches:
                logger.info(f"⏳ Waiting {batch_delay}s before next batch...")
                await asyncio.sleep(batch_delay)
        
        return successful_loads

    successful_count = await process_in_batches(session_files, BATCH_SIZE, BATCH_DELAY)
    
    # Final session loading summary
    total_sessions = len(session_files)
    failed_count = total_sessions - successful_count
    success_rate = (successful_count / total_sessions * 100) if total_sessions > 0 else 0
    
    logger.info(f"🎯 Session loading completed!")
    logger.info(f"📈 Final Results:")
    logger.info(f"   ✅ Successful: {successful_count}")
    logger.info(f"   ❌ Failed: {failed_count}")
    logger.info(f"   📊 Success Rate: {success_rate:.1f}%")
    logger.info(f"   � Total Processed: {total_sessions}")

    # Clean up corrupted files with retry mechanism
    if corrupted_files:
        logger.info(f"🗑️ Moving {len(corrupted_files)} corrupted files...")
        for corrupted_file in corrupted_files:
            try:
                remove_corrupted_during_load(str(corrupted_file))
                logger.info(f"   ↪️ Moved corrupted: {corrupted_file.name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to move {corrupted_file.name}: {e}")
    
    if corrupted_files:
        logger.warning(f"⚠️ {len(corrupted_files)} sessions were corrupted and moved to corrupted folder")

    logger.info(f"🎯 Session loading completed with {len(user_clients)} active sessions")

# Access control middleware
class AccessControlMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user_id = event.from_user.id if hasattr(event, 'from_user') and event.from_user else None
        if user_id:
            known_users.add(user_id)
        return await handler(event, data)

# Menu creation functions
async def create_main_menu(user_id: int) -> InlineKeyboardMarkup:
    """Create the main menu based on user permissions."""
    keyboard = [
        [InlineKeyboardButton(text="🔐 Account Management", callback_data="account_management")]
    ]
    
    # Add member management for owners and co-owners
    if user_id in OWNER_IDS or user_roles.get(user_id, {}).get('role') == 'co-owner':
        keyboard.append([InlineKeyboardButton(text="� Member Management", callback_data="member_management")])
        keyboard.append([InlineKeyboardButton(text="⚡ Auto Features", callback_data="auto_features")])
        keyboard.append([InlineKeyboardButton(text="🔐 Login Helper", callback_data="login_helper")])  # New Login Helper button
    
    # Add sudo access management for all users (permissions checked in handler)
    keyboard.append([InlineKeyboardButton(text="🔑 Sudo Access", callback_data="sudo_access")])
    keyboard.append([InlineKeyboardButton(text="❓ Help", callback_data="help")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_account_management_menu(user_id: int) -> InlineKeyboardMarkup:
    """Create the account management menu based on user permissions."""
    keyboard = [
        [InlineKeyboardButton(text="➕ Add Account", callback_data="addaccount")]
    ]
    
    # Add list accounts for sudo users
    if has_sudo_access(user_id):
        keyboard.append([InlineKeyboardButton(text="📋 List Accounts", callback_data="listaccounts")])
        keyboard.append([InlineKeyboardButton(text="ℹ️ Info", callback_data="info")])

    # Add remove account for owners only
    if user_id in OWNER_IDS:
        keyboard.append([InlineKeyboardButton(text="➖ Remove Account", callback_data="removeaccount")])

    keyboard.append([InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Remove all orphaned code below and add next function
# Menu creation functions
async def create_main_menu(user_id: int) -> InlineKeyboardMarkup:
    """Create the main menu based on user permissions."""
    keyboard = [
        [InlineKeyboardButton(text="🔐 Account Management", callback_data="account_management")]
    ]
    
    # Add member management for owners and co-owners
    if user_id in OWNER_IDS or user_roles.get(user_id, {}).get('role') == 'co-owner':
        keyboard.append([InlineKeyboardButton(text="👥 Member Management", callback_data="member_management")])
        keyboard.append([InlineKeyboardButton(text="⚡ Auto Features", callback_data="auto_features")])
        keyboard.append([InlineKeyboardButton(text="🔐 Login Helper", callback_data="login_helper")])  # New Login Helper button
    
    # Add sudo access management for all users (permissions checked in handler)
    keyboard.append([InlineKeyboardButton(text="🔑 Sudo Access", callback_data="sudo_access")])
    keyboard.append([InlineKeyboardButton(text="❓ Help", callback_data="help")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_account_management_menu(user_id: int) -> InlineKeyboardMarkup:
    """Create the account management menu based on user permissions."""
    keyboard = [
        [InlineKeyboardButton(text="➕ Add Account", callback_data="addaccount")]
    ]
    
    # Add list accounts for sudo users
    if has_sudo_access(user_id):
        keyboard.append([InlineKeyboardButton(text="📋 List Accounts", callback_data="listaccounts")])
        keyboard.append([InlineKeyboardButton(text="ℹ️ Info", callback_data="info")])
    
    # Add remove account for owners only
    if user_id in OWNER_IDS:
        keyboard.append([InlineKeyboardButton(text="➖ Remove Account", callback_data="removeaccount")])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_member_management_menu() -> InlineKeyboardMarkup:
    """Create the member management menu."""
    keyboard = [
        [InlineKeyboardButton(text="🚀 Mega Join (All Accounts)", callback_data="mega_join")],
        [InlineKeyboardButton(text="➕ Join (Limited Accounts)", callback_data="join")],
        [InlineKeyboardButton(text="🚫 Mega Leave (All Accounts)", callback_data="mega_leave")],
        [InlineKeyboardButton(text="➖ Leave (Limited Accounts)", callback_data="leave")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_auto_features_menu() -> InlineKeyboardMarkup:
    """Create the auto features menu."""
    status = "Enabled" if auto_features_enabled else "Disabled"
    keyboard = [
        [InlineKeyboardButton(text="👁️ Auto-View Management", callback_data="auto_view_menu")],
        [InlineKeyboardButton(text=f"🔄 Toggle Auto Views ({status})", callback_data="toggle_auto_features")],
        [InlineKeyboardButton(text="🔍 Verify Chats", callback_data="verify_chats")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_auto_react_menu() -> InlineKeyboardMarkup:
    """Create the auto-react management menu."""
    keyboard = [
        [InlineKeyboardButton(text="⚡ Mega Auto-React (All Accounts)", callback_data="mega_autoreact")],
        [InlineKeyboardButton(text="➕ Auto-React (Limited Accounts)", callback_data="autoreact")],
        [InlineKeyboardButton(text="🗑️ Mega Remove Auto-React (All)", callback_data="mega_removeautoreact")],
        [InlineKeyboardButton(text="➖ Remove Auto-React (Limited)", callback_data="removeautoreact")],
        [InlineKeyboardButton(text="📋 View Auto-React List", callback_data="reactlist")],
        [InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_auto_view_menu() -> InlineKeyboardMarkup:
    """Create the auto-view management menu."""
    keyboard = [
        [InlineKeyboardButton(text="👁️ Mega Auto-View (All Accounts)", callback_data="mega_autoview")],
        [InlineKeyboardButton(text="➕ Auto-View (Limited Accounts)", callback_data="autoview")],
        [InlineKeyboardButton(text="🗑️ Mega Remove Auto-View (All)", callback_data="mega_removeautoview")],
        [InlineKeyboardButton(text="➖ Remove Auto-View (Limited)", callback_data="removeautoview")],
        [InlineKeyboardButton(text="📋 View Auto-View List", callback_data="viewlist")],
        [InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_auto_dual_menu() -> InlineKeyboardMarkup:
    """Create the auto-dual management menu."""
    keyboard = [
        [InlineKeyboardButton(text="⚡ Mega Auto-Dual (All Accounts)", callback_data="mega_autodual")],
        [InlineKeyboardButton(text="➕ Auto-Dual (Limited Accounts)", callback_data="autodual")],
        [InlineKeyboardButton(text="🗑️ Mega Remove Auto-Dual (All)", callback_data="mega_removeautodual")],
        [InlineKeyboardButton(text="➖ Remove Auto-Dual (Limited)", callback_data="removeautodual")],
        [InlineKeyboardButton(text="📋 View Auto-Dual List", callback_data="autoduallist")],
        [InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_sudo_access_menu() -> InlineKeyboardMarkup:
    """Create the sudo access menu."""
    keyboard = [
        [InlineKeyboardButton(text="➕ Add Sudo User", callback_data="add_sudo_user")],
        [InlineKeyboardButton(text="➖ Remove Sudo User", callback_data="remove_sudo_user")],
        [InlineKeyboardButton(text="📝 List Sudo Users", callback_data="list_sudo_users")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_login_helper_menu() -> InlineKeyboardMarkup:
    """Create the login helper menu."""
    keyboard = [
        [InlineKeyboardButton(text="🔑 Login to Account", callback_data="login_to_account")],
        [InlineKeyboardButton(text="🔐 Manage 2FA Passwords", callback_data="manage_2fa")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

async def create_2fa_management_menu() -> InlineKeyboardMarkup:
    """Create the 2FA password management menu."""
    keyboard = [
        [InlineKeyboardButton(text="➕ Add 2FA Password", callback_data="add_2fa_password")],
        [InlineKeyboardButton(text="🔄 Update 2FA Password", callback_data="update_2fa_password")],
        [InlineKeyboardButton(text="❌ Remove 2FA Password", callback_data="remove_2fa_password")],
        [InlineKeyboardButton(text="🔙 Back", callback_data="login_helper")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Command handlers
@dp.message(Command(commands=['start', 'help']))
async def cmd_start(message: types.Message):
    """Handle /start and /help commands."""
    user_id = message.from_user.id
    keyboard = await create_main_menu(user_id)
    await message.reply(
        "🌟 **Telegram Meta Bot** 🌟\n\nSelect an option below:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.message(Command('listsudo'))
async def cmd_list_sudo(message: types.Message):
    """Handle /listsudo command."""
    user_id = message.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await message.reply("❌ You are not authorized to use this command.")
        return
    await show_sudo_list(message)

@dp.message(Command('cancel'))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Handle /cancel command."""
    user_id = message.from_user.id
    # Check if there's an ongoing operation to cancel
    event_obj = cancellation_events.get(user_id)
    if event_obj:
        event_obj.set()
        await message.reply(
            "✅ Operation canceled.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]])
        )
    else:
        # Check if there's an active state to cancel
        current_state = await state.get_state()
        if current_state is not None:
            await state.clear()
            await message.reply(
                "✅ Operation canceled.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
                ]])
            )
        else:
            await message.reply(
                "❌ No ongoing operation to cancel.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
                ]])
            )

@dp.message(Command('addaccount'))
async def cmd_add_account(message: types.Message, state: FSMContext):
    """Handle /addaccount command."""
    # Clear any existing state for this user
    await state.clear()
    # Set the new state
    await state.set_state(AccountStates.phone)
    await message.reply(
        "📱 Please enter the phone number to add (e.g., +1234567890):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_addaccount")
        ]])
    )

@dp.message(Command('listaccounts'))
async def cmd_list_accounts(message: types.Message):
    """Handle /listaccounts command."""
    if not has_sudo_access(message.from_user.id):
        await message.reply("❌ You are not authorized to use this command.")
        return
    await list_accounts(message)

@dp.message(Command('testfrozen'))
async def cmd_test_frozen(message: types.Message):
    """Handle /testfrozen command to test frozen account detection."""
    if not has_sudo_access(message.from_user.id):
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    if not user_clients:
        await message.reply("❌ No accounts loaded.")
        return
    
    # Get command arguments
    args = message.text.split()[1:] if message.text else []
    
    if args and len(args[0]) > 5:  # Likely a phone number
        # Test specific account
        phone = args[0]
        if phone not in user_clients:
            await message.reply(f"❌ Account {phone} not found.")
            return
        
        await message.reply(f"🔍 Testing account {phone} for frozen status...")
        
        try:
            result = await test_account_frozen_status(phone)
            await message.reply(f"📊 Test Result:\n{result}")
        except Exception as e:
            await message.reply(f"❌ Error testing account: {e}")
    else:
        # Test all accounts
        await message.reply(f"🔍 Testing all {len(user_clients)} accounts for frozen status...")
        
        healthy_count = 0
        frozen_count = 0
        frozen_list = []
        
        for phone in list(user_clients.keys())[:10]:  # Limit to first 10 to avoid spam
            try:
                client, _ = user_clients[phone]
                is_healthy, reason = await validate_session_health(client, phone)
                
                if is_healthy:
                    healthy_count += 1
                else:
                    frozen_count += 1
                    frozen_list.append(f"📱 {phone}: {reason}")
                    
            except Exception as e:
                frozen_count += 1
                frozen_list.append(f"📱 {phone}: Error - {e}")
        
        result_text = f"📊 Frozen Detection Test Results:\n"
        result_text += f"✅ Healthy: {healthy_count}\n"
        result_text += f"❄️ Frozen/Unhealthy: {frozen_count}\n"
        
        if frozen_list:
            result_text += f"\n❄️ Frozen Accounts:\n" + "\n".join(frozen_list[:5])
            if len(frozen_list) > 5:
                result_text += f"\n... and {len(frozen_list) - 5} more"
        
        await message.reply(result_text)

@dp.message(Command('stats'))
async def cmd_stats(message: types.Message):
    """Handle /stats command to show system statistics."""
    if not has_sudo_access(message.from_user.id):
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Get account statistics
    account_stats = get_account_statistics()
    
    # Get views_core statistics
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    hour_key = now.strftime("%Y%m%d%H")
    
    # Sample budget status for a few accounts
    budget_samples = []
    sample_phones = list(user_clients.keys())[:5]  # Sample first 5 accounts
    
    for phone in sample_phones:
        if not is_account_frozen(phone):
            budget_status = views_core.get_budget_status(phone, 0, today, hour_key)  # Use chat_id=0 as sample
            budget_samples.append(f"📱 {phone}: {budget_status['account']['used']}/{budget_status['account']['limit']} daily")
    
    # Quarantine statistics
    quarantine_count = sum(1 for phone in views_core.quarantine.keys() 
                          if views_core.is_quarantined(phone))
    
    # Flood wait statistics
    flood_count = sum(1 for phone, until in views_core.FLOOD_UNTIL.items() 
                     if int(time.time()) < until)
    
    stats_text = (
        f"📊 **System Statistics** 📊\n\n"
        f"**Accounts:**\n"
        f"• Total: {account_stats['total']}\n"
        f"• Active: {account_stats['active']}\n"
        f"• Frozen: {account_stats['frozen']}\n"
        f"• Connected: {account_stats['connected_active']}\n\n"
        f"**Views Core Status:**\n"
        f"• Quarantined: {quarantine_count}\n"
        f"• Flood Waited: {flood_count}\n"
        f"• Views Sent: {view_count}\n\n"
        f"**Auto Features:**\n"
        f"• Auto-View Chats: {len(AUTO_VIEW_CHATS)}\n"
        f"• Features Status: {'Enabled' if auto_features_enabled else 'Disabled'}\n\n"
        f"**Sample Budget Usage:**\n" + 
        ("\n".join(budget_samples) if budget_samples else "No active accounts to sample")
    )
    
    await message.reply(stats_text, parse_mode="Markdown")

@dp.message(Command('test_events'))
async def test_events_command(message: types.Message):
    """Test if event handlers are properly attached to clients."""
    user_id = message.from_user.id
    if not has_sudo_access(user_id):
        await message.reply(f"❌ Access denied. Please contact @{ADMIN_CONTACT_USERNAME} for assistance.")
        return
    
    response = "🔍 **Event Handler Status:**\n\n"
    
    for phone, (client, _) in user_clients.items():
        handler_count = len(client.list_event_handlers())
        connected = "✅ Connected" if client.is_connected() else "❌ Disconnected"
        response += f"• `{phone}`: {connected}, {handler_count} handlers\n"
    
    response += f"\n📊 Total clients: {len(user_clients)}"
    response += f"\n🎯 Auto-view chats: {len(AUTO_VIEW_CHATS)}"
    response += f"\n🔧 Auto features: {'Enabled' if auto_features_enabled else 'Disabled'}"
    
    await message.reply(response, parse_mode="Markdown")
async def cmd_info(message: types.Message):
    """Handle /info command."""
    user_id = message.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await message.reply("❌ You are not authorized to use this command.")
        return
    await show_account_info(message)

@dp.message(Command('login'))
async def cmd_login(message: types.Message, state: FSMContext):
    """Handle /login command."""
    user_id = message.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    if not user_clients:
        await message.reply("📭 No accounts added yet. Add accounts first.")
        return
    
    # Create a list of accounts with numbers
    accounts_list = "\n".join(f"{i+1}. {phone}" for i, phone in enumerate(user_clients.keys()))
    
    await state.set_state(LoginHelperStates.select_account)
    await message.reply(
        f"🔐 **Account Login Helper**\n\nSelect an account number to login:\n\n{accounts_list}\n\n"
         f"Send a number between 1 and {len(user_clients)} or type /cancel to abort.",
        parse_mode="Markdown"
    )

@dp.message(Command('megareact'))
async def cmd_mega_react(message: types.Message):
    """Handle /megareact command to make all accounts react to a message."""
    user_id = message.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    # Reaction feature has been removed
    await message.reply("⚠️ Reaction feature has been removed from this bot. Only auto-view is available.", parse_mode="Markdown")

# State handlers for account management
@dp.message(AccountStates.phone)
async def process_phone(message: types.Message, state: FSMContext):
    """Process phone number input."""
    phone = normalize_phone(message.text)
    
    if not phone.startswith('+') or len(phone) < 8:
        await message.reply(
            "❌ Invalid phone number format. Please use international format (e.g., +1234567890):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_addaccount")
            ]])
        )
        return

    if phone in user_clients:
        await message.reply(
            "⚠️ This phone number is already added. Please add a different one or cancel.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_addaccount")
            ]])
        )
        return

    # Store phone number and attempt to send code
    await state.update_data(phone=phone)
    await message.reply("⏳ Sending authentication code...")

    # Create client with InitConnectionRequest
    client = create_telegram_client_with_init_request(
        StringSession(),
        phone,
        None
    )
    try:
        # Check if client is connected
        if not client.is_connected():
            return False, "Not connected"

        # Try to get user info
        me = await asyncio.wait_for(client.get_me(), timeout=10)
        if not me:
            return False, "Cannot get user info"

        # Check for deleted account
        if getattr(me, 'deleted', False):
            return False, "Account deleted"

        # Check for restricted/frozen/banned/suspended status
        status_str = str(me).lower()
        if any(keyword in status_str for keyword in ['restricted', 'frozen', 'banned', 'suspended']):
            mark_account_frozen(phone, f"Detected status: {status_str}")
            return False, f"Account frozen/restricted: {status_str}"

        # Optionally, check for Telethon errors by making a simple request
        try:
            await client(GetAuthorizationsRequest())
        except (UserDeactivatedBanError, UserDeactivatedError, PhoneNumberBannedError) as e:
            mark_account_frozen(phone, str(e))
            return False, f"Account banned/deactivated: {str(e)}"
        except FloodWaitError as e:
            logger.warning(f"FloodWaitError for {phone}: waiting {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return False, f"FloodWait: {e.seconds}s"
        except Exception as e:
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['restricted', 'frozen', 'banned', 'suspended']):
                mark_account_frozen(phone, str(e))
                return False, f"Account frozen/restricted: {str(e)}"

        return True, "Healthy"

    except FloodWaitError as e:
        logger.warning(f"FloodWaitError for {phone}: waiting {e.seconds} seconds")
        await asyncio.sleep(e.seconds)
        return False, f"FloodWait: {e.seconds}s"
    except (UserDeactivatedBanError, UserDeactivatedError, PhoneNumberBannedError) as e:
        mark_account_frozen(phone, str(e))
        return False, f"Account banned/deactivated: {str(e)}"
    except Exception as e:
        logger.exception(f"Error validating session health for {phone}: {e}")
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['restricted', 'frozen', 'banned', 'suspended']):
            mark_account_frozen(phone, str(e))
            return False, f"Account frozen/restricted: {str(e)}"
        return False, f"Error: {str(e)}"
        session_string = client.session.save()
        
        # Save session to file
        session_file = SESSIONS_DIR / f"{phone}.session"
        with open(session_file, 'w') as f:
            f.write(session_string)
            
        # Save session metadata to JSON file for consistency
        account_params = get_consistent_account_parameters(phone, None)
        metadata = {
            'session_file': phone,
            'phone': phone,
            'app_id': account_params['app_id'],
            'app_hash': account_params['app_hash'],
            'sdk': account_params['system_version'],
            'app_version': account_params['app_version'],
            'device': account_params['device_model'],
            'lang_code': account_params['lang_code'],
            'system_lang_code': account_params['system_lang_code'],
            'lang_pack': account_params['lang_pack'],
            'ipv6': account_params['ipv6']
        }
        save_session_metadata(phone, metadata)
            
        user_clients[phone] = (client, session_string)
        
        # Attach only auto_view_handler for the new client (auto_react is disabled)
        client.add_event_handler(auto_view_handler, events.NewMessage())
        logger.info(f"Attached event handlers for new client {phone}")

        await state.clear()
        del cancellation_events[message.from_user.id]
        await message.reply(
            f"✅ Account {phone} added successfully!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]])
        )
        logger.info(f"Account {phone} added and session saved.")
    except SessionPasswordNeededError:
        await state.set_state(AccountStates.password)
        await message.reply(
            "🔒 Two-factor authentication is enabled. Please enter your cloud password:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_addaccount")
            ]])
        )
    except FloodWaitError as e:
        await state.clear()
        if client.is_connected():
            await client.disconnect()
        del cancellation_events[message.from_user.id]
        await message.reply(
            f"❌ Flood wait: Please try again after {e.seconds} seconds. Operation cancelled."
        )
        logger.error(f"FloodWaitError when signing in {phone}: {e}")
    except Exception as e:
        await state.clear()
        if client.is_connected():
            await client.disconnect()
        del cancellation_events[message.from_user.id]
        await message.reply(
            f"❌ Failed to sign in with OTP for {phone}: {e}\n\nPlease try again.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]])
        )
        logger.error(f"Error signing in with OTP for {phone}: {e}")

@dp.message(AccountStates.password)
async def process_password(message: types.Message, state: FSMContext):
    """Process 2FA password input."""
    data = await state.get_data()
    phone = data['phone']
    client = data['client']
    password = message.text.strip()

    try:
        # Check for cancellation before processing password
        if cancellation_events.get(message.from_user.id) and cancellation_events[message.from_user.id].is_set():
            await state.clear()
            if client.is_connected():
                await client.disconnect()
            del cancellation_events[message.from_user.id]
            return

        await client.sign_in(password=password)
        session_string = client.session.save()
        
        # Save session to file
        session_file = SESSIONS_DIR / f"{phone}.session"
        with open(session_file, 'w') as f:
            f.write(session_string)
            
        # Save session metadata to JSON file for consistency
        account_params = get_consistent_account_parameters(phone, None)
        metadata = {
            'session_file': phone,
            'phone': phone,
            'app_id': account_params['app_id'],
            'app_hash': account_params['app_hash'],
            'sdk': account_params['system_version'],
            'app_version': account_params['app_version'],
            'device': account_params['device_model'],
            'lang_code': account_params['lang_code'],
            'system_lang_code': account_params['system_lang_code'],
            'lang_pack': account_params['lang_pack'],
            'ipv6': account_params['ipv6']
        }
        save_session_metadata(phone, metadata)
        
        # Save 2FA password for future reference
        save_2fa_password(phone, password)
            
        user_clients[phone] = (client, session_string)
        
        # Attach only auto_view_handler for the new client (auto_react is disabled)
        client.add_event_handler(auto_view_handler, events.NewMessage())
        logger.info(f"Attached event handlers for new client {phone}")

        await state.clear()
        del cancellation_events[message.from_user.id]
        await message.reply(
            f"✅ Account {phone} added successfully with 2FA!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]])
        )
        logger.info(f"Account {phone} added with 2FA and session saved.")
    except FloodWaitError as e:
        await state.clear()
        if client.is_connected():
            await client.disconnect()
        del cancellation_events[message.from_user.id]
        await message.reply(
            f"❌ Flood wait: Please try again after {e.seconds} seconds. Operation cancelled."
        )
        logger.error(f"FloodWaitError when signing in with password for {phone}: {e}")
    except Exception as e:
        await state.clear()
        if client.is_connected():
            await client.disconnect()
        del cancellation_events[message.from_user.id]
        await message.reply(
            f"❌ Failed to sign in with password for {phone}: {e}\n\nPlease try again.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]])
        )
        logger.error(f"Error signing in with password for {phone}: {e}")





@dp.callback_query(lambda c: c.data == 'cancel_addaccount')
async def callback_cancel_addaccount(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle cancellation of account addition."""
    user_id = callback_query.from_user.id
    data = await state.get_data()
    client = data.get('client')

    if client and client.is_connected():
        await client.disconnect()
    
    # Set the event to signal cancellation to the state handler
    if user_id in cancellation_events:
        cancellation_events[user_id].set()

    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="✅ Account addition cancelled.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
        ]])
    )
    await callback_query.answer()

@dp.message(Command('removeaccount'))
async def cmd_remove_account(message: types.Message, state: FSMContext):
    """Handle /removeaccount command."""
    if message.from_user.id not in OWNER_IDS:
        await message.reply("❌ You are not authorized to use this command.")
        return
    
    if not user_clients:
        await message.reply("🚫 No accounts currently loaded.")
        return

    # List accounts for removal
    account_list = "\n".join([f"- {phone}" for phone in user_clients.keys()])
    await state.set_state(RemoveAccountStates.phone)
    await message.reply(
        f"⬇️ Currently loaded accounts:\n{account_list}\n\n"
        "📱 Please enter the phone number of the account to remove (e.g., +1234567890):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_removeaccount")
        ]])
    )

@dp.message(RemoveAccountStates.phone)
async def process_remove_phone(message: types.Message, state: FSMContext):
    """Process phone number for removal."""
    raw_phone = message.text.strip()
    phone_to_remove = normalize_phone(raw_phone)

    if phone_to_remove not in user_clients:
        await message.reply(
            "❌ Account not found. Please ensure you enter the correct phone number including the country code (e.g., +1234567890).\n",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_removeaccount")
            ]])
        )
        return

    try:
        client_to_disconnect, _ = user_clients.pop(phone_to_remove)
        if client_to_disconnect.is_connected():
            await client_to_disconnect.disconnect()
            logger.info(f"Disconnected client {phone_to_remove}")
        
        # Delete session file
        session_file = SESSIONS_DIR / f"{phone_to_remove}.session"
        if session_file.exists():
            session_file.unlink()
            logger.info(f"Deleted session file for {phone_to_remove}")

        await state.clear()
        await message.reply(
            f"✅ Account {phone_to_remove} removed successfully!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]])
        )
    except Exception as e:
        logger.error(f"Error removing account {phone_to_remove}: {e}")
        await state.clear()
        await message.reply(
            f"❌ Failed to remove account {phone_to_remove}: {e}\n\nPlease try again.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]])
        )

@dp.callback_query(lambda c: c.data == 'cancel_removeaccount')
async def callback_cancel_removeaccount(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle cancellation of account removal."""
    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="✅ Account removal cancelled.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
        ]])
    )
    await callback_query.answer()

async def list_accounts(message: types.Message):
    """List all loaded accounts with their status."""
    if not user_clients:
        await message.reply("🚫 No accounts currently loaded.")
        return

    stats = get_account_statistics()
    account_list = f"⬇️ **Loaded Accounts:** (Total: {stats['total']}, Active: {stats['active']}, Frozen: {stats['frozen']})\n\n"
    
    for phone, (client, _) in user_clients.items():
        connection_status = "Connected" if client.is_connected() else "Disconnected"
        status_str = "🟢 Active"
        reason = ""
        if is_account_frozen(phone):
            status_str = "🔴 Frozen"
            if phone in account_status:
                reason = account_status[phone].get('reason', 'Unknown')
        if reason:
            account_list += f"- `{phone}` | {connection_status} | {status_str} ({reason})\n"
        else:
            account_list += f"- `{phone}` | {connection_status} | {status_str}\n"

    await message.reply(account_list, parse_mode="Markdown")

async def show_account_info(message: types.Message):
    """Show information about loaded accounts and bot stats."""
    
    stats = get_account_statistics()

    info_text = (
        "📊 **Bot Statistics & Account Info**\n\n"
        f"Total Accounts Loaded: `{stats['total']}`\n"
        f"🟢 Active Accounts: `{stats['active']}`\n"
        f"🔴 Frozen Accounts: `{stats['frozen']}`\n"
        f"Connected Active Accounts: `{stats['connected_active']}`\n\n"
        f"Views Sent (since last restart): `{view_count}`\n"
        f"Posts (not tracked yet): `{post_count}`\n\n"
        "🔗 You can manage your accounts from the Account Management menu.\n"
        "ℹ️ Only active (non-frozen) accounts are used for operations."
    )
    await message.reply(info_text, parse_mode="Markdown")

# Callback query handlers for main menu
@dp.callback_query(lambda c: c.data == 'main_menu')
async def callback_main_menu(callback_query: types.CallbackQuery):
    """Handle main menu callback."""
    user_id = callback_query.from_user.id
    keyboard = await create_main_menu(user_id)
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🌟 **Telegram Meta Bot** 🌟\n\nSelect an option below:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'account_management')
async def callback_account_management(callback_query: types.CallbackQuery):
    """Handle account management menu callback."""
    user_id = callback_query.from_user.id
    keyboard = await create_account_management_menu(user_id)
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🔐 **Account Management**\n\nChoose an action:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'addaccount')
async def callback_addaccount(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle add account callback."""
    await state.set_state(AccountStates.phone)
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="📱 Please enter the phone number to add (e.g., +1234567890):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_addaccount")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'listaccounts')
async def callback_listaccounts(callback_query: types.CallbackQuery):
    """Handle list accounts callback."""
    if not has_sudo_access(callback_query.from_user.id):
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    if not user_clients:
        await callback_query.answer("🚫 No accounts currently loaded.", show_alert=True)
        return

    # Get all account phones and sort them
    all_phones = sorted(user_clients.keys())
    total_accounts = len(all_phones)
    
    # Telegram message limit is 4096 chars, so split if needed
    header_text = f"⬇️ **Loaded Accounts:** (Total: {total_accounts})\n\n"
    footer_text = f"\n\nShowing {{start}}-{{end}} of {total_accounts} accounts."
    account_lines = [f"- {phone}" for phone in all_phones]
    max_length = 4000
    chunks = []
    current_chunk = header_text
    start_num = 1
    for idx, line in enumerate(account_lines, 1):
        if len(current_chunk) + len(line) + len(footer_text.format(start=start_num, end=idx)) + 2 > max_length:
            # Add footer to current chunk
            current_chunk += footer_text.format(start=start_num, end=idx-1)
            chunks.append(current_chunk)
            current_chunk = header_text
            start_num = idx
        current_chunk += line + "\n"
    if current_chunk.strip() != header_text.strip():
        current_chunk += footer_text.format(start=start_num, end=total_accounts)
        chunks.append(current_chunk)
    # Send all chunks sequentially
    for chunk in chunks:
        await bot.send_message(
            chat_id=callback_query.message.chat.id,
            text=chunk,
            parse_mode="Markdown"
        )
        await asyncio.sleep(0.5)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'info')
async def callback_info(callback_query: types.CallbackQuery):
    """Handle info callback."""
    if not has_sudo_access(callback_query.from_user.id):
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    stats = get_account_statistics()

    info_text = (
        "📊 **Bot Statistics & Account Info**\n\n"
        f"Total Accounts Loaded: `{stats['total']}`\n"
        f"🟢 Active Accounts: `{stats['active']}`\n"
        f"🔴 Frozen Accounts: `{stats['frozen']}`\n"
        f"Connected Active Accounts: `{stats['connected_active']}`\n\n"
        f"Views Sent (since last restart): `{view_count}`\n"
        f"Posts (not tracked yet): `{post_count}`\n\n"
        "🔗 You can manage your accounts from the Account Management menu.\n"
        "ℹ️ Only active (non-frozen) accounts are used for operations."
    )
    await bot.send_message(
        chat_id=callback_query.message.chat.id,
        text=info_text,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'removeaccount')
async def callback_removeaccount(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle remove account callback."""
    if callback_query.from_user.id not in OWNER_IDS:
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    if not user_clients:
        await callback_query.answer("🚫 No accounts currently loaded.", show_alert=True)
        return

    account_list = "\n".join([f"- {phone}" for phone in user_clients.keys()])
    await state.set_state(RemoveAccountStates.phone)
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"⬇️ Currently loaded accounts:\n{account_list}\n\n"
             "📱 Please enter the phone number of the account to remove (e.g., +1234567890):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_removeaccount")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'member_management')
async def callback_member_management(callback_query: types.CallbackQuery):
    """Handle member management menu callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    keyboard = await create_member_management_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="👥 **Member Management**\n\nChoose an action:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'auto_features')
async def callback_auto_features(callback_query: types.CallbackQuery):
    """Handle auto features menu callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return

    keyboard = await create_auto_features_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="⚡ **Auto Features Management**\n\nConfigure automatic reactions and views:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'auto_react_menu')
async def callback_auto_react_menu(callback_query: types.CallbackQuery):
    """Handle auto-react management menu callback - feature removed."""
    await callback_query.answer("🚧 Auto-react feature has been removed from this bot version.", show_alert=True)

@dp.callback_query(lambda c: 'react' in c.data and c.data != 'auto_react_menu')
async def callback_auto_react_features(callback_query: types.CallbackQuery):
    """Handle all auto-react feature callbacks - feature removed."""
    await callback_query.answer("🚧 Auto-react feature has been removed from this bot version.", show_alert=True)

@dp.callback_query(lambda c: c.data == 'auto_view_menu')
async def callback_auto_view_menu(callback_query: types.CallbackQuery):
    """Handle auto-view management menu callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    keyboard = await create_auto_view_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="👁️ **Auto-View Management**\n\nConfigure automatic message views:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'auto_dual_menu')
async def callback_auto_dual_menu(callback_query: types.CallbackQuery):
    """Handle auto-dual management menu callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    keyboard = await create_auto_dual_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="⚡ **Auto-Dual Management**\n\nConfigure automatic reactions and views:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'toggle_auto_features')
async def callback_toggle_auto_features(callback_query: types.CallbackQuery):
    """Toggle the global auto features enabled/disabled status."""
    global auto_features_enabled
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return

    auto_features_enabled = not auto_features_enabled
    status_text = "Enabled" if auto_features_enabled else "Disabled"
    logger.info(f"Auto features toggled to: {status_text}")
    await callback_query.answer(f"Auto Features are now {status_text}.", show_alert=True)

    keyboard = await create_auto_features_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="⚡ **Auto Features Management**\n\nConfigure automatic reactions and views:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.callback_query(lambda c: c.data == 'verify_chats')
async def callback_verify_chats(callback_query: types.CallbackQuery):
    """Verify the current auto-feature chat lists."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return

    response_text = "📋 **Current Auto Feature Chats:**\n\n"
    
    if AUTO_REACT_CHATS:
        response_text += "🔥 **Auto-React Chats:**\n"
        for chat_id in AUTO_REACT_CHATS:
            count = AUTO_REACT_COUNTS.get(chat_id, "All")
            response_text += f"- `{chat_id}` (Accounts: {count})\n"
    else:
        response_text += "🔥 No Auto-React Chats configured.\n"

    response_text += "\n"

    if AUTO_VIEW_CHATS:
        response_text += "👁️ **Auto-View Chats:**\n"
        for chat_id in AUTO_VIEW_CHATS:
            count = AUTO_VIEW_COUNTS.get(chat_id, "All")
            response_text += f"- `{chat_id}` (Accounts: {count})\n"
    else:
        response_text += "👁️ No Auto-View Chats configured.\n"
        
    response_text += "\n"
    
    if AUTO_DUAL_CHATS:
        response_text += "⚡ **Auto-Dual Chats:**\n"
        for chat_id in AUTO_DUAL_CHATS:
            count = AUTO_DUAL_COUNTS.get(chat_id, "All")
            response_text += f"- `{chat_id}` (Accounts: {count})\n"
    else:
        response_text += "⚡ No Auto-Dual Chats configured.\n"

    await bot.send_message(
        chat_id=callback_query.message.chat.id,
        text=response_text,
        parse_mode="Markdown"
    )
    await callback_query.answer()

# Login Helper callbacks
@dp.callback_query(lambda c: c.data == 'login_helper')
async def callback_login_helper(callback_query: types.CallbackQuery):
    """Handle login helper menu callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    keyboard = await create_login_helper_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🔐 **Login Helper**\n\nChoose an option:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'login_to_account')
async def callback_login_to_account(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle login to account callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    if not user_clients:
        await callback_query.answer("📭 No accounts added yet. Add accounts first.", show_alert=True)
        return
    
    # Create a list of accounts with numbers
    accounts_list = "\n".join(f"{i+1}. {phone}" for i, phone in enumerate(user_clients.keys()))
    
    await state.set_state(LoginHelperStates.select_account)
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"🔐 **Account Login Helper**\n\nSelect an account number to login:\n\n{accounts_list}\n\n"
             f"Send a number between 1 and {len(user_clients)} or type /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_login_helper")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'manage_2fa')
async def callback_manage_2fa(callback_query: types.CallbackQuery):
    """Handle manage 2FA passwords callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    # Load current passwords
    passwords_file = SESSIONS_DIR / "passwords.json"
    passwords = {}
    if passwords_file.exists():
        try:
            with passwords_file.open('r') as f:
                passwords = json.load(f)
        except Exception as e:
            logger.error(f"Error loading passwords: {e}")
    
    # Create message with accounts that have 2FA
    message = "🔐 **2FA Password Management**\n\n"
    
    if not passwords:
        message += "No 2FA passwords stored yet.\n\n"
    else:
        message += f"Accounts with stored 2FA passwords: {len(passwords)}\n\n"
        for i, (phone, _) in enumerate(passwords.items(), 1):
            message += f"{i}. {phone}\n"
    
    keyboard = await create_2fa_management_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=message,
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'add_2fa_password')
async def callback_add_2fa_password(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle add 2FA password callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    await state.set_state(PasswordManagementStates.phone)
    await state.update_data(action='add_2fa')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="📱 Enter the phone number to add 2FA password for (e.g., +1234567890):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_2fa")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'update_2fa_password')
async def callback_update_2fa_password(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle update 2FA password callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    # Load current passwords
    passwords_file = SESSIONS_DIR / "passwords.json"
    passwords = {}
    if passwords_file.exists():
        try:
            with passwords_file.open('r') as f:
                passwords = json.load(f)
        except Exception as e:
            logger.error(f"Error loading passwords: {e}")
    
    if not passwords:
        await callback_query.answer("❌ No 2FA passwords stored yet.", show_alert=True)
        return
    
    # List phones with 2FA passwords
    phones_list = "\n".join(f"{i+1}. {phone}" for i, phone in enumerate(passwords.keys()))
    
    await state.set_state(PasswordManagementStates.phone)
    await state.update_data(action='update_2fa')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"📱 Select a phone number to update 2FA password:\n\n{phones_list}\n\nEnter the number or full phone:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_2fa")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'remove_2fa_password')
async def callback_remove_2fa_password(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle remove 2FA password callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    # Load current passwords
    passwords_file = SESSIONS_DIR / "passwords.json"
    passwords = {}
    if passwords_file.exists():
        try:
            with passwords_file.open('r') as f:
                passwords = json.load(f)
        except Exception as e:
            logger.error(f"Error loading passwords: {e}")
    
    if not passwords:
        await callback_query.answer("❌ No 2FA passwords stored yet.", show_alert=True)
        return
    
    # List phones with 2FA passwords
    phones_list = "\n".join(f"{i+1}. {phone}" for i, phone in enumerate(passwords.keys()))
    
    await state.set_state(PasswordManagementStates.phone)
    await state.update_data(action='remove_2fa')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"📱 Select a phone number to remove 2FA password:\n\n{phones_list}\n\nEnter the number or full phone:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_2fa")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'cancel_2fa')
async def callback_cancel_2fa(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle cancellation of 2FA operations."""
    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="✅ 2FA operation canceled.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back", callback_data="manage_2fa")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'cancel_login_helper')
async def callback_cancel_login_helper(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle cancellation of login helper."""
    data = await state.get_data()
    phone = data.get('login_phone')
    client = data.get('login_client')
    
    # Clean up any event handlers
    if client:
        for handler in client.list_event_handlers():
            if handler[0].__name__ in ['otp_handler', 'password_handler']:
                client.remove_event_handler(handler[0])
    
    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="✅ Login helper operation canceled.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back", callback_data="login_helper")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

# Login Helper message handlers
@dp.message(LoginHelperStates.select_account)
async def process_login_account_selection(message: types.Message, state: FSMContext):
    """Process account selection for login."""
    user_id = message.from_user.id
    try:
        selection = int(message.text.strip())
        if selection < 1 or selection > len(user_clients):
            await message.reply(f"❌ Invalid selection. Please enter a number between 1 and {len(user_clients)}.")
            return
        
        # Get the selected account
        phone = list(user_clients.keys())[selection - 1]
        client, _ = user_clients[phone]
        
        # Store the selected account info
        await state.update_data(
            login_phone=phone,
            login_client=client,
            timestamp=asyncio.get_event_loop().time()
        )
        await state.set_state(LoginHelperStates.waiting_for_otp)
        
        # Set up OTP listener for this account
        await setup_otp_listener(user_id, phone, client, state)
        
        # Get F2F data if available
        f2f_info = get_f2f_data(phone)
        f2f_status = "✅ F2F data available" if f2f_info else "❌ No F2F data stored"
        
        await message.reply(
            f"📱 **Login Information**\n\n"
            f"**Phone Number**: `{phone}`\n\n"
            f"**F2F Status**: {f2f_status}\n\n"
            f"Use this phone number to log in to your desired service.\n"
            f"I'm now monitoring for OTP codes sent to this account.\n"
            f"When you initiate login elsewhere, I'll detect and show the OTP here.\n\n"
            f"Waiting for OTP... (will timeout after 5 minutes)\n"
            f"Type /cancel to stop waiting.",
            parse_mode="Markdown"
        )
        
        # Set a timeout for OTP waiting
        asyncio.create_task(timeout_otp_wait(user_id, 300, state))  # 5 minutes timeout
        
    except ValueError:
        await message.reply("❌ Please enter a valid number.")
    except Exception as e:
        await message.reply(f"❌ An error occurred: {str(e)}")
        logger.error(f"Error in login account selection: {e}")
        await state.clear()

@dp.message(PasswordManagementStates.phone)
async def process_2fa_phone(message: types.Message, state: FSMContext):
    """Process phone number for 2FA operations."""
    user_id = message.from_user.id
    text = message.text.strip()
    
    # Load current passwords
    passwords_file = SESSIONS_DIR / "passwords.json"
    passwords = {}
    if passwords_file.exists():
        try:
            with passwords_file.open('r') as f:
                passwords = json.load(f)
        except Exception as e:
            logger.error(f"Error loading passwords: {e}")
    
    # Check if input is a number (index) or phone number
    phone = None
    if text.isdigit():
        idx = int(text) - 1
        if 0 <= idx < len(passwords):
            phone = list(passwords.keys())[idx]
    else:
        phone = normalize_phone(text)
    
    if not phone:
        await message.reply(
            "❌ Invalid selection. Please enter a valid number or phone number.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_2fa")
            ]]),
            parse_mode="Markdown"
        )
        return
    
    data = await state.get_data()
    action = data.get('action')
    
    if action in ['add_2fa', 'update_2fa']:
        await state.update_data(phone=phone)
        await state.set_state(PasswordManagementStates.password)
        await message.reply(
            f"🔐 Enter the 2FA password for {phone}:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_2fa")
            ]]),
            parse_mode="Markdown"
        )
    elif action == 'remove_2fa':
        if phone in passwords:
            del passwords[phone]
            try:
                with passwords_file.open('w') as f:
                    json.dump(passwords, f)
                await message.reply(
                    f"✅ 2FA password for {phone} removed successfully.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 Back", callback_data="manage_2fa")
                    ]]),
                    parse_mode="Markdown"
                )
            except Exception as e:
                await message.reply(
                    f"❌ Error removing password: {e}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 Back", callback_data="manage_2fa")
                    ]]),
                    parse_mode="Markdown"
                )
        else:
            await message.reply(
                f"❌ No 2FA password found for {phone}.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 Back", callback_data="manage_2fa")
                ]]),
                parse_mode="Markdown"
            )
        await state.clear()

@dp.message(PasswordManagementStates.password)
async def process_2fa_password(message: types.Message, state: FSMContext):
    """Process 2FA password input."""
    password = message.text.strip()
    data = await state.get_data()
    phone = data.get('phone')
    action = data.get('action')
    
    # Save the password
    result = save_2fa_password(phone, password)
    
    if result:
        action_text = "added" if action == 'add_2fa' else "updated"
        await message.reply(
            f"✅ 2FA password for {phone} {action_text} successfully.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="manage_2fa")
            ]]),
            parse_mode="Markdown"
        )
    else:
        await message.reply(
            f"❌ Error saving password for {phone}.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="manage_2fa")
            ]]),
            parse_mode="Markdown"
        )
    
    await state.clear()

# Login Helper utility functions
async def setup_otp_listener(user_id, phone, client, state):
    """Set up a listener for OTP codes."""
    # Make sure client is connected
    if not await ensure_connected(client, phone):
        return
    
    # Create a handler for incoming messages
    @client.on(events.NewMessage(from_users=777000))  # 777000 is Telegram's official account
    async def otp_handler(event):
        current_state = await state.get_state()
        if current_state != 'LoginHelperStates:waiting_for_otp':
            return
        
        message_text = event.message.text
        # Look for login code patterns in the message
        otp_match = re.search(r'[Cc]ode:?\s*(\d{5})', message_text)
        if not otp_match:
            otp_match = re.search(r'(\d{5})[^\d]', message_text)
        
        if otp_match:
            otp_code = otp_match.group(1)
            logger.info(f"OTP detected for {phone}: {otp_code}")
            
            # Check if 2FA is enabled
            has_2fa = False
            try:
                me = await client.get_me()
                auth_info = await client(GetAuthorizationsRequest())
                has_2fa = auth_info.authorizations[0].password_pending
            except Exception as e:
                logger.error(f"Error checking 2FA status: {e}")
                has_2fa = False  # Assume no 2FA if we can't check
            
            # Get F2F data if available
            f2f_info = get_f2f_data(phone)
            
            # Send OTP to user
            await bot.send_message(
                user_id,
                f"🔐 **Login Code Detected!**\n\n"
                f"**Phone**: `{phone}`\n"
                f"**OTP Code**: `{otp_code}`\n\n"
                f"{'⚠️ This account has 2FA enabled. Waiting for password verification...' if has_2fa else '✅ Login should now complete automatically if no 2FA is set.'}"
                "\n\n**F2F Data Available**: Yes" if f2f_info else "",
                parse_mode="Markdown"
            )
            
            # If 2FA is enabled, wait for password verification message
            if has_2fa:
                await state.set_state(LoginHelperStates.waiting_for_2fa)
                
                # Set up 2FA detection
                @client.on(events.NewMessage(from_users=777000))
                async def password_handler(event):
                    current_state = await state.get_state()
                    if current_state != 'LoginHelperStates:waiting_for_2fa':
                        return
                    
                    message_text = event.message.text
                    if "password" in message_text.lower() or "2fa" in message_text.lower() or "two-factor" in message_text.lower():
                        # Try to extract password from stored sessions
                        stored_password = get_2fa_password(phone)
                        
                        # Get F2F data
                        f2f_info = get_f2f_data(phone)
                        f2f_password = f2f_info.get('2fa_password') if f2f_info else None
                        
                        # Use F2F password if available, otherwise use stored password
                        password = f2f_password or stored_password or "Unknown - Please enter your 2FA password manually"
                        
                        password_message = (
                            f"🔐 **2FA Password Required**\n\n"
                            f"**Phone**: `{phone}`\n"
                            f"**Password**: `{password}`\n\n"
                        )
                        
                        if f2f_password or stored_password:
                            password_message += "Enter this password to complete your login."
                        else:
                            password_message += "⚠️ No stored password found. Please enter your 2FA password manually."
                        
                        await bot.send_message(
                            user_id,
                            password_message,
                            parse_mode="Markdown"
                        )
                        
                        # Clean up state
                        await state.set_state(None)
                        client.remove_event_handler(password_handler)
                
                # Set timeout for 2FA wait
                asyncio.create_task(timeout_otp_wait(user_id, 300, state))  # 5 more minutes for 2FA
            else:
                # No 2FA, login is complete
                await state.set_state(None)
                await bot.send_message(
                    user_id,
                    f"✅ **Login Process Complete**\n\n"
                    f"You should now be logged in with account {phone}.\n\n"
                    f"Click below to return to the main menu.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
                    ]]),
                    parse_mode="Markdown"
                )
                
                # Clean up
                client.remove_event_handler(otp_handler)

async def timeout_otp_wait(user_id, timeout_seconds, state):
    """Handle timeout for OTP waiting."""
    await asyncio.sleep(timeout_seconds)
    current_state = await state.get_state()
    if current_state in ['LoginHelperStates:waiting_for_otp', 'LoginHelperStates:waiting_for_2fa']:
        data = await state.get_data()
        phone = data.get('login_phone')
        client = data.get('login_client')
        
        await bot.send_message(
            user_id,
            f"⏱️ **Login Helper Timeout**\n\n"
            f"No {'OTP' if current_state == 'LoginHelperStates:waiting_for_otp' else '2FA confirmation'} was detected within {timeout_seconds} seconds.\n\n"
            f"The login helper has been deactivated. Please try again if needed.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
            ]]),
            parse_mode="Markdown"
        )
        
        # Clean up any event handlers
        if client:
            for handler in client.list_event_handlers():
                if handler[0].__name__ in ['otp_handler', 'password_handler']:
                    client.remove_event_handler(handler[0])
        
        await state.clear()

@dp.callback_query(lambda c: c.data == 'mega_autoreact')
async def callback_mega_autoreact(callback_query: types.CallbackQuery, state: FSMContext):
    """Set up mega auto-react."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(MegaAutoFeatureStates.chat_ids)
    await state.update_data(feature_type='react')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🔥 **Mega Auto-React (All Accounts)**\n\n"
             "Please send the chat ID(s) where all accounts should automatically react to new messages.\n"
             "You can send multiple IDs separated by commas (e.g., `-1001234567890, -1009876543210`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'autoreact')
async def callback_autoreact(callback_query: types.CallbackQuery, state: FSMContext):
    """Set up auto-react with limited accounts."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(AutoFeatureStates.chat_id)
    await state.update_data(feature_type='react')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🔥 **Auto-React (Limited Accounts)**\n\n"
             "Please send the chat ID where a limited number of accounts should automatically react to new messages.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'mega_removeautoreact')
async def callback_mega_removeautoreact(callback_query: types.CallbackQuery, state: FSMContext):
    """Remove mega auto-react."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    if not AUTO_REACT_CHATS:
        await callback_query.answer("🚫 No auto-react chats configured to remove.", show_alert=True)
        return

    await state.set_state(MegaAutoFeatureStates.chat_ids)
    await state.update_data(action='remove_mega_react')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🗑️ **Mega Remove Auto-React (All)**\n\n"
             "Please send the chat ID(s) from which to remove mega auto-react.\n"
             "You can send multiple IDs separated by commas (e.g., `-1001234567890, -1009876543210`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'removeautoreact')
async def callback_removeautoreact(callback_query: types.CallbackQuery, state: FSMContext):
    """Remove auto-react for limited accounts."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    if not AUTO_REACT_CHATS:
        await callback_query.answer("🚫 No auto-react chats configured to remove.", show_alert=True)
        return

    await state.set_state(AutoFeatureStates.chat_id)
    await state.update_data(feature_type='remove_react')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="➖ **Remove Auto-React (Limited)**\n\n"
             "Please send the chat ID from which to remove auto-react.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'reactlist')
async def callback_reactlist(callback_query: types.CallbackQuery):
    """List auto-react chats."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return

    if not AUTO_REACT_CHATS:
        await callback_query.answer("🚫 No auto-react chats configured.", show_alert=True)
        return

    react_list = "📋 **Auto-React Chats:**\n\n"
    for chat_id in AUTO_REACT_CHATS:
        count = AUTO_REACT_COUNTS.get(chat_id, "All")
        react_list += f"- `{chat_id}` (Accounts: {count})\n"
    
    await bot.send_message(
        chat_id=callback_query.message.chat.id,
        text=react_list,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'mega_autoview')
async def callback_mega_autoview(callback_query: types.CallbackQuery, state: FSMContext):
    """Set up mega auto-view."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(MegaAutoFeatureStates.chat_ids)
    await state.update_data(feature_type='view')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="👁️ **Mega Auto-View (All Accounts)**\n\n"
             "Please send the chat ID(s) where all accounts should automatically view new messages.\n"
             "You can send multiple IDs separated by commas (e.g., `-1001234567890, -1009876543210`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'autoview')
async def callback_autoview(callback_query: types.CallbackQuery, state: FSMContext):
    """Set up auto-view with limited accounts."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(AutoFeatureStates.chat_id)
    await state.update_data(feature_type='view')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="👁️ **Auto-View (Limited Accounts)**\n\n"
             "Please send the chat ID where a limited number of accounts should automatically view new messages.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'mega_removeautoview')
async def callback_mega_removeautoview(callback_query: types.CallbackQuery, state: FSMContext):
    """Remove mega auto-view."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    if not AUTO_VIEW_CHATS:
        await callback_query.answer("🚫 No auto-view chats configured to remove.", show_alert=True)
        return

    await state.set_state(MegaAutoFeatureStates.chat_ids)
    await state.update_data(action='remove_mega_view')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🗑️ **Mega Remove Auto-View (All)**\n\n"
             "Please send the chat ID(s) from which to remove mega auto-view.\n"
             "You can send multiple IDs separated by commas (e.g., `-1001234567890, -1009876543210`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'removeautoview')
async def callback_removeautoview(callback_query: types.CallbackQuery, state: FSMContext):
    """Remove auto-view for limited accounts."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    if not AUTO_VIEW_CHATS:
        await callback_query.answer("🚫 No auto-view chats configured to remove.", show_alert=True)
        return

    await state.set_state(AutoFeatureStates.chat_id)
    await state.update_data(feature_type='remove_view')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="➖ **Remove Auto-View (Limited)**\n\n"
             "Please send the chat ID from which to remove auto-view.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'viewlist')
async def callback_viewlist(callback_query: types.CallbackQuery):
    """List auto-view chats."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return

    if not AUTO_VIEW_CHATS:
        await callback_query.answer("🚫 No auto-view chats configured.", show_alert=True)
        return

    view_list = "📋 **Auto-View Chats:**\n\n"
    for chat_id in AUTO_VIEW_CHATS:
        count = AUTO_VIEW_COUNTS.get(chat_id, "All")
        view_list += f"- `{chat_id}` (Accounts: {count})\n"
    
    await bot.send_message(
        chat_id=callback_query.message.chat.id,
        text=view_list,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'mega_autodual')
async def callback_mega_autodual(callback_query: types.CallbackQuery, state: FSMContext):
    """Set up mega auto-dual."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(MegaAutoFeatureStates.chat_ids)
    await state.update_data(feature_type='dual')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="⚡ **Mega Auto-Dual (All Accounts)**\n\n"
             "Please send the chat ID(s) where all accounts should automatically react and view new messages.\n"
             "You can send multiple IDs separated by commas (e.g., `-1001234567890, -1009876543210`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'autodual')
async def callback_autodual(callback_query: types.CallbackQuery, state: FSMContext):
    """Set up auto-dual with limited accounts."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(AutoFeatureStates.chat_id)
    await state.update_data(feature_type='dual')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="⚡ **Auto-Dual (Limited Accounts)**\n\n"
             "Please send the chat ID where a limited number of accounts should automatically react and view new messages.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'mega_removeautodual')
async def callback_mega_removeautodual(callback_query: types.CallbackQuery, state: FSMContext):
    """Remove mega auto-dual."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    if not AUTO_DUAL_CHATS:
        await callback_query.answer("🚫 No auto-dual chats configured to remove.", show_alert=True)
        return

    await state.set_state(MegaAutoFeatureStates.chat_ids)
    await state.update_data(action='remove_mega_dual')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🗑️ **Mega Remove Auto-Dual (All)**\n\n"
             "Please send the chat ID(s) from which to remove mega auto-dual.\n"
             "You can send multiple IDs separated by commas (e.g., `-1001234567890, -1009876543210`).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'removeautodual')
async def callback_removeautodual(callback_query: types.CallbackQuery, state: FSMContext):
    """Remove auto-dual for limited accounts."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    if not AUTO_DUAL_CHATS:
        await callback_query.answer("🚫 No auto-dual chats configured to remove.", show_alert=True)
        return

    await state.set_state(AutoFeatureStates.chat_id)
    await state.update_data(feature_type='remove_dual')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="➖ **Remove Auto-Dual (Limited)**\n\n"
             "Please send the chat ID from which to remove auto-dual.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'autoduallist')
async def callback_autoduallist(callback_query: types.CallbackQuery):
    """List auto-dual chats."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return

    if not AUTO_DUAL_CHATS:
        await callback_query.answer("🚫 No auto-dual chats configured.", show_alert=True)
        return

    dual_list = "📋 **Auto-Dual Chats:**\n\n"
    for chat_id in AUTO_DUAL_CHATS:
        count = AUTO_DUAL_COUNTS.get(chat_id, "All")
        dual_list += f"- `{chat_id}` (Accounts: {count})\n"
    
    await bot.send_message(
        chat_id=callback_query.message.chat.id,
        text=dual_list,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.message(AutoFeatureStates.chat_id)
async def process_auto_feature_chat_id(message: types.Message, state: FSMContext):
    """Process chat ID for auto-react, auto-view, or auto-dual."""
    try:
        chat_id = int(message.text.strip())
        data = await state.get_data()
        feature_type = data.get('feature_type')

        if feature_type in ['react', 'view', 'dual']:
            await state.update_data(chat_id=chat_id)
            await state.set_state(AutoFeatureStates.count)
            feature_name = {
                'react': 'Auto-React',
                'view': 'Auto-View',
                'dual': 'Auto-Dual'
            }[feature_type]
            await message.reply(
                f"Please send the number of accounts to use for {feature_name} in chat `{chat_id}` (1 to {len(user_clients)}):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
                ]]),
                parse_mode="Markdown"
            )
        elif feature_type in ['remove_react', 'remove_view', 'remove_dual']:
            target_set = {
                'remove_react': AUTO_REACT_CHATS,
                'remove_view': AUTO_VIEW_CHATS,
                'remove_dual': AUTO_DUAL_CHATS
            }[feature_type]
            remove_count_func = {
                'remove_react': remove_auto_react_count,
                'remove_view': remove_auto_view_count,
                'remove_dual': remove_auto_dual_count
            }[feature_type]
            feature_name = {
                'remove_react': 'Auto-React',
                'remove_view': 'Auto-View',
                'remove_dual': 'Auto-Dual'
            }[feature_type]

            if chat_id in target_set:
                target_set.remove(chat_id)
                remove_count_func(chat_id)
                save_auto_chat_lists()
                await message.reply(
                    f"✅ {feature_name} removed for chat `{chat_id}`.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")
                    ]]),
                    parse_mode="Markdown"
                )
            else:
                await message.reply(
                    f"❌ Chat `{chat_id}` is not configured for {feature_name}.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
                    ]]),
                    parse_mode="Markdown"
                )
            await state.clear()

    except ValueError:
        await message.reply(
            "❌ Invalid chat ID. Please enter a valid integer.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
            ]])
        )

@dp.message(AutoFeatureStates.count)
async def process_auto_feature_count(message: types.Message, state: FSMContext):
    """Process the number of accounts for auto features."""
    try:
        count = int(message.text.strip())
        if not (1 <= count <= len(user_clients)):
            await message.reply(
                f"❌ Invalid count. Please enter a number between 1 and {len(user_clients)} (number of available accounts).",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
                ]])
            )
            return

        data = await state.get_data()
        chat_id = data['chat_id']
        feature_type = data['feature_type']
        
        target_set = {
            'react': AUTO_REACT_CHATS,
            'view': AUTO_VIEW_CHATS,
            'dual': AUTO_DUAL_CHATS
        }[feature_type]
        set_count_func = {
            'react': set_auto_react_count,
            'view': set_auto_view_count,
            'dual': set_auto_dual_count
        }[feature_type]
        feature_name = {
            'react': 'Auto-React',
            'view': 'Auto-View',
            'dual': 'Auto-Dual'
        }[feature_type]

        target_set.add(chat_id)
        set_count_func(chat_id, count)
        save_auto_chat_lists()
        
        await message.reply(
            f"✅ {feature_name} enabled for chat `{chat_id}` with `{count}` accounts.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")
            ]]),
            parse_mode="Markdown"
        )
        await state.clear()

    except ValueError:
        await message.reply(
            "❌ Invalid count. Please enter a valid integer.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
            ]])
        )

@dp.message(MegaAutoFeatureStates.chat_ids)
async def process_mega_auto_feature_chat_ids(message: types.Message, state: FSMContext):
    """Process multiple chat IDs for mega auto-react, auto-view, or auto-dual."""
    chat_ids_input = message.text.strip()
    chat_ids = [id.strip() for id in chat_ids_input.split(',')]
    valid_chat_ids = []
    invalid_chat_ids = []

    for chat_id in chat_ids:
        try:
            valid_chat_ids.append(int(chat_id))
        except ValueError:
            invalid_chat_ids.append(chat_id)

    if invalid_chat_ids:
        await message.reply(
            f"❌ Invalid chat IDs: {', '.join(invalid_chat_ids)}. Please provide valid integers separated by commas.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_auto_feature_setup")
            ]])
        )
        return

    data = await state.get_data()
    action = data.get('action', data.get('feature_type'))  # Handle both feature_type and action

    if action in ['react', 'view', 'dual']:
        target_set = {
            'react': AUTO_REACT_CHATS,
            'view': AUTO_VIEW_CHATS,
            'dual': AUTO_DUAL_CHATS
        }[action]
        set_count_func = {
            'react': set_auto_react_count,
            'view': set_auto_view_count,
            'dual': set_auto_dual_count
        }[action]
        feature_name = {
            'react': 'Auto-React',
            'view': 'Auto-View',
            'dual': 'Auto-Dual'
        }[action]

        for chat_id in valid_chat_ids:
            target_set.add(chat_id)
            set_count_func(chat_id, len(user_clients))  # Use all accounts for mega actions

        save_auto_chat_lists()
        await message.reply(
            f"✅ {feature_name} enabled for chats: {', '.join(map(str, valid_chat_ids))} with all accounts.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")
            ]]),
            parse_mode="Markdown"
        )

    elif action in ['remove_mega_react', 'remove_mega_view', 'remove_mega_dual']:
        target_set = {
            'remove_mega_react': AUTO_REACT_CHATS,
            'remove_mega_view': AUTO_VIEW_CHATS,
            'remove_mega_dual': AUTO_DUAL_CHATS
        }[action]
        remove_count_func = {
            'remove_mega_react': remove_auto_react_count,
            'remove_mega_view': remove_auto_view_count,
            'remove_mega_dual': remove_auto_dual_count
        }[action]
        feature_name = {
            'remove_mega_react': 'Auto-React',
            'remove_mega_view': 'Auto-View',
            'remove_mega_dual': 'Auto-Dual'
        }[action]

        removed_chats = []
        not_configured_chats = []

        for chat_id in valid_chat_ids:
            if chat_id in target_set:
                target_set.remove(chat_id)
                remove_count_func(chat_id)
                removed_chats.append(str(chat_id))
            else:
                not_configured_chats.append(str(chat_id))

        save_auto_chat_lists()

        response_text = f"✅ {feature_name} removed for chats: {', '.join(removed_chats)}\n" if removed_chats else ""
        if not_configured_chats:
            response_text += f"⚠️ {feature_name} not configured for chats: {', '.join(not_configured_chats)}"

        await message.reply(
            response_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")
            ]]),
            parse_mode="Markdown"
        )

    await state.clear()

@dp.callback_query(lambda c: c.data == 'cancel_auto_feature_setup')
async def callback_cancel_auto_feature_setup(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle cancellation of auto feature setup."""
    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="✅ Auto feature setup cancelled.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back to Auto Features", callback_data="auto_features")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'sudo_access')
async def callback_sudo_access(callback_query: types.CallbackQuery):
    """Handle sudo access menu callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    keyboard = await create_sudo_access_menu()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🔑 **Sudo Access Management**\n\nManage privileged users:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'add_sudo_user')
async def callback_add_sudo_user(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle add sudo user callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS:
        await callback_query.answer("❌ Only owners can add sudo users.", show_alert=True)
        return
    
    await state.set_state(SudoStates.user_id)
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="➕ **Add Sudo User**\n\nPlease send the user ID of the person to add as a sudo user:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'remove_sudo_user')
async def callback_remove_sudo_user(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle remove sudo user callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS:
        await callback_query.answer("❌ Only owners can remove sudo users.", show_alert=True)
        return
    
    if not user_roles:
        await callback_query.answer("🚫 No sudo users configured to remove.", show_alert=True)
        return

    await state.set_state(SudoStates.user_id)
    await state.update_data(action='remove')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="➖ **Remove Sudo User**\n\nPlease send the user ID of the sudo user to remove:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'list_sudo_users')
async def callback_list_sudo_users(callback_query: types.CallbackQuery):
    """Handle list sudo users callback."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    
    await show_sudo_list(callback_query.message)
    await callback_query.answer()

async def show_sudo_list(message_or_callback: types.Message):
    """Show the list of sudo users."""
    if not user_roles:
        await message_or_callback.reply("🚫 No sudo users configured.")
        return

    sudo_list = "📝 **Sudo Users:**\n\n"
    for user_id, info in user_roles.items():
        role = info['role']
        promoted_by = info['promoted_by']
        promoted_at = datetime.fromtimestamp(info['promoted_at']).strftime('%Y-%m-%d %H:%M:%S')
        sudo_list += f"- User ID: `{user_id}`, Role: `{role}`, Promoted by: `{promoted_by}`, At: `{promoted_at}`\n"
    
    await message_or_callback.reply(sudo_list, parse_mode="Markdown")

@dp.message(SudoStates.user_id)
async def process_sudo_user_id(message: types.Message, state: FSMContext):
    """Process user ID for sudo operations."""
    try:
        user_id = int(message.text.strip())
        data = await state.get_data()
        action = data.get('action')

        if action == 'remove':
            if user_id not in user_roles:
                await message.reply(
                    f"❌ User `{user_id}` is not a sudo user.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")
                    ]]),
                    parse_mode="Markdown"
                )
                return
            await state.update_data(user_id=user_id)
            await state.set_state(SudoStates.confirm_remove)
            await message.reply(
                f"⚠️ Are you sure you want to remove sudo user `{user_id}` (Role: `{user_roles[user_id]['role']}`)?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Confirm", callback_data="confirm_remove_sudo")],
                    [InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")]
                ]),
                parse_mode="Markdown"
            )
        else:
            if user_id in OWNER_IDS:
                await message.reply(
                    "❌ Cannot add an owner as a sudo user.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")
                    ]])
                )
                await state.clear()
                return
            if user_id in user_roles:
                await message.reply(
                    f"❌ User `{user_id}` is already a sudo user with role `{user_roles[user_id]['role']}`.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")
                    ]]),
                    parse_mode="Markdown"
                )
                await state.clear()
                return
            await state.update_data(user_id=user_id)
            await state.set_state(SudoStates.role)
            await message.reply(
                "Please select the role for the new sudo user:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Co-Owner", callback_data="role_co-owner")],
                    [InlineKeyboardButton(text="Admin", callback_data="role_admin")],
                    [InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")]
                ])
            )

    except ValueError:
        await message.reply(
            "❌ Invalid user ID. Please enter a valid integer.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_sudo_operation")
            ]])
        )

@dp.callback_query(lambda c: c.data.startswith('role_'))
async def process_sudo_role(callback_query: types.CallbackQuery, state: FSMContext):
    """Process role selection for sudo user."""
    role = callback_query.data.replace('role_', '')
    data = await state.get_data()
    user_id = data.get('user_id')
    promoter_id = callback_query.from_user.id

    user_roles[user_id] = {
        'role': role,
        'promoted_by': promoter_id,
        'promoted_at': int(asyncio.get_event_loop().time())
    }
    save_sudo_users()

    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=f"✅ User `{user_id}` added as `{role}`.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back to Sudo Access", callback_data="sudo_access")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'confirm_remove_sudo')
async def confirm_remove_sudo(callback_query: types.CallbackQuery, state: FSMContext):
    """Confirm removal of sudo user."""
    data = await state.get_data()
    user_id = data.get('user_id')
    if user_id in user_roles:
        del user_roles[user_id]
        save_sudo_users()
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=f"✅ Sudo user `{user_id}` removed.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Sudo Access", callback_data="sudo_access")
            ]]),
            parse_mode="Markdown"
        )
    else:
        await bot.edit_message_text(
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            text=f"❌ User `{user_id}` is not a sudo user.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Sudo Access", callback_data="sudo_access")
            ]]),
            parse_mode="Markdown"
        )
    await state.clear()
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'cancel_sudo_operation')
async def callback_cancel_sudo_operation(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle cancellation of sudo operation."""
    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="✅ Sudo operation cancelled.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back to Sudo Access", callback_data="sudo_access")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'help')
async def callback_help(callback_query: types.CallbackQuery):
    """Handle help callback."""
    user_id = callback_query.from_user.id
    role = user_roles.get(user_id, {}).get('role') if user_id not in OWNER_IDS else 'owner'
    
    help_text = (
        "🌟 **Telegram Meta Bot Help** 🌟\n\n"
        "This bot manages multiple Telegram accounts to automate reactions, views, and member actions.\n\n"
        "**Available Commands:**\n"
        "- `/start` or `/help`: Show the main menu\n"
        "- `/addaccount`: Add a new Telegram account\n"
        "- `/listaccounts`: List all loaded accounts (sudo users only)\n"
        "- `/removeaccount`: Remove an account (owners only)\n"
        "- `/info`: Show bot statistics and account info (owners/co-owners only)\n"
        "- `/megareact <chat_id> <message_id> [emoji]`: Make all accounts react to a message (owners/co-owners only)\n"
        "- `/listsudo`: List sudo users (owners/co-owners only)\n"
        "- `/cancel`: Cancel an ongoing operation\n"
    )
    
    # Add Login Helper information for owners and co-owners
    if role in ['owner', 'co-owner']:
        help_text += (
            "- `/login`: Start the login helper to get OTP codes\n\n"
            "**Features:**\n"
            "- **Account Management**: Add, remove, and list accounts\n"
            "- **Member Management**: Join/leave groups or channels with multiple accounts\n"
            "- **Auto Features**: Configure automatic reactions and views for specific chats\n"
            "- **Sudo Access**: Manage privileged users (co-owners and admins)\n"
            "- **Login Helper**: Assist with logging into accounts elsewhere\n"
            "  - Automatically detect and display OTP codes\n"
            "  - Manage 2FA passwords\n"
            "  - Store account metadata\n"
        )
    else:
        help_text += (
            "\n**Features:**\n"
            "- **Account Management**: Add, remove, and list accounts\n"
            "- **Member Management**: Join/leave groups or channels with multiple accounts\n"
            "- **Auto Features**: Configure automatic reactions and views for specific chats\n"
            "- **Sudo Access**: Manage privileged users (co-owners and admins)\n"
        )
    
    help_text += "\nUse the inline buttons to navigate the menus and configure settings."
    
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text=help_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back", callback_data="main_menu")
        ]]),
        parse_mode="Markdown"
    )
    await callback_query.answer()

# Member Management Handlers
@dp.callback_query(lambda c: c.data == 'mega_join')
async def callback_mega_join(callback_query: types.CallbackQuery, state: FSMContext):
    """Start process for mega join."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(MemberManagementStates.invite_link)
    await state.update_data(action='mega_join')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🚀 **Mega Join (All Accounts)**\n\n"
             "Please send the invite link (e.g., `https://t.me/+AbCdeFgHijK`) for the group/channel.\n"
             "All loaded accounts will attempt to join.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'join')
async def callback_join(callback_query: types.CallbackQuery, state: FSMContext):
    """Start process for limited join."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(MemberManagementStates.count)
    await state.update_data(action='join')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="➕ **Join (Limited Accounts)**\n\n"
             "Please send the number of accounts to join (1 to all available accounts):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'mega_leave')
async def callback_mega_leave(callback_query: types.CallbackQuery, state: FSMContext):
    """Start process for mega leave."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(MemberManagementStates.invite_link) # Reusing invite_link state to get chat_id
    await state.update_data(action='mega_leave')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="🚫 **Mega Leave (All Accounts)**\n\n"
             "Please send the invite link or chat ID of the group/channel.\n"
             "All loaded accounts will attempt to leave.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
        ]])
    )
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == 'leave')
async def callback_leave(callback_query: types.CallbackQuery, state: FSMContext):
    """Start process for limited leave."""
    user_id = callback_query.from_user.id
    if user_id not in OWNER_IDS and user_roles.get(user_id, {}).get('role') != 'co-owner':
        await callback_query.answer("❌ You are not authorized to access this feature.", show_alert=True)
        return
    await state.set_state(MemberManagementStates.count)
    await state.update_data(action='leave')
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="➖ **Leave (Limited Accounts)**\n\n"
             "Please send the number of accounts to leave (1 to all available accounts):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
        ]])
    )
    await callback_query.answer()

@dp.message(MemberManagementStates.count)
async def process_member_count(message: types.Message, state: FSMContext):
    """Process the number of accounts for join/leave operations."""
    try:
        count = int(message.text.strip())
        if not (1 <= count <= len(user_clients)):
            await message.reply(
                f"❌ Invalid count. Please enter a number between 1 and {len(user_clients)} (number of available accounts).",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
                ]])
            )
            return

        await state.update_data(num_accounts=count)
        data = await state.get_data()
        action = data['action']
        
        if action == 'join':
            await state.set_state(MemberManagementStates.invite_link)
            await message.reply(
                f"Please send the invite link (e.g., `https://t.me/+AbCdeFgHijK`) for the group/channel.\n"
                f"`{count}` accounts will attempt to join.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
                ]])
            )
        elif action == 'leave':
            await state.set_state(MemberManagementStates.invite_link) # Reusing state to get chat_id
            await message.reply(
                f"Please send the invite link or chat ID of the group/channel.\n"
                f"`{count}` accounts will attempt to leave.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
                ]])
            )

    except ValueError:
        await message.reply(
            "❌ Invalid count. Please enter a valid integer.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="Cancel", callback_data="cancel_member_management")
            ]])
        )

@dp.message(MemberManagementStates.invite_link)
async def process_invite_link_or_chat_id(message: types.Message, state: FSMContext):
    """Process invite link or chat ID for join/leave operations."""
    link_or_id = message.text.strip()
    data = await state.get_data()
    action = data['action']
    num_accounts = data.get('num_accounts', len(user_clients)) # Default to all for mega actions

    if not user_clients:
        await message.reply("🚫 No accounts loaded to perform this action.")
        await state.clear()
        return

    # Use only active (non-frozen) accounts
    active_clients = get_active_clients()
    if not active_clients:
        await message.reply("🚫 No active (non-frozen) accounts available to perform this action.")
        await state.clear()
        return
    
    # Adjust num_accounts to available active accounts and inform user
    available_active = len(active_clients)
    if num_accounts > available_active:
        await message.reply(
            f"⚠️ **Account Availability Notice**\n\n"
            f"Requested: {num_accounts} accounts\n"
            f"🟢 Active accounts available: {available_active}\n"
            f"🔴 Frozen/unavailable accounts: {len(user_clients) - available_active}\n\n"
            f"Proceeding with {available_active} active accounts only.",
            parse_mode="Markdown"
        )
        num_accounts = available_active
        
    selected_clients = list(active_clients.values())[:num_accounts]
    
    success_count = 0
    fail_count = 0
    
    if action == 'mega_join' or (action == 'join' and num_accounts > 0):
        action_name = "join"
        processing_message = await message.reply(
            f"🚀 Attempting to {action_name} `{link_or_id}` with {len(selected_clients)} accounts...\n\nProcessing...",
            parse_mode="Markdown"
        )
        for client_obj, _ in selected_clients:
            phone = next((p for p, (c, _) in active_clients.items() if c == client_obj), "unknown")
            await respect_flood_limit(phone, JOIN_DELAY)
            try:
                if not await ensure_connected(client_obj, phone):
                    logger.error(f"Client {phone} is not connected. Skipping join.")
                    fail_count += 1
                    continue

                # Try to import chat invite if it's a link
                if "t.me/+" in link_or_id or "t.me/joinchat/" in link_or_id:
                    hash_match = re.search(r'(?:\+|joinchat/)([A-Za-z0-9_-]+)', link_or_id)
                    if hash_match:
                        invite_hash = hash_match.group(1)
                        try:
                            updates = await client_obj(ImportChatInviteRequest(invite_hash))
                            logger.info(f"Client {phone} joined chat via invite link: {link_or_id}")
                            success_count += 1
                        except UserAlreadyParticipantError:
                            logger.info(f"Client {phone} is already a participant in {link_or_id}")
                            success_count += 1 # Consider as success if already there
                        except InviteHashExpiredError:
                            logger.warning(f"Invite link expired for {phone}: {link_or_id}")
                            fail_count += 1
                        except InviteHashInvalidError:
                            logger.warning(f"Invite link invalid for {phone}: {link_or_id}")
                            fail_count += 1
                        except Exception as e:
                            logger.error(f"Error importing chat invite for {phone}: {e}")
                            fail_count += 1
                    else:
                        logger.error(f"Could not extract invite hash from link: {link_or_id}")
                        fail_count += 1
                else: # Assume it's a public channel username or chat ID
                    try:
                        await client_obj(JoinChannelRequest(channel=link_or_id))
                        logger.info(f"Client {phone} joined chat: {link_or_id}")
                        success_count += 1
                    except UserAlreadyParticipantError:
                        logger.info(f"Client {phone} is already a participant in {link_or_id}")
                        success_count += 1 # Consider as success if already there
                    except Exception as e:
                        logger.error(f"Error joining channel {link_or_id} for {phone}: {e}")
                        fail_count += 1

            except FloodWaitError as e:
                views_core.FLOOD_UNTIL[phone] = asyncio.get_event_loop().time() + e.seconds
                logger.warning(f"FloodWaitError for {phone}: Must wait {e.seconds} seconds before joining again.")
                fail_count += 1
            except Exception as e:
                logger.error(f"Unexpected error during join for {phone}: {e}")
                fail_count += 1

        await bot.edit_message_text(
            chat_id=processing_message.chat.id,
            message_id=processing_message.message_id,
            text=f"🚀 Join operation completed for `{link_or_id}`!\n\n"
                 f"✅ Successful joins: {success_count}\n"
                 f"❌ Failed joins: {fail_count}",
            parse_mode="Markdown"
        )

    elif action == 'mega_leave' or (action == 'leave' and num_accounts > 0):
        action_name = "leave"
        processing_message = await message.reply(
            f"🚫 Attempting to {action_name} `{link_or_id}` with {len(selected_clients)} accounts...\n\nProcessing...",
            parse_mode="Markdown"
        )
        for client_obj, _ in selected_clients:
            phone = next((p for p, (c, _) in active_clients.items() if c == client_obj), "unknown")
            await respect_flood_limit(phone, LEAVE_DELAY)
            try:
                if not await ensure_connected(client_obj, phone):
                    logger.error(f"Client {phone} is not connected. Skipping leave.")
                    fail_count += 1
                    continue
                
                # Resolve chat ID from link or direct ID
                try:
                    entity = await client_obj.get_entity(link_or_id)
                    chat_id = entity.id
                    if not hasattr(entity, 'megagroup') and not hasattr(entity, 'channel'): # Private chats might not have LeaveChannelRequest
                        raise ValueError("Provided entity is not a channel or megagroup.")
                except Exception as e:
                    logger.warning(f"Could not resolve chat ID for {link_or_id} for client {phone}: {e}. Attempting direct leave.")
                    # If it's just a number, try using it as a chat ID
                    try:
                        chat_id = int(link_or_id)
                    except ValueError:
                        fail_count += 1
                        logger.error(f"Invalid chat ID or link for client {phone}: {link_or_id}")
                        continue
                        
                await client_obj(LeaveChannelRequest(channel=chat_id))
                logger.info(f"Client {phone} left chat: {link_or_id}")
                success_count += 1
            except UserNotParticipantError:
                logger.info(f"Client {phone} is not a participant in {link_or_id}, no need to leave.")
                success_count += 1 # Consider as success if already not there
            except FloodWaitError as e:
                views_core.FLOOD_UNTIL[phone] = asyncio.get_event_loop().time() + e.seconds
                logger.warning(f"FloodWaitError for {phone}: Must wait {e.seconds} seconds before leaving again.")
                fail_count += 1
            except Exception as e:
                logger.error(f"Error leaving chat {link_or_id} for {phone}: {e}")
                fail_count += 1
        
        await bot.edit_message_text(
            chat_id=processing_message.chat.id,
            message_id=processing_message.message_id,
            text=f"🚫 Leave operation completed for `{link_or_id}`!\n\n"
                 f"✅ Successful leaves: {success_count}\n"
                 f"❌ Failed leaves: {fail_count}",
            parse_mode="Markdown"
        )
    
    await state.clear()
    await message.reply(
        "Done with member management.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back to Member Management", callback_data="member_management")
        ]])
    )

@dp.callback_query(lambda c: c.data == 'cancel_member_management')
async def callback_cancel_member_management(callback_query: types.CallbackQuery, state: FSMContext):
    """Handle cancellation of member management operations."""
    await state.clear()
    await bot.edit_message_text(
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        text="✅ Member management operation cancelled.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 Back to Member Management", callback_data="member_management")
        ]])
    )
    await callback_query.answer()

# Error handler
@dp.errors()
async def error_handler(update, exception):
    """Handle errors globally."""
    logger.error(f"Update caused error: {exception}")
    try:
        if isinstance(exception, TelegramRetryAfter):
            await asyncio.sleep(exception.retry_after)
            return True  # Retry the update
        elif isinstance(exception, TelegramForbiddenError):
            try:
                chat_id = update.message.chat.id if hasattr(update, 'message') and update.message else update.callback_query.message.chat.id
                await bot.send_message(chat_id, "❌ Bot was blocked or kicked from the chat. Please check permissions.")
            except:
                pass  # Ignore if we can't send the message
        elif isinstance(exception, TelegramNotFound):
            try:
                chat_id = update.message.chat.id if hasattr(update, 'message') and update.message else update.callback_query.message.chat.id
                await bot.send_message(chat_id, "❌ Resource not found. Please check the chat ID or message ID.")
            except:
                pass  # Ignore if we can't send the message
        else:
            try:
                chat_id = update.message.chat.id if hasattr(update, 'message') and update.message else update.callback_query.message.chat.id
                await bot.send_message(chat_id, f"❌ An error occurred: {str(exception)}")
            except:
                pass  # Ignore if we can't send the message
    except Exception as e:
        logger.error(f"Error in error handler: {e}")
    return False  # Do not retry by default

# Session monitoring and recovery
async def monitor_sessions():
    """Periodically monitor and recover failed sessions."""
    while True:
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            if not user_clients:
                continue
                
            logger.info("Starting session health check...")
            unhealthy_count = 0
            recovered_count = 0
            
            for phone, (client, session_string) in list(user_clients.items()):
                is_healthy, reason = await validate_session_health(client, phone)
                
                if not is_healthy:
                    unhealthy_count += 1
                    logger.warning(f"Session {phone} is unhealthy: {reason}")
                    
                    # Check if account is frozen/restricted
                    if any(keyword in reason.lower() for keyword in ['restricted', 'frozen', 'banned', 'suspended']):
                        mark_account_frozen(phone, reason)
                    # Try to recover the session if it's just a connection issue
                    elif "Not connected" in reason:
                        try:
                            if await ensure_connected(client, phone, retries=2):
                                recovered_count += 1
                                logger.info(f"Recovered session {phone}")
                                # Mark as active if it was previously frozen but now recovered
                                if is_account_frozen(phone):
                                    mark_account_active(phone)
                        except Exception as e:
                            logger.error(f"Failed to recover session {phone}: {e}")
                else:
                    # Mark as active if it was previously frozen but now healthy
                    if is_account_frozen(phone):
                        mark_account_active(phone)
                        logger.info(f"Account {phone} recovered from frozen state")
                            
            if unhealthy_count > 0:
                logger.info(f"Health check completed: {unhealthy_count} unhealthy, {recovered_count} recovered")
                
                # Notify owners if too many sessions are failing
                if unhealthy_count > len(user_clients) * 0.1:  # More than 10% unhealthy
                    stats = get_account_statistics()
                    for owner_id in OWNER_IDS:
                        try:
                            await bot.send_message(
                                owner_id,
                                f"⚠️ Session Health Alert:\n"
                                f"Unhealthy sessions: {unhealthy_count}\n"
                                f"Recovered sessions: {recovered_count}\n"
                                f"Total sessions: {stats['total']}\n"
                                f"🟢 Active: {stats['active']}\n"
                                f"🔴 Frozen: {stats['frozen']}\n"
                                f"Connected Active: {stats['connected_active']}"
                            )
                        except Exception as e:
                            logger.error(f"Failed to notify owner {owner_id}: {e}")
                            
        except Exception as e:
            logger.error(f"Error in session monitoring: {e}")

# Bot startup and shutdown
async def on_startup():
    """Perform startup tasks."""
    # Setup signal handlers for graceful shutdown
    setup_signal_handlers()
    
    # Display startup banner
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    🌟 TELEGRAM META BOT 🌟                   ║
║                     Multi-Account Manager                    ║
╠══════════════════════════════════════════════════════════════╣
║  • Advanced Session Management                              ║
║  • Auto-View Features                                       ║
║  • Flood Control & Rate Limiting                           ║
║  • State Persistence                                        ║
║  • Real-time Monitoring                                     ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    logger.info("🚀 Bot starting up...")
    
    # Load views_core state first
    logger.info("📊 Loading views_core state...")
    load_views_core_state()
    
    # Load sessions with improved error handling
    try:
        logger.info("📱 Loading user sessions...")
        await load_sessions()
        logger.info(f"✅ Successfully loaded {len(user_clients)} user sessions")
    except Exception as e:
        logger.error(f"❌ Error loading sessions: {e}")
    
    # Load configuration files
    logger.info("⚙️ Loading configuration files...")
    load_auto_chat_lists()
    load_sudo_users()
    load_known_users()
    
    # Load F2F data and passwords
    global f2f_data, passwords
    f2f_data = load_f2f_data()
    passwords = load_passwords()
    
    # Session health checks are now done in comprehensive cleanup during load_sessions()
    logger.info("✅ Session health checks completed during comprehensive cleanup")
    
    # Start session monitoring task
    logger.info("🔍 Starting session monitoring...")
    asyncio.create_task(monitor_sessions())
    
    # Notify owners with detailed statistics
    stats = get_account_statistics()
    startup_message = (
        f"🌟 **Bot Started Successfully!** 🌟\n\n"
        f"📊 **Session Statistics:**\n"
        f"• Total Sessions: {stats['total']}\n"
        f"• Active Accounts: {stats['active']}\n"
        f"• Connected Active: {stats['connected_active']}\n"
        f"• Frozen Accounts: {stats['frozen']}\n\n"
        f"⚡ **Features Status:**\n"
        f"• Auto-Views: {'Enabled' if auto_features_enabled else 'Disabled'}\n"
        f"• Auto-View Chats: {len(AUTO_VIEW_CHATS)}\n"
        f"• State Persistence: Active\n"
        f"• Session Monitoring: Enabled\n\n"
        f"✅ **Ready to serve!**"
    )
    
    for owner_id in OWNER_IDS:
        try:
            await bot.send_message(owner_id, startup_message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Failed to notify owner {owner_id}: {e}")
    
    logger.info("🎉 Bot startup complete!")

async def on_shutdown():
    """Perform shutdown tasks."""
    logger.info("🔄 Bot shutting down...")
    
    # Save views_core state before shutdown
    logger.info("💾 Saving state before shutdown...")
    save_views_core_state()
    
    logger.info("🔌 Disconnecting user clients...")
    for phone, (client, _) in user_clients.items():
        try:
            if client.is_connected():
                # Use proper disconnection with cleanup according to Telethon docs
                await client.disconnect()
                logger.debug(f"✅ Disconnected client {phone}")
            else:
                logger.debug(f"ℹ️ Client {phone} was already disconnected")
        except Exception as e:
            logger.error(f"❌ Failed to disconnect client {phone}: {e}")
            # Force cleanup if normal disconnect fails
            try:
                if hasattr(client, '_sender') and client._sender:
                    await client._sender.disconnect()
            except Exception as e2:
                logger.error(f"❌ Failed to force disconnect {phone}: {e2}")
    
    logger.info("🔒 Closing bot session...")
    await bot.session.close()
    await storage.close()
    logger.info("✅ Bot shutdown complete!")

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown and state persistence."""
    def signal_handler(signum, frame):
        logger.info(f"🔔 Received signal {signum}, saving state before shutdown...")
        try:
            save_views_core_state()
            logger.info("💾 State saved successfully on signal")
        except Exception as e:
            logger.error(f"❌ Error saving state on signal: {e}")
        # Allow normal shutdown to continue
    
    # Register signal handlers for common shutdown signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGBREAK'):  # Windows
        signal.signal(signal.SIGBREAK, signal_handler)
    
    # Register atexit handler as backup
    atexit.register(lambda: save_views_core_state())
    logger.info("🔔 Signal handlers and atexit state persistence registered")

async def periodic_state_saver():
    """Periodically save views_core state every 30-60 seconds for better persistence."""
    while True:
        # Random interval between 30-60 seconds to avoid synchronized writes
        interval = random.uniform(30, 60)
        await asyncio.sleep(interval)
        try:
            save_views_core_state()
            logger.debug("💾 Periodic state save completed")
        except Exception as e:
            logger.error(f"❌ Error in periodic state save: {e}")

async def main():
    """Main function to start the bot."""
    try:
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)
        dp.message.middleware(AccessControlMiddleware())
        
        # Start periodic state saving task
        asyncio.create_task(periodic_state_saver())
        dp.callback_query.middleware(AccessControlMiddleware())
        await dp.start_polling(bot, handle_signals=True)
    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}")
        await on_shutdown()

if __name__ == '__main__':
    asyncio.run(main())
