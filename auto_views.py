import asyncio, random, datetime
from telethon import types
from views_core import add_views, has_budget, consume_budget, select_accounts_for_views, is_quarantined, mark_restriction

def within_active_hours(now, tz=None):
    """Centralized check for active hours to avoid processing during quiet times."""
    ACTIVE_HOURS = (8, 23)  # 8 AM to 11 PM local time
    if tz is None:
        # Use local time if no timezone provided
        current_hour = now.hour
    else:
        current_hour = now.astimezone(tz).hour
    return ACTIVE_HOURS[0] <= current_hour < ACTIVE_HOURS[1]

async def schedule_views(chat_id, message_ids, state=None, notify=None):
    """
    Centralized view scheduling function that integrates with views_core.
    This replaces the separate polling loop to unify the auto-view pipeline.
    """
    if not message_ids:
        return
        
    # This function can be called from either the event-driven handler 
    # or from auto_views_for_chat to ensure a single pipeline
    from views_core import get_budget_status
    
    now = datetime.datetime.now()
    if not within_active_hours(now):
        return
        
    # Get phones that are eligible for this chat (this would come from configuration)
    # For now, assume all phones are eligible - this would be configured per chat
    phones_for_chat = []  # This should be populated from chat configuration
    
    if not phones_for_chat:
        return
        
    target_accounts = min(len(phones_for_chat), 20)
    today = now.strftime("%Y-%m-%d")
    hour_key = now.strftime("%Y%m%d%H")
    
    # Use views_core selection logic
    if state is None:
        state = {}
    chosen_phones = select_accounts_for_views(phones_for_chat, chat_id, target_accounts, state=state)
    
    # Filter for quarantine and budget
    active_phones = []
    for phone in chosen_phones:
        if is_quarantined(phone):
            continue
        if not has_budget(phone, chat_id, today, hour_key):
            continue
        active_phones.append(phone)
    
    # Process views for active phones
    successes = 0
    for phone in active_phones:
        # This would need access to the client map
        # For now, this is a framework for the unified approach
        # The actual client handling would be done by the caller
        pass
    
    return {"scheduled": len(active_phones), "target": target_accounts}

async def auto_views_for_chat(chat, message_ids_provider, clients_map, phones_for_chat, *, state, notify, tz):
    """
    DEPRECATED: This polling-based auto-view system is deprecated.
    Use the event-driven auto_view_handler in hh.py instead for better performance
    and to avoid competing with the unified views_core pipeline.
    
    This function is kept for backward compatibility but should not be used in new code.
    """
    import logging
    logger = logging.getLogger(__name__)
    logger.warning("⚠️ DEPRECATED: auto_views_for_chat polling loop is deprecated. "
                  "Use event-driven auto_view_handler instead.")
    
    # For backward compatibility, run a few iterations then exit
    iterations = 0
    max_iterations = 5
    
    while iterations < max_iterations:
        iterations += 1
        logger.info(f"🔄 Running deprecated auto_views_for_chat (iteration {iterations}/{max_iterations})")
        
        now = datetime.datetime.now(tz)
        if not within_active_hours(now, tz):
            await asyncio.sleep(60)
            continue

        msg_ids = await message_ids_provider()
        if not msg_ids:
            await asyncio.sleep(10)
            continue

        # how many accounts should view this batch?
        target_accounts = min(len(phones_for_chat), 20)  # tune per your risk tolerance
        chosen_phones = select_accounts_for_views(phones_for_chat, chat_id=getattr(chat, "id", chat), need=target_accounts, state=state)

        # Refill selection if under-filled after filters (with proper tracking)
        max_candidates = min(len(phones_for_chat), target_accounts * 2)  # Over-select to compensate for filters
        remaining_phones = [p for p in phones_for_chat if p not in chosen_phones]
        
        today = now.strftime("%Y-%m-%d")
        hour_key = now.strftime("%Y%m%d%H")
        
        # Process initial chosen phones and track actually active phones
        active_phones = []
        for phone in chosen_phones:
            if phone not in clients_map:
                continue
            if is_quarantined(phone):
                continue
            if not has_budget(phone, getattr(chat, "id", chat), today, hour_key):
                continue
            active_phones.append(phone)
            
        # Refill from remaining phones if we're under target, but track selected count properly
        selected_count = len(active_phones)
        while (selected_count < target_accounts and 
               remaining_phones and 
               len(chosen_phones) < max_candidates):
            extra_phone = remaining_phones.pop(0)
            if (extra_phone in clients_map and 
                not is_quarantined(extra_phone) and 
                has_budget(extra_phone, getattr(chat, "id", chat), today, hour_key)):
                active_phones.append(extra_phone)
                chosen_phones.append(extra_phone)
                selected_count += 1
        
        # Now process views with the finalized active_phones list
        successes = 0
        for phone in active_phones:
            client = clients_map[phone]
            res = await add_views(client, phone, chat, msg_ids, label=f"views:{getattr(chat,'id',chat)}")
            if res.get("ok"):
                consume_budget(phone, getattr(chat, "id", chat), n=1, today=today, hour_key=hour_key)
                successes += 1
            else:
                err = res.get("error", "")
                if any(x in err for x in ["flood", "banned", "authkey_error"]):
                    mark_restriction(phone)
                    await notify(f"Account {phone} quarantined due to: {err}")
            # gentle pacing
            await asyncio.sleep(random.uniform(0.4, 1.6))

        if successes:
            await notify(f"Auto-views: {successes} accounts viewed {len(msg_ids)} messages in chat {getattr(chat,'id',chat)}")

        # pause before next poll
        await asyncio.sleep(random.uniform(8.0, 20.0))
    
    logger.warning("⚠️ Deprecated auto_views_for_chat completed limited run. "
                  "Please migrate to event-driven auto_view_handler.")
