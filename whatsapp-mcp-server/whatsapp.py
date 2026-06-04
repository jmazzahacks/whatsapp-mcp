import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Tuple
import os.path
import requests
import json
import logging
import sys
import audio

MESSAGES_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'whatsapp-bridge', 'store', 'messages.db')
WHATSAPP_API_BASE_URL = "http://localhost:8080/api"

# This module runs inside an MCP stdio server, where stdout carries the JSON-RPC
# protocol stream — writing log lines there corrupts it. Log to stderr instead.
logger = logging.getLogger("whatsapp_mcp")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: Optional[str] = None
    media_type: Optional[str] = None

@dataclass
class Chat:
    jid: str
    name: Optional[str]
    last_message_time: Optional[datetime]
    last_message: Optional[str] = None
    last_sender: Optional[str] = None
    last_is_from_me: Optional[bool] = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")

@dataclass
class Contact:
    """A WhatsApp contact resolved across both identifier formats.

    `phone_jid` is the traditional `<number>@s.whatsapp.net` form; `lid` is
    the newer `<id>@lid` form. One of them may be empty if the bridge has
    only seen this person under a single identifier. `jid` is whichever one
    is preferred for send/lookup operations (phone JID when known).
    `phone_number` is the bare digits of the phone JID when known, otherwise
    empty — do not use it to identify the contact; use `jid` / `lid`.
    """
    phone_number: str
    name: Optional[str]
    jid: str
    phone_jid: str = ""
    lid: str = ""

@dataclass
class MessageContext:
    message: Message
    before: List[Message]
    after: List[Message]

_CONTACT_COLS = (
    "COALESCE(phone_jid,'') AS phone_jid, "
    "COALESCE(lid,'') AS lid, "
    "COALESCE(display_name,'') AS display_name, "
    "COALESCE(push_name,'') AS push_name, "
    "COALESCE(first_name,'') AS first_name, "
    "COALESCE(business_name,'') AS business_name"
)


def _normalize_phone_digits(value: str) -> str:
    """Strip everything except digits from a phone-like string."""
    if not value:
        return ""
    return "".join(c for c in value if c.isdigit())


def _lookup_contact_row(cursor, identifier: str):
    """Look up a single contact row matching `identifier` (a phone JID, an
    @lid JID, a bare phone number, or a bare LID user-part).

    Returns a 6-tuple (phone_jid, lid, display_name, push_name, first_name,
    business_name) with empty strings for NULL columns, or None on no match.

    Tolerates the contacts table being absent — returns None in that case
    so callers can fall back to chats-table heuristics during the window
    between deploying this MCP code and restarting the bridge.
    """
    if not identifier:
        return None

    try:
        # Exact match on either identifier column — the common case.
        cursor.execute(
            f"SELECT {_CONTACT_COLS} FROM contacts WHERE phone_jid = ? OR lid = ? LIMIT 1",
            (identifier, identifier),
        )
        row = cursor.fetchone()
        if row:
            return row

        # Bare-user fallback: try the same user-part as both @s.whatsapp.net
        # and @lid suffixes. Handles legacy senders and phone-number args
        # from tools.
        digits = _normalize_phone_digits(identifier)
        candidates = []
        if "@" not in identifier:
            candidates.append(identifier)
        if digits and digits != identifier:
            candidates.append(digits)
        for user in candidates:
            cursor.execute(
                f"SELECT {_CONTACT_COLS} FROM contacts WHERE phone_jid = ? OR lid = ? LIMIT 1",
                (f"{user}@s.whatsapp.net", f"{user}@lid"),
            )
            row = cursor.fetchone()
            if row:
                return row
    except sqlite3.OperationalError as e:
        # "no such table: contacts" while the bridge hasn't been restarted
        # yet. Logged at debug to avoid spamming the steady-state log.
        logger.debug("Contacts table unavailable for lookup: %s", e)
    return None


def _best_contact_name(row) -> str:
    """Pick the best human-readable name from a contact row tuple."""
    if not row:
        return ""
    _phone_jid, _lid, display, push, first, business = row
    return display or first or business or push or ""


def _all_jids_for_identifier(cursor, identifier: str) -> List[str]:
    """Return every JID and bare-user form known for the person matched by
    `identifier`. Used to expand a phone-number lookup into the set of sender
    values that may appear in the messages table (canonical + legacy)."""
    if not identifier:
        return []

    seen: List[str] = []

    def add(value: str) -> None:
        if value and value not in seen:
            seen.append(value)

    contact = _lookup_contact_row(cursor, identifier)
    if contact:
        phone_jid, lid, *_ = contact
        if phone_jid:
            add(phone_jid)
            add(phone_jid.split("@")[0])
        if lid:
            add(lid)
            add(lid.split("@")[0])

    # Always include the raw identifier so we still match rows that haven't
    # been backfilled or whose contact row is missing.
    add(identifier)
    if "@" not in identifier:
        # Bare input. The same digits as a phone-server user-part and as an
        # @lid user-part are completely unrelated people, so we must NOT
        # synthesize the cross-server form without contacts evidence — doing
        # so could fold a stranger's @lid chat into a phone-number lookup.
        # When `contact` resolved above, both forms are already in `seen`.
        digits = _normalize_phone_digits(identifier) or identifier
        if contact:
            add(f"{digits}@s.whatsapp.net")
            add(f"{digits}@lid")
        else:
            # Default to phone form only — bare phone numbers are the
            # documented input for the affected tools, and pre-LID legacy
            # senders were stored as digits with no server.
            add(f"{digits}@s.whatsapp.net")
        add(digits)
    else:
        # Full JID — safe to also match the bare user-part since legacy
        # rows for the same person on the same server might still use it.
        user_part = identifier.split("@", 1)[0]
        add(user_part)

    return seen


def get_sender_name(sender_jid: str) -> str:
    """Resolve a sender identifier (phone JID, @lid JID, or bare user part)
    to a human-readable name via the contacts table populated by the bridge."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        contact = _lookup_contact_row(cursor, sender_jid)
        name = _best_contact_name(contact)
        if name:
            return name

        # Legacy fallback: the chats table sometimes carries a usable name
        # (e.g. for group chats or contacts that pre-date the contacts table).
        cursor.execute("SELECT name FROM chats WHERE jid = ? LIMIT 1", (sender_jid,))
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]

        return sender_jid

    except sqlite3.Error as e:
        logger.error(f"Database error while getting sender name: {e}")
        return sender_jid
    finally:
        if 'conn' in locals():
            conn.close()

def format_message(message: Message, show_chat_info: bool = True) -> None:
    """Print a single message with consistent formatting."""
    output = ""
    
    if show_chat_info and message.chat_name:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] Chat: {message.chat_name} "
    else:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] "
        
    content_prefix = ""
    if hasattr(message, 'media_type') and message.media_type:
        content_prefix = f"[{message.media_type} - Message ID: {message.id} - Chat JID: {message.chat_jid}] "
    
    try:
        sender_name = get_sender_name(message.sender) if not message.is_from_me else "Me"
        output += f"From: {sender_name}: {content_prefix}{message.content}\n"
    except Exception as e:
        logger.error(f"Error formatting message: {e}")
    return output

def format_messages_list(messages: List[Message], show_chat_info: bool = True) -> None:
    output = ""
    if not messages:
        output += "No messages to display."
        return output
    
    for message in messages:
        output += format_message(message, show_chat_info)
    return output

def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1
) -> List[Message]:
    """Get messages matching the specified criteria with optional context."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        # Build base query
        query_parts = ["SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type FROM messages"]
        query_parts.append("JOIN chats ON messages.chat_jid = chats.jid")
        where_clauses = []
        params = []
        
        # Add filters
        if after:
            try:
                after = datetime.fromisoformat(after)
            except ValueError:
                raise ValueError(f"Invalid date format for 'after': {after}. Please use ISO-8601 format.")
            
            where_clauses.append("messages.timestamp > ?")
            params.append(after)

        if before:
            try:
                before = datetime.fromisoformat(before)
            except ValueError:
                raise ValueError(f"Invalid date format for 'before': {before}. Please use ISO-8601 format.")
            
            where_clauses.append("messages.timestamp < ?")
            params.append(before)

        if sender_phone_number:
            # Expand the requested identifier to every form that may appear
            # in messages.sender (phone JID, @lid JID, and bare-user variants
            # for un-backfilled rows) so phone-number lookups work even when
            # the underlying chat is @lid-based.
            sender_jids = _all_jids_for_identifier(cursor, sender_phone_number)
            placeholders = ",".join(["?"] * len(sender_jids))
            where_clauses.append(f"messages.sender IN ({placeholders})")
            params.extend(sender_jids)

        if chat_jid:
            # Likewise allow callers to pass either JID format for the same
            # person; we'll match both.
            chat_jids = _all_jids_for_identifier(cursor, chat_jid)
            placeholders = ",".join(["?"] * len(chat_jids))
            where_clauses.append(f"messages.chat_jid IN ({placeholders})")
            params.extend(chat_jids)
            
        if query:
            where_clauses.append("LOWER(messages.content) LIKE LOWER(?)")
            params.append(f"%{query}%")
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add pagination
        offset = page * limit
        query_parts.append("ORDER BY messages.timestamp DESC")
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
        
        cursor.execute(" ".join(query_parts), tuple(params))
        messages = cursor.fetchall()
        
        result = []
        for msg in messages:
            message = Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            )
            result.append(message)
            
        if include_context and result:
            # Add context for each message
            messages_with_context = []
            for msg in result:
                context = get_message_context(msg.id, context_before, context_after)
                messages_with_context.extend(context.before)
                messages_with_context.append(context.message)
                messages_with_context.extend(context.after)
            
            return format_messages_list(messages_with_context, show_chat_info=True)
            
        # Format and display messages without context
        return format_messages_list(result, show_chat_info=True)    
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> MessageContext:
    """Get context around a specific message."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        # Get the target message first
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.chat_jid, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.id = ?
        """, (message_id,))
        msg_data = cursor.fetchone()
        
        if not msg_data:
            raise ValueError(f"Message with ID {message_id} not found")
            
        target_message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[8]
        )
        
        # Get messages before
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp < ?
            ORDER BY messages.timestamp DESC
            LIMIT ?
        """, (msg_data[7], msg_data[0], before))
        
        before_messages = []
        for msg in cursor.fetchall():
            before_messages.append(Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            ))
        
        # Get messages after
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp > ?
            ORDER BY messages.timestamp ASC
            LIMIT ?
        """, (msg_data[7], msg_data[0], after))
        
        after_messages = []
        for msg in cursor.fetchall():
            after_messages.append(Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7]
            ))
        
        return MessageContext(
            message=target_message,
            before=before_messages,
            after=after_messages
        )
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Chat]:
    """Get chats matching the specified criteria."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        # Build base query. The message columns only exist in scope when the
        # LEFT JOIN below is present; otherwise select NULLs so the result keeps
        # a constant 6-column shape for the Chat(...) construction.
        if include_last_message:
            msg_cols = "messages.content as last_message, messages.sender as last_sender, messages.is_from_me as last_is_from_me"
        else:
            msg_cols = "NULL as last_message, NULL as last_sender, NULL as last_is_from_me"

        query_parts = [f"""
            SELECT
                chats.jid,
                chats.name,
                chats.last_message_time,
                {msg_cols}
            FROM chats
        """]

        if include_last_message:
            query_parts.append("""
                LEFT JOIN messages ON chats.jid = messages.chat_jid 
                AND chats.last_message_time = messages.timestamp
            """)
            
        where_clauses = []
        params = []
        
        if query:
            where_clauses.append("(LOWER(chats.name) LIKE LOWER(?) OR chats.jid LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add sorting
        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")

        # Pagination happens post-dedup (see below). Doing it in SQL is
        # incorrect because dedup collapses adjacent rows by person; an
        # SQL OFFSET would skip rows that dedup would have merged, causing
        # both duplicates across pages and silently dropped chats. Instead
        # we fetch a generous superset, dedup, then slice. WhatsApp users
        # rarely have more than a few hundred chats so the upper bound is
        # only a guardrail for pathological cases.
        query_parts.append("LIMIT ?")
        params.append(5000)

        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()

        # First pass: build raw Chat objects and resolve a contact-aware name.
        raw_chats: List[Chat] = []
        person_keys: List[Optional[str]] = []
        for chat_data in chats:
            chat_jid = chat_data[0]
            chat_name = chat_data[1]

            person_key: Optional[str] = None
            if not chat_jid.endswith("@g.us"):
                contact_row = _lookup_contact_row(cursor, chat_jid)
                if contact_row:
                    better_name = _best_contact_name(contact_row)
                    if better_name:
                        chat_name = better_name
                    # The person's identity for dedup: prefer phone JID.
                    person_key = contact_row[0] or contact_row[1] or chat_jid
                else:
                    person_key = chat_jid

            raw_chats.append(Chat(
                jid=chat_jid,
                name=chat_name,
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5],
            ))
            person_keys.append(person_key)

        # Second pass: dedup 1:1 chats by person_key, keeping the most recent.
        # Groups (@g.us, person_key is None) pass through unchanged.
        merged: dict = {}
        ordered: List[Chat] = []
        for chat, key in zip(raw_chats, person_keys):
            if key is None:
                ordered.append(chat)
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = chat
                ordered.append(chat)
                continue
            # Same person, two chat rows — keep whichever has the newer
            # last_message_time. Preserve the better name if the loser had one.
            winner, loser = (chat, existing) if (
                (chat.last_message_time or datetime.min) >
                (existing.last_message_time or datetime.min)
            ) else (existing, chat)
            if not winner.name and loser.name:
                winner.name = loser.name
            # Replace in the ordered list.
            try:
                idx = ordered.index(existing)
                ordered[idx] = winner
            except ValueError:
                ordered.append(winner)
            merged[key] = winner

        # Apply pagination after dedup so callers see consistent page
        # boundaries even when phone/lid rows for the same person merge.
        if limit > 0:
            start = page * limit
            return ordered[start:start + limit]
        return ordered

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def search_contacts(query: str) -> List[Contact]:
    """Search the contacts table by name, push name, business name, phone JID,
    LID, or bare digits. Returns one Contact per real person — phone JID and
    LID rows for the same person are already merged by the bridge.

    Also includes any chats whose name matches but lack a contacts row (e.g.
    group-only contacts the user never saved). Those come back with empty
    `phone_jid`/`lid` and the chat's JID in `jid`.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        pattern = f"%{query}%"
        # Only treat the query as a digit search when it's long enough to
        # avoid bulk-matching everyone who happens to share a 2- or 3-digit
        # country code or area code. 6 digits is a reasonable lower bound:
        # short enough to allow partial-number searches, long enough that
        # the result set is meaningful rather than dragnet.
        digits = _normalize_phone_digits(query)
        digits_pattern = f"%{digits}%" if len(digits) >= 6 else None

        # Pull from contacts first. The OR chain matches name fields plus
        # both identifier columns; the digits clause handles "+39 349 ..."
        # style queries where the user typed punctuation. If the contacts
        # table is absent (bridge not restarted yet) we fall through to a
        # chats-only search below.
        sql = f"""
            SELECT {_CONTACT_COLS}
            FROM contacts
            WHERE LOWER(display_name) LIKE LOWER(?)
               OR LOWER(push_name)    LIKE LOWER(?)
               OR LOWER(first_name)   LIKE LOWER(?)
               OR LOWER(business_name) LIKE LOWER(?)
               OR phone_jid LIKE ?
               OR lid       LIKE ?
        """
        params = [pattern, pattern, pattern, pattern, pattern, pattern]
        if digits_pattern:
            sql += " OR phone_jid LIKE ? OR lid LIKE ?"
            params.extend([digits_pattern, digits_pattern])
        sql += " ORDER BY display_name COLLATE NOCASE, push_name COLLATE NOCASE LIMIT 50"

        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        except sqlite3.OperationalError as e:
            logger.debug("Contacts table unavailable for search: %s", e)
            rows = []

        result: List[Contact] = []
        seen_keys = set()
        for row in rows:
            phone_jid, lid, display, push, first, business = row
            key = phone_jid or lid
            if key in seen_keys:
                continue
            seen_keys.add(key)
            name = display or first or business or push or None
            preferred_jid = phone_jid or lid
            result.append(Contact(
                phone_number=phone_jid.split("@")[0] if phone_jid else "",
                name=name,
                jid=preferred_jid,
                phone_jid=phone_jid,
                lid=lid,
            ))

        # Backfill: any chats with a name match that aren't represented by
        # a contacts row (no phone/lid identifier in the result so far).
        cursor.execute("""
            SELECT DISTINCT jid, name
            FROM chats
            WHERE (LOWER(name) LIKE LOWER(?) OR LOWER(jid) LIKE LOWER(?))
              AND jid NOT LIKE '%@g.us'
            ORDER BY name, jid
            LIMIT 50
        """, (pattern, pattern))
        for chat_jid, chat_name in cursor.fetchall():
            if chat_jid in seen_keys:
                continue
            # Skip if we already have this person via the contacts table.
            contact_row = _lookup_contact_row(cursor, chat_jid)
            if contact_row:
                key = contact_row[0] or contact_row[1]
                if key in seen_keys:
                    continue
            seen_keys.add(chat_jid)
            is_lid = chat_jid.endswith("@lid")
            result.append(Contact(
                phone_number="" if is_lid else chat_jid.split("@")[0],
                name=chat_name,
                jid=chat_jid,
                phone_jid="" if is_lid else chat_jid,
                lid=chat_jid if is_lid else "",
            ))

        return result

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Chat]:
    """Get all chats involving the contact.

    Args:
        jid: The contact's JID, LID, or bare phone number — any known
            identifier; the contacts table is used to expand to all forms.
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        identifiers = _all_jids_for_identifier(cursor, jid)
        if not identifiers:
            return []
        placeholders = ",".join(["?"] * len(identifiers))
        params = list(identifiers) + list(identifiers) + [limit, page * limit]
        cursor.execute(f"""
            SELECT DISTINCT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            JOIN messages m ON c.jid = m.chat_jid
            WHERE m.sender IN ({placeholders}) OR c.jid IN ({placeholders})
            ORDER BY c.last_message_time DESC
            LIMIT ? OFFSET ?
        """, tuple(params))
        
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_last_interaction(jid: str) -> str:
    """Get most recent message involving the contact.

    Accepts any known identifier for the person — phone JID, @lid JID, or
    bare digits — and expands it to all stored forms via the contacts table.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        identifiers = _all_jids_for_identifier(cursor, jid)
        if not identifiers:
            return None
        placeholders = ",".join(["?"] * len(identifiers))
        params = list(identifiers) + list(identifiers)
        cursor.execute(f"""
            SELECT
                m.timestamp,
                m.sender,
                c.name,
                m.content,
                m.is_from_me,
                c.jid,
                m.id,
                m.media_type
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.sender IN ({placeholders}) OR c.jid IN ({placeholders})
            ORDER BY m.timestamp DESC
            LIMIT 1
        """, tuple(params))
        
        msg_data = cursor.fetchone()
        
        if not msg_data:
            return None
            
        message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[7]
        )
        
        return format_message(message)
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_chat(chat_jid: str, include_last_message: bool = True) -> Optional[Chat]:
    """Get chat metadata by JID."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        query = """
            SELECT 
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
        """
        
        if include_last_message:
            query += """
                LEFT JOIN messages m ON c.jid = m.chat_jid 
                AND c.last_message_time = m.timestamp
            """
            
        query += " WHERE c.jid = ?"
        
        cursor.execute(query, (chat_jid,))
        chat_data = cursor.fetchone()
        
        if not chat_data:
            return None
            
        return Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_direct_chat_by_contact(sender_phone_number: str) -> Optional[Chat]:
    """Get chat metadata for a 1:1 conversation by any contact identifier.

    Accepts a phone number (with or without punctuation), a phone JID, or an
    @lid JID. Resolves to the most-recently-active matching chat — if a
    person has both phone-JID and @lid chat rows, the newer wins.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        identifiers = _all_jids_for_identifier(cursor, sender_phone_number)
        if not identifiers:
            return None
        placeholders = ",".join(["?"] * len(identifiers))
        cursor.execute(f"""
            SELECT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            LEFT JOIN messages m ON c.jid = m.chat_jid
                AND c.last_message_time = m.timestamp
            WHERE c.jid IN ({placeholders}) AND c.jid NOT LIKE '%@g.us'
            ORDER BY c.last_message_time DESC
            LIMIT 1
        """, tuple(identifiers))
        
        chat_data = cursor.fetchone()
        
        if not chat_data:
            return None
            
        return Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def send_message(recipient: str, message: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "message": message,
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_file(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": media_path
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_audio_message(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        if not media_path.endswith(".ogg"):
            try:
                media_path = audio.convert_to_opus_ogg_temp(media_path)
            except Exception as e:
                return False, f"Error converting file to opus ogg. You likely need to install ffmpeg: {str(e)}"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": media_path
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def download_media(message_id: str, chat_jid: str) -> Optional[str]:
    """Download media from a message and return the local file path.
    
    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
    
    Returns:
        The local file path if download was successful, None otherwise
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/download"
        payload = {
            "message_id": message_id,
            "chat_jid": chat_jid
        }
        
        response = requests.post(url, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                path = result.get("path")
                logger.info(f"Media downloaded successfully: {path}")
                return path
            else:
                logger.error(f"Download failed: {result.get('message', 'Unknown error')}")
                return None
        else:
            logger.error(f"Error: HTTP {response.status_code} - {response.text}")
            return None

    except requests.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Error parsing response: {response.text}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return None
