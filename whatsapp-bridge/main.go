package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages
	db, err := sql.Open("sqlite3", "file:store/messages.db?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist.
	//
	// contacts is the fork's local cache of whatsmeow's contact + LID stores
	// (whatsapp.db). We mirror it into messages.db so the Python MCP can
	// resolve identity across phone JIDs and @lid JIDs in a single query.
	// phone_jid is the canonical row key when known; lid-only contacts are
	// allowed (phone_jid empty string, lid populated).
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);

		CREATE TABLE IF NOT EXISTS contacts (
			phone_jid TEXT UNIQUE,
			lid TEXT UNIQUE,
			display_name TEXT,
			push_name TEXT,
			first_name TEXT,
			business_name TEXT,
			updated_at TIMESTAMP,
			CHECK (phone_jid IS NOT NULL OR lid IS NOT NULL)
		);
		CREATE INDEX IF NOT EXISTS idx_contacts_lid ON contacts(lid);

		CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages 
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// ContactRow is the resolved contact row used for identity lookups.
// Empty strings mean "unknown" (NULL in the database).
type ContactRow struct {
	PhoneJID     string
	LID          string
	DisplayName  string
	PushName     string
	FirstName    string
	BusinessName string
}

// BestName returns the best human-readable name available, preferring the
// most authoritative source (display name from the contact book) over
// push/business names.
func (c ContactRow) BestName() string {
	switch {
	case c.DisplayName != "":
		return c.DisplayName
	case c.FirstName != "":
		return c.FirstName
	case c.BusinessName != "":
		return c.BusinessName
	case c.PushName != "":
		return c.PushName
	}
	return ""
}

// nullIfEmpty converts an empty string to a nil interface so the database/sql
// driver writes a true NULL (the UNIQUE constraints on contacts.phone_jid /
// contacts.lid need NULLs, not empty strings, to allow multiple "unknown" rows).
func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// LookupContactByJID resolves any sender/chat JID (phone JID, @lid JID, or a
// bare user part) to the contact row that owns it. Returns nil if no match.
func (store *MessageStore) LookupContactByJID(jid string) (*ContactRow, error) {
	if jid == "" {
		return nil, nil
	}

	// Try a direct match on either identifier first; this is the common path.
	rows, err := store.db.Query(`
		SELECT COALESCE(phone_jid,''), COALESCE(lid,''),
		       COALESCE(display_name,''), COALESCE(push_name,''),
		       COALESCE(first_name,''), COALESCE(business_name,'')
		FROM contacts
		WHERE phone_jid = ? OR lid = ?
		LIMIT 1
	`, jid, jid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var c ContactRow
		if err := rows.Scan(&c.PhoneJID, &c.LID, &c.DisplayName, &c.PushName, &c.FirstName, &c.BusinessName); err != nil {
			return nil, err
		}
		return &c, nil
	}

	// Fallback: if the caller passed a bare user part (no '@'), try to match
	// the user portion of stored JIDs. This handles old sender rows that were
	// written before sender normalization landed.
	if !strings.Contains(jid, "@") {
		pattern1 := jid + "@s.whatsapp.net"
		pattern2 := jid + "@lid"
		rows2, err := store.db.Query(`
			SELECT COALESCE(phone_jid,''), COALESCE(lid,''),
			       COALESCE(display_name,''), COALESCE(push_name,''),
			       COALESCE(first_name,''), COALESCE(business_name,'')
			FROM contacts
			WHERE phone_jid = ? OR lid = ?
			LIMIT 1
		`, pattern1, pattern2)
		if err != nil {
			return nil, err
		}
		defer rows2.Close()
		if rows2.Next() {
			var c ContactRow
			if err := rows2.Scan(&c.PhoneJID, &c.LID, &c.DisplayName, &c.PushName, &c.FirstName, &c.BusinessName); err != nil {
				return nil, err
			}
			return &c, nil
		}
	}
	return nil, nil
}

// CanonicalSenderJID returns the canonical sender form for an incoming JID.
// Phone JID wins when known, otherwise LID, otherwise the input is returned
// unchanged (with any device part stripped by the caller before invocation).
func (store *MessageStore) CanonicalSenderJID(jid string) string {
	if jid == "" {
		return jid
	}
	contact, err := store.LookupContactByJID(jid)
	if err != nil || contact == nil {
		return jid
	}
	if contact.PhoneJID != "" {
		return contact.PhoneJID
	}
	if contact.LID != "" {
		return contact.LID
	}
	return jid
}

// UpsertContact merges a single contact row into the contacts table. At
// least one of phoneJID or lid must be non-empty. Empty string fields are
// treated as "no new info" and won't clobber existing values.
//
// This handles the merge case where we previously had a row keyed only by
// lid and later learn the phone_jid (or vice versa) — that row is updated
// in place rather than producing a UNIQUE constraint conflict.
func (store *MessageStore) UpsertContact(phoneJID, lid, displayName, pushName, firstName, businessName string) error {
	if phoneJID == "" && lid == "" {
		return fmt.Errorf("UpsertContact: need at least one of phone_jid or lid")
	}

	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Find any existing rows that match either identifier. There can be at
	// most two: one matching phone_jid, one matching lid. If they collide on
	// a single row we just update; if they're two separate rows we merge.
	type existingRow struct {
		phoneJID, lid, displayName, pushName, firstName, businessName sql.NullString
	}
	rows, err := tx.Query(`
		SELECT phone_jid, lid, display_name, push_name, first_name, business_name
		FROM contacts
		WHERE (? != '' AND phone_jid = ?) OR (? != '' AND lid = ?)
	`, phoneJID, phoneJID, lid, lid)
	if err != nil {
		return err
	}
	var existing []existingRow
	for rows.Next() {
		var r existingRow
		if err := rows.Scan(&r.phoneJID, &r.lid, &r.displayName, &r.pushName, &r.firstName, &r.businessName); err != nil {
			rows.Close()
			return err
		}
		existing = append(existing, r)
	}
	rows.Close()

	// Merge whatever we found with the new info; later writes win for
	// non-empty fields.
	finalPN := phoneJID
	finalLID := lid
	finalDisplay := displayName
	finalPush := pushName
	finalFirst := firstName
	finalBusiness := businessName
	for _, r := range existing {
		if finalPN == "" && r.phoneJID.Valid {
			finalPN = r.phoneJID.String
		}
		if finalLID == "" && r.lid.Valid {
			finalLID = r.lid.String
		}
		if finalDisplay == "" && r.displayName.Valid {
			finalDisplay = r.displayName.String
		}
		if finalPush == "" && r.pushName.Valid {
			finalPush = r.pushName.String
		}
		if finalFirst == "" && r.firstName.Valid {
			finalFirst = r.firstName.String
		}
		if finalBusiness == "" && r.businessName.Valid {
			finalBusiness = r.businessName.String
		}
	}

	// Wipe the matching rows and write one merged row.
	if len(existing) > 0 {
		_, err = tx.Exec(`
			DELETE FROM contacts
			WHERE (? != '' AND phone_jid = ?) OR (? != '' AND lid = ?)
		`, phoneJID, phoneJID, lid, lid)
		if err != nil {
			return err
		}
	}

	_, err = tx.Exec(`
		INSERT INTO contacts (phone_jid, lid, display_name, push_name, first_name, business_name, updated_at)
		VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?)
	`, nullIfEmpty(finalPN), nullIfEmpty(finalLID), finalDisplay, finalPush, finalFirst, finalBusiness, time.Now())
	if err != nil {
		return err
	}

	return tx.Commit()
}

// CountContacts returns the number of rows in the contacts table.
func (store *MessageStore) CountContacts() (int, error) {
	var n int
	err := store.db.QueryRow("SELECT COUNT(*) FROM contacts").Scan(&n)
	return n, err
}

// GetMeta reads a value from the meta key/value table. Returns ("", nil) if
// the key doesn't exist.
func (store *MessageStore) GetMeta(key string) (string, error) {
	var v string
	err := store.db.QueryRow("SELECT value FROM meta WHERE key = ?", key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return v, err
}

// SetMeta writes a value to the meta key/value table.
func (store *MessageStore) SetMeta(key, value string) error {
	_, err := store.db.Exec("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", key, value)
	return err
}

// syncContactsMu serializes calls to syncContactsFromWhatsmeow. Two paths
// can fire concurrently — the post-connect startup goroutine and the
// post-HistorySync goroutine. Both run a full DELETE+INSERT, so overlapping
// runs are wasteful and produce confusing log output; the mutex makes the
// second one wait for the first to finish.
var syncContactsMu sync.Mutex

// syncContactsFromWhatsmeow does a one-shot rebuild of the contacts table
// from whatsmeow's internal stores (whatsapp.db's whatsmeow_contacts and
// whatsmeow_lid_map). The table is replaced wholesale so stale rows are
// dropped — incremental updates afterward come from events.Contact /
// events.PushName handlers.
func syncContactsFromWhatsmeow(client *whatsmeow.Client, store *MessageStore, logger waLog.Logger) error {
	syncContactsMu.Lock()
	defer syncContactsMu.Unlock()

	ctx := context.Background()

	contacts, err := client.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return fmt.Errorf("GetAllContacts: %w", err)
	}
	logger.Infof("Syncing %d contacts from whatsmeow", len(contacts))

	// Collect every PN we know about so we can bulk-resolve LIDs in one call.
	var phoneJIDs []types.JID
	for jid := range contacts {
		if jid.Server == types.DefaultUserServer {
			phoneJIDs = append(phoneJIDs, jid.ToNonAD())
		}
	}
	pnToLID, err := client.Store.LIDs.GetManyLIDsForPNs(ctx, phoneJIDs)
	if err != nil {
		logger.Warnf("GetManyLIDsForPNs: %v (continuing without bulk LID map)", err)
		pnToLID = make(map[types.JID]types.JID)
	}

	// Build a merged set keyed by canonical identity. Both PN-keyed and
	// LID-keyed entries for the same person need to collapse into one row.
	type row struct {
		phoneJID, lid string
		info          types.ContactInfo
	}
	merged := make(map[string]*row) // key = phone JID if known, else lid
	for jid, info := range contacts {
		nonAD := jid.ToNonAD()
		var phoneJID, lid string
		switch nonAD.Server {
		case types.DefaultUserServer:
			phoneJID = nonAD.String()
			if mapped, ok := pnToLID[nonAD]; ok && !mapped.IsEmpty() {
				lid = mapped.ToNonAD().String()
			}
		case types.HiddenUserServer:
			lid = nonAD.String()
			pn, err := client.Store.LIDs.GetPNForLID(ctx, nonAD)
			if err == nil && !pn.IsEmpty() {
				phoneJID = pn.ToNonAD().String()
			}
		default:
			continue
		}

		key := phoneJID
		if key == "" {
			key = lid
		}
		if existing, ok := merged[key]; ok {
			if existing.phoneJID == "" {
				existing.phoneJID = phoneJID
			}
			if existing.lid == "" {
				existing.lid = lid
			}
			if existing.info.FullName == "" {
				existing.info.FullName = info.FullName
			}
			if existing.info.FirstName == "" {
				existing.info.FirstName = info.FirstName
			}
			if existing.info.PushName == "" {
				existing.info.PushName = info.PushName
			}
			if existing.info.BusinessName == "" {
				existing.info.BusinessName = info.BusinessName
			}
		} else {
			merged[key] = &row{phoneJID: phoneJID, lid: lid, info: info}
		}
	}

	// Replace the table in one transaction so callers always see a
	// consistent snapshot.
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM contacts"); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO contacts (phone_jid, lid, display_name, push_name, first_name, business_name, updated_at)
		VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now()
	inserted := 0
	for _, r := range merged {
		if r.phoneJID == "" && r.lid == "" {
			continue
		}
		_, err := stmt.Exec(nullIfEmpty(r.phoneJID), nullIfEmpty(r.lid),
			r.info.FullName, r.info.PushName, r.info.FirstName, r.info.BusinessName, now)
		if err != nil {
			logger.Warnf("Failed to insert contact (phone=%s lid=%s): %v", r.phoneJID, r.lid, err)
			continue
		}
		inserted++
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	logger.Infof("Synced %d contact rows into messages.db", inserted)
	return nil
}

// backfillSenderJIDs upgrades historical messages.sender rows from the legacy
// bare-user-part format (e.g. "14155551234" or "113851144110310") to canonical
// full JIDs ("14155551234@s.whatsapp.net" / "113851144110310@lid") using the
// contacts table. Rows that can't be resolved are left alone; the Python read
// path has a bare-user fallback that still matches them.
//
// Idempotent and gated by a meta marker so subsequent startups skip the scan.
// Bump the marker key when the resolution rules change.
func backfillSenderJIDs(store *MessageStore, logger waLog.Logger) error {
	const markerKey = "backfill.senders.v1"
	marker, err := store.GetMeta(markerKey)
	if err != nil {
		return fmt.Errorf("read backfill marker: %w", err)
	}
	if marker == "done" {
		return nil
	}

	rows, err := store.db.Query(`
		SELECT DISTINCT sender FROM messages
		WHERE sender != '' AND sender NOT LIKE '%@%'
	`)
	if err != nil {
		return err
	}
	var bareSenders []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			rows.Close()
			return err
		}
		bareSenders = append(bareSenders, s)
	}
	rows.Close()

	if len(bareSenders) == 0 {
		logger.Infof("Sender backfill: nothing to do")
		return store.SetMeta(markerKey, "done")
	}

	logger.Infof("Sender backfill: scanning %d unique bare senders", len(bareSenders))

	// Wrap the per-sender UPDATEs in one transaction. Without it each
	// UPDATE is an autocommit and SQLite issues a separate fsync per row,
	// making the backfill orders of magnitude slower on busy databases.
	// The lookup itself is read-only so it stays outside the tx.
	tx, err := store.db.Begin()
	if err != nil {
		return fmt.Errorf("begin backfill tx: %w", err)
	}
	defer tx.Rollback()

	updated := 0
	resolved := 0
	for _, raw := range bareSenders {
		contact, err := store.LookupContactByJID(raw)
		if err != nil {
			logger.Warnf("backfill: lookup failed for %s: %v", raw, err)
			continue
		}
		if contact == nil {
			// Stays bare for now. The Python read-side fallback in
			// _all_jids_for_identifier still matches bare-user rows, so
			// callers continue to work; but a future bridge version that
			// learns this contact won't retroactively normalize old rows
			// unless we bump the marker key to re-run the backfill.
			continue
		}
		var canonical string
		if contact.PhoneJID != "" {
			canonical = contact.PhoneJID
		} else if contact.LID != "" {
			canonical = contact.LID
		}
		if canonical == "" || canonical == raw {
			continue
		}
		result, err := tx.Exec("UPDATE messages SET sender = ? WHERE sender = ?", canonical, raw)
		if err != nil {
			logger.Warnf("backfill: update %s -> %s failed: %v", raw, canonical, err)
			continue
		}
		n, _ := result.RowsAffected()
		updated += int(n)
		resolved++
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit backfill tx: %w", err)
	}

	logger.Infof("Sender backfill: resolved %d/%d distinct senders, updated %d message rows", resolved, len(bareSenders), updated)
	return store.SetMeta(markerKey, "done")
}

// identifiersFor returns (phoneJID, lid) for a JID, based on its server type.
// Only one of the two will be populated unless we can resolve the cross-mapping
// from whatsmeow's LID store.
func identifiersFor(client *whatsmeow.Client, jid types.JID) (string, string) {
	ctx := context.Background()
	nonAD := jid.ToNonAD()
	var phoneJID, lid string
	switch nonAD.Server {
	case types.DefaultUserServer:
		phoneJID = nonAD.String()
		if mapped, err := client.Store.LIDs.GetLIDForPN(ctx, nonAD); err == nil && !mapped.IsEmpty() {
			lid = mapped.ToNonAD().String()
		}
	case types.HiddenUserServer:
		lid = nonAD.String()
		if pn, err := client.Store.LIDs.GetPNForLID(ctx, nonAD); err == nil && !pn.IsEmpty() {
			phoneJID = pn.ToNonAD().String()
		}
	}
	return phoneJID, lid
}

// handleContactEvent updates the contacts table when whatsmeow's contact
// store changes (e.g. the user edits a contact on another device).
func handleContactEvent(client *whatsmeow.Client, store *MessageStore, evt *events.Contact, logger waLog.Logger) {
	info, err := client.Store.Contacts.GetContact(context.Background(), evt.JID.ToNonAD())
	if err != nil {
		logger.Warnf("Contact event for %s: GetContact failed: %v", evt.JID, err)
		return
	}
	phoneJID, lid := identifiersFor(client, evt.JID)
	if phoneJID == "" && lid == "" {
		return
	}
	if err := store.UpsertContact(phoneJID, lid, info.FullName, info.PushName, info.FirstName, info.BusinessName); err != nil {
		logger.Warnf("Contact event upsert failed for %s: %v", evt.JID, err)
	}
}

// handlePushNameEvent updates the cached push name for a sender.
func handlePushNameEvent(store *MessageStore, evt *events.PushName, logger waLog.Logger) {
	var phoneJID, lid string
	switch evt.JID.ToNonAD().Server {
	case types.DefaultUserServer:
		phoneJID = evt.JID.ToNonAD().String()
	case types.HiddenUserServer:
		lid = evt.JID.ToNonAD().String()
	default:
		return
	}
	// JIDAlt is set when whatsmeow knows the cross-mapped identifier; use it
	// to populate the other column when we have it.
	if !evt.JIDAlt.IsEmpty() {
		alt := evt.JIDAlt.ToNonAD()
		switch alt.Server {
		case types.DefaultUserServer:
			if phoneJID == "" {
				phoneJID = alt.String()
			}
		case types.HiddenUserServer:
			if lid == "" {
				lid = alt.String()
			}
		}
	}
	if phoneJID == "" && lid == "" {
		return
	}
	if err := store.UpsertContact(phoneJID, lid, "", evt.NewPushName, "", ""); err != nil {
		logger.Warnf("PushName event upsert failed for %s: %v", evt.JID, err)
	}
}

// handleBusinessNameEvent updates the cached business name for a sender.
func handleBusinessNameEvent(store *MessageStore, evt *events.BusinessName, logger waLog.Logger) {
	var phoneJID, lid string
	switch evt.JID.ToNonAD().Server {
	case types.DefaultUserServer:
		phoneJID = evt.JID.ToNonAD().String()
	case types.HiddenUserServer:
		lid = evt.JID.ToNonAD().String()
	default:
		return
	}
	if err := store.UpsertContact(phoneJID, lid, "", "", "", evt.NewBusinessName); err != nil {
		logger.Warnf("BusinessName event upsert failed for %s: %v", evt.JID, err)
	}
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// For now, we're ignoring non-text messages
	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(client *whatsmeow.Client, recipient string, message string, mediaPath string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		// Read media file
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types (for any other file type)
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title:         proto.String(mediaPath[strings.LastIndex(mediaPath, "/")+1:]),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	// Send message
	_, err = client.SendMessage(context.Background(), recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database
	chatJID := msg.Info.Chat.String()
	// Persist the full sender JID (server included, device stripped) so we
	// can distinguish phone JIDs from @lid JIDs downstream. Then map to the
	// canonical form via the contacts table — if we know both identifiers
	// for this person we always prefer phone JID for consistency.
	rawSenderJID := msg.Info.Sender.ToNonAD().String()
	sender := messageStore.CanonicalSenderJID(rawSenderJID)

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, msg.Info.Sender.User, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Generate a local path for the file
	localPath = fmt.Sprintf("%s/%s", chatDir, filename)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(localPath); err == nil {
		// File exists, return it
		return true, mediaType, filename, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client
	mediaData, err := client.Download(context.Background(), downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(localPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// The direct path is typically in the URL, we need to extract it
	// Example URL: https://mmg.whatsapp.net/v/t62.7118-24/13812002_698058036224062_3424455886509161511_n.enc?ccb=11-4&oh=...

	// Find the path part after the domain
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathPart := parts[1]

	// Remove query parameters
	pathPart = strings.SplitN(pathPart, "?", 2)[0]

	// Create proper direct path format
	return "/" + pathPart
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int) {
	// Handler for sending messages
	http.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		fmt.Println("Received request to send message", req.Message, req.MediaPath)

		// Send the message
		success, message := sendWhatsAppMessage(client, req.Recipient, req.Message, req.MediaPath)
		fmt.Println("Message sent", success, message)
		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Set appropriate status code
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}

		// Send response
		json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for downloading media
	http.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Download the media
		success, mediaType, filename, path, err := downloadMedia(client, messageStore, req.MessageID, req.ChatJID)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Start the server
	serverAddr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)

	// Run server in a goroutine so it doesn't block.
	// Exit the process on failure so launchd / supervisors can restart us —
	// the bridge is useless to the MCP server without the REST endpoint.
	go func() {
		if err := http.ListenAndServe(serverAddr, nil); err != nil {
			fmt.Printf("REST API server error: %v\n", err)
			os.Exit(1)
		}
	}()
}

func main() {
	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	container, err := sqlstore.New(context.Background(), "sqlite3", "file:store/whatsapp.db?_foreign_keys=on", dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// Identify as a recognizable client so iOS pairing doesn't show "Unknown"
	// (workaround for https://github.com/tulir/whatsmeow/issues/1039).
	store.DeviceProps.Os = proto.String("Chrome (Linux)")
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events. The first history sync after pairing
			// is also when whatsmeow's contact store gets populated, so
			// re-run the contact sync afterward to mirror it into messages.db.
			// The backfill is re-attempted too — it self-skips if the marker
			// already says done.
			handleHistorySync(client, messageStore, v, logger)
			go func() {
				if err := syncContactsFromWhatsmeow(client, messageStore, logger); err != nil {
					logger.Warnf("Post-history-sync contact rebuild failed: %v", err)
					return
				}
				if err := backfillSenderJIDs(messageStore, logger); err != nil {
					logger.Warnf("Sender backfill failed: %v", err)
				}
			}()

		case *events.Contact:
			handleContactEvent(client, messageStore, v, logger)

		case *events.PushName:
			handlePushNameEvent(messageStore, v, logger)

		case *events.BusinessName:
			handleBusinessNameEvent(messageStore, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}

		// Print QR code for pairing with phone
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			case "success":
				connected <- true
			default:
				logger.Warnf("QR channel event: %s (error=%v)", evt.Event, evt.Error)
			}
			if evt.Event == "success" {
				break
			}
		}

		// Wait for connection
		select {
		case <-connected:
			fmt.Println("\nSuccessfully connected and authenticated!")
		case <-time.After(3 * time.Minute):
			logger.Errorf("Timeout waiting for QR code scan")
			return
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	// Mirror whatsmeow's contact + LID stores into messages.db so the Python
	// MCP can resolve identity across phone JIDs and @lid JIDs. Background
	// goroutine so a slow contact store doesn't delay REST startup. The
	// HistorySync event handler also re-runs this for first-time pairings
	// where the contact store fills in only after history arrives.
	//
	// Once contacts are populated, run the one-time sender backfill so old
	// messages stored with bare-user-part senders pick up canonical JIDs.
	go func() {
		if err := syncContactsFromWhatsmeow(client, messageStore, logger); err != nil {
			logger.Warnf("Initial contact sync failed: %v", err)
			return
		}
		if err := backfillSenderJIDs(messageStore, logger); err != nil {
			logger.Warnf("Sender backfill failed: %v", err)
		}
	}()

	// Start REST API server
	startRESTServer(client, messageStore, 8080)

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Prefer the local contacts table — it resolves names across phone JID
		// and @lid identifiers in one lookup. Fall back to whatsmeow's contact
		// store only if our cache hasn't seen this person yet.
		if cached, err := messageStore.LookupContactByJID(chatJID); err == nil && cached != nil {
			if n := cached.BestName(); n != "" {
				name = n
			}
		}
		if name == "" {
			contact, err := client.Store.Contacts.GetContact(context.Background(), jid.ToNonAD())
			if err == nil && contact.FullName != "" {
				name = contact.FullName
			} else if err == nil && contact.PushName != "" {
				name = contact.PushName
			} else if sender != "" {
				name = sender
			} else {
				name = jid.User
			}
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender as a full JID (device stripped). Falling
				// back to the chat JID for 1:1 messages keeps the same person
				// across all three identifier formats by virtue of the
				// canonical lookup below.
				var senderJID types.JID
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						if parsed, err := types.ParseJID(*msg.Message.Key.Participant); err == nil {
							senderJID = parsed.ToNonAD()
						}
					} else if isFromMe && client.Store.ID != nil {
						senderJID = client.Store.ID.ToNonAD()
					} else {
						senderJID = jid.ToNonAD()
					}
				} else {
					senderJID = jid.ToNonAD()
				}
				rawSenderJID := senderJID.String()
				sender := messageStore.CanonicalSenderJID(rawSenderJID)

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// Request history sync from the server
func requestHistorySync(client *whatsmeow.Client) {
	if client == nil {
		fmt.Println("Client is not initialized. Cannot request history sync.")
		return
	}

	if !client.IsConnected() {
		fmt.Println("Client is not connected. Please ensure you are connected to WhatsApp first.")
		return
	}

	if client.Store.ID == nil {
		fmt.Println("Client is not logged in. Please scan the QR code first.")
		return
	}

	// Build and send a history sync request
	historyMsg := client.BuildHistorySyncRequest(nil, 100)
	if historyMsg == nil {
		fmt.Println("Failed to build history sync request.")
		return
	}

	_, err := client.SendMessage(context.Background(), types.JID{
		Server: "s.whatsapp.net",
		User:   "status",
	}, historyMsg)

	if err != nil {
		fmt.Printf("Failed to request history sync: %v\n", err)
	} else {
		fmt.Println("History sync requested. Waiting for server response...")
	}
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}
