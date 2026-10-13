-- ════════════════════════════════════════════════════════════════════════════
-- 0001_init_event_log_typed.sql — Typed Event Log + Derived Entities Table
--
-- ARCHITECTURE:
--   1. entity_kinds  — Lookup table of all supported domain entity types.
--   2. entity_events — Append-only global event log carrying event metadata
--                      (sortable ULID cursor, entity_kind, entity_id, version,
--                      event_type, timestamp, audit reason).
--   3. entities      — Derived current-head metadata per (entity_kind, entity_id),
--                      maintained automatically by triggers on entity_events.
--                      Eliminates ROW_NUMBER() scans on current-state reads.
--   4. *_states      — Append-only typed state snapshots per entity kind.
--                      One row per 'created' or 'updated' event; no row on
--                      'deleted' events.
--   5. <plural>      — Current-state views (VIEW_SNAPSHOT) joining `entities`
--                      to `*_states` via PRIMARY KEY (event_id).
--   6. <kind>_events — History/stream views (VIEW_EVENTS) joining
--                      `entity_events` to `*_states`.
--
-- REQUIRED CONNECTION PRAGMAS (set on every connection in Rust):
--   PRAGMA journal_mode = WAL;
--   PRAGMA synchronous  = NORMAL;
--   PRAGMA foreign_keys = ON;
--   PRAGMA busy_timeout = 5000;
-- ════════════════════════════════════════════════════════════════════════════

-- ─────────────────────────── 1. entity kinds ──────────────────────────────

CREATE TABLE entity_kinds (
  name TEXT PRIMARY KEY
) WITHOUT ROWID;

INSERT INTO entity_kinds (name) VALUES
  ('order'),
  ('budget'),
  ('spec'),
  ('known_machine'),
  ('ship'),
  ('status'),
  ('alarm'),
  ('log');

-- ─────────────────────────── 2. generic event log ─────────────────────────

CREATE TABLE entity_events (
  -- Gapless local insertion order (monotonic across all writes)
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,

  -- Sortable cursor (ULID) — maps to common.EventMeta.event_id
  event_id    TEXT    NOT NULL,

  entity_kind TEXT    NOT NULL REFERENCES entity_kinds (name),
  entity_id   TEXT    NOT NULL,

  -- 1-based ordinal within (entity_kind, entity_id)
  version     INTEGER NOT NULL CHECK (version >= 1),

  -- Maps to common.Op ('created' = OP_CREATED, 'updated' = OP_UPDATED, 'deleted' = OP_DELETED)
  event_type  TEXT    NOT NULL CHECK (event_type IN ('created', 'updated', 'deleted')),

  -- RFC3339 UTC with 6 fractional digits, e.g. 2026-10-13T09:30:00.000000Z (27 chars)
  time        TEXT    NOT NULL CHECK (length(time) = 27),

  -- Optional free-form audit note (PlaceOptions.reason / RemoveOptions.reason)
  reason      TEXT,

  UNIQUE (event_id),
  -- Composite FK target so *_states rows can only attach to an event of their own kind
  UNIQUE (event_id, entity_kind),
  -- Per-entity fold and replay key
  UNIQUE (entity_kind, entity_id, version)
);

-- Per-kind cursor streaming: WHERE entity_kind = ? AND event_id > ?
CREATE INDEX idx_entity_events_stream ON entity_events (entity_kind, event_id);

-- Time-window reads (ReadOptions.after_time / before_time)
CREATE INDEX idx_entity_events_time ON entity_events (time);

-- Per-kind + time-window reads
CREATE INDEX idx_entity_events_kind_time ON entity_events (entity_kind, time);

-- Append-only guards on entity_events
CREATE TRIGGER trg_entity_events_no_update BEFORE UPDATE ON entity_events
BEGIN
  SELECT RAISE(ABORT, 'entity_events is append-only');
END;

CREATE TRIGGER trg_entity_events_no_delete BEFORE DELETE ON entity_events
BEGIN
  SELECT RAISE(ABORT, 'entity_events is append-only');
END;

-- ─────────────────────────── 3. generic entities table ────────────────────
-- Derived head pointer for each (entity_kind, entity_id).
-- Automatically maintained by triggers when rows are inserted into entity_events.

CREATE TABLE entities (
  entity_kind   TEXT    NOT NULL REFERENCES entity_kinds (name),
  entity_id     TEXT    NOT NULL,
  version       INTEGER NOT NULL,
  last_event_id TEXT    NOT NULL UNIQUE REFERENCES entity_events (event_id),
  created_time  TEXT    NOT NULL,
  updated_time  TEXT    NOT NULL,
  is_deleted    INTEGER NOT NULL CHECK (is_deleted IN (0, 1)),
  PRIMARY KEY (entity_kind, entity_id)
) WITHOUT ROWID;

-- Fast scan of active entities per kind (powers VIEW_SNAPSHOT views)
CREATE INDEX idx_entities_active_kind
  ON entities (entity_kind, entity_id)
  WHERE is_deleted = 0;

-- Fast reverse lookup when filtering *_states by secondary indexes first
CREATE UNIQUE INDEX idx_entities_active_last_event
  ON entities (last_event_id)
  WHERE is_deleted = 0;

-- Prevent deleting rows from `entities` directly (soft-deletes happen via 'deleted' events)
CREATE TRIGGER trg_entities_no_delete BEFORE DELETE ON entities
BEGIN
  SELECT RAISE(ABORT, 'entities is derived from entity_events and cannot be deleted directly');
END;

-- Enforce valid version sequence & lifecycle transitions before inserting an event
CREATE TRIGGER trg_entity_events_validate_lifecycle
BEFORE INSERT ON entity_events
BEGIN
  -- 1. First event for an entity must have version = 1 and event_type = 'created'
  SELECT RAISE(ABORT, 'initial event for a new entity must have version = 1 and event_type = created')
  WHERE NOT EXISTS (
    SELECT 1 FROM entities
    WHERE entity_kind = NEW.entity_kind AND entity_id = NEW.entity_id
  ) AND (NEW.version <> 1 OR NEW.event_type <> 'created');

  -- 2. Subsequent events must increment entities.version by exactly 1
  SELECT RAISE(ABORT, 'event version must equal current entity version + 1')
  WHERE EXISTS (
    SELECT 1 FROM entities
    WHERE entity_kind = NEW.entity_kind
      AND entity_id   = NEW.entity_id
      AND version + 1 <> NEW.version
  );

  -- 3. Active entities can only receive 'updated' or 'deleted' events
  SELECT RAISE(ABORT, 'active entity cannot receive a created event')
  WHERE EXISTS (
    SELECT 1 FROM entities
    WHERE entity_kind = NEW.entity_kind
      AND entity_id   = NEW.entity_id
      AND is_deleted  = 0
  ) AND NEW.event_type = 'created';

  -- 4. Deleted entities can only be resurrected by a 'created' event
  SELECT RAISE(ABORT, 'deleted entity can only receive a created event')
  WHERE EXISTS (
    SELECT 1 FROM entities
    WHERE entity_kind = NEW.entity_kind
      AND entity_id   = NEW.entity_id
      AND is_deleted  = 1
  ) AND NEW.event_type <> 'created';
END;

-- Automatically update the derived `entities` pointer whenever an event is inserted
CREATE TRIGGER trg_entity_events_sync_entities
AFTER INSERT ON entity_events
BEGIN
  INSERT INTO entities (
    entity_kind,
    entity_id,
    version,
    last_event_id,
    created_time,
    updated_time,
    is_deleted
  )
  VALUES (
    NEW.entity_kind,
    NEW.entity_id,
    NEW.version,
    NEW.event_id,
    NEW.time,
    NEW.time,
    CASE WHEN NEW.event_type = 'deleted' THEN 1 ELSE 0 END
  )
  ON CONFLICT (entity_kind, entity_id) DO UPDATE SET
    version       = excluded.version,
    last_event_id = excluded.last_event_id,
    -- Reset created_time if the entity is being re-created after a prior deletion
    created_time  = CASE
                      WHEN entities.is_deleted = 1 AND NEW.event_type = 'created'
                      THEN excluded.created_time
                      ELSE entities.created_time
                    END,
    updated_time  = excluded.updated_time,
    is_deleted    = excluded.is_deleted;
END;

-- ─────────────────────────── 4. typed state tables ────────────────────────
-- Write order inside a transaction:
--   1. INSERT INTO entity_events (...)
--   2. INSERT INTO <kind>_states (...)  (for 'created' and 'updated' events)

-- ═══ 4.1 ORDERS (host.Order) ═══
CREATE TABLE order_states (
  event_id    TEXT    NOT NULL,
  entity_kind TEXT    NOT NULL DEFAULT 'order' CHECK (entity_kind = 'order'),
  id          TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  name        TEXT,
  budget_id   TEXT,
  spec_id     TEXT,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE INDEX idx_order_states_budget_id ON order_states (budget_id);
CREATE INDEX idx_order_states_spec_id   ON order_states (spec_id);

CREATE TRIGGER trg_order_states_no_deleted BEFORE INSERT ON order_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'order_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_order_states_no_update BEFORE UPDATE ON order_states
BEGIN
  SELECT RAISE(ABORT, 'order_states is append-only');
END;

CREATE TRIGGER trg_order_states_no_delete BEFORE DELETE ON order_states
BEGIN
  SELECT RAISE(ABORT, 'order_states is append-only');
END;

-- ═══ 4.2 BUDGETS (host.Budget) ═══
CREATE TABLE budget_states (
  event_id           TEXT    NOT NULL,
  entity_kind        TEXT    NOT NULL DEFAULT 'budget' CHECK (entity_kind = 'budget'),
  id                 TEXT    NOT NULL,
  version            INTEGER NOT NULL,
  total_budget       REAL,
  max_price_per_hour REAL,
  lease_timeout      REAL,
  health_timeout     REAL,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE TRIGGER trg_budget_states_no_deleted BEFORE INSERT ON budget_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'budget_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_budget_states_no_update BEFORE UPDATE ON budget_states
BEGIN
  SELECT RAISE(ABORT, 'budget_states is append-only');
END;

CREATE TRIGGER trg_budget_states_no_delete BEFORE DELETE ON budget_states
BEGIN
  SELECT RAISE(ABORT, 'budget_states is append-only');
END;

-- ═══ 4.3 SPECS (host.Spec) ═══
CREATE TABLE spec_states (
  event_id      TEXT    NOT NULL,
  entity_kind   TEXT    NOT NULL DEFAULT 'spec' CHECK (entity_kind = 'spec'),
  id            TEXT    NOT NULL,
  version       INTEGER NOT NULL,
  gpu_name      TEXT,
  num_gpus      INTEGER,
  min_cpu_cores INTEGER,
  min_ram_mb    INTEGER,
  min_disk_gb   INTEGER,
  location      TEXT,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE TRIGGER trg_spec_states_no_deleted BEFORE INSERT ON spec_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'spec_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_spec_states_no_update BEFORE UPDATE ON spec_states
BEGIN
  SELECT RAISE(ABORT, 'spec_states is append-only');
END;

CREATE TRIGGER trg_spec_states_no_delete BEFORE DELETE ON spec_states
BEGIN
  SELECT RAISE(ABORT, 'spec_states is append-only');
END;

-- ═══ 4.4 KNOWN_MACHINES (host.KnownMachine) ═══
CREATE TABLE known_machine_states (
  event_id    TEXT    NOT NULL,
  entity_kind TEXT    NOT NULL DEFAULT 'known_machine' CHECK (entity_kind = 'known_machine'),
  id          TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  gpu_name    TEXT    NOT NULL,
  num_gpus    INTEGER NOT NULL,
  cpu_cores   INTEGER NOT NULL,
  ram_mb      INTEGER NOT NULL,
  disk_gb     INTEGER NOT NULL,
  location    TEXT    NOT NULL,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE TRIGGER trg_known_machine_states_no_deleted BEFORE INSERT ON known_machine_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'known_machine_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_known_machine_states_no_update BEFORE UPDATE ON known_machine_states
BEGIN
  SELECT RAISE(ABORT, 'known_machine_states is append-only');
END;

CREATE TRIGGER trg_known_machine_states_no_delete BEFORE DELETE ON known_machine_states
BEGIN
  SELECT RAISE(ABORT, 'known_machine_states is append-only');
END;

-- ═══ 4.5 SHIPS (host.Ship) ═══
CREATE TABLE ship_states (
  event_id         TEXT    NOT NULL,
  entity_kind      TEXT    NOT NULL DEFAULT 'ship' CHECK (entity_kind = 'ship'),
  id               TEXT    NOT NULL,
  version          INTEGER NOT NULL,
  order_id         TEXT    NOT NULL,
  known_machine_id TEXT    NOT NULL,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE INDEX idx_ship_states_order_id         ON ship_states (order_id);
CREATE INDEX idx_ship_states_known_machine_id ON ship_states (known_machine_id);

CREATE TRIGGER trg_ship_states_no_deleted BEFORE INSERT ON ship_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'ship_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_ship_states_no_update BEFORE UPDATE ON ship_states
BEGIN
  SELECT RAISE(ABORT, 'ship_states is append-only');
END;

CREATE TRIGGER trg_ship_states_no_delete BEFORE DELETE ON ship_states
BEGIN
  SELECT RAISE(ABORT, 'ship_states is append-only');
END;

-- ═══ 4.6 STATUSES (host.Status) ═══
-- StatusCode enum: 0=UNSPECIFIED, 1=PENDING, 2=PULLING, 3=INSTALLING, 4=RUNNING, 5=STOPPED, 6=ERROR
CREATE TABLE status_states (
  event_id    TEXT    NOT NULL,
  entity_kind TEXT    NOT NULL DEFAULT 'status' CHECK (entity_kind = 'status'),
  id          TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  ship_id     TEXT    NOT NULL,
  code        INTEGER NOT NULL CHECK (code BETWEEN 0 AND 6),
  message     TEXT    NOT NULL,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE INDEX idx_status_states_ship_id ON status_states (ship_id, code);
CREATE INDEX idx_status_states_code    ON status_states (code);

CREATE TRIGGER trg_status_states_no_deleted BEFORE INSERT ON status_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'status_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_status_states_no_update BEFORE UPDATE ON status_states
BEGIN
  SELECT RAISE(ABORT, 'status_states is append-only');
END;

CREATE TRIGGER trg_status_states_no_delete BEFORE DELETE ON status_states
BEGIN
  SELECT RAISE(ABORT, 'status_states is append-only');
END;

-- ═══ 4.7 ALARMS (host.Alarm) ═══
-- AlarmCode enum: 0=HEALTHCHECK, 1=LEASE
CREATE TABLE alarm_states (
  event_id    TEXT    NOT NULL,
  entity_kind TEXT    NOT NULL DEFAULT 'alarm' CHECK (entity_kind = 'alarm'),
  id          TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  ship_id     TEXT    NOT NULL,
  code        INTEGER NOT NULL CHECK (code IN (0, 1)),
  message     TEXT    NOT NULL,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE INDEX idx_alarm_states_ship_id ON alarm_states (ship_id, code);
CREATE INDEX idx_alarm_states_code    ON alarm_states (code);

CREATE TRIGGER trg_alarm_states_no_deleted BEFORE INSERT ON alarm_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'alarm_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_alarm_states_no_update BEFORE UPDATE ON alarm_states
BEGIN
  SELECT RAISE(ABORT, 'alarm_states is append-only');
END;

CREATE TRIGGER trg_alarm_states_no_delete BEFORE DELETE ON alarm_states
BEGIN
  SELECT RAISE(ABORT, 'alarm_states is append-only');
END;

-- ═══ 4.8 LOGS (host.Log) ═══
-- LOG_IMPORTANCE enum: 0=UNSPECIFIED, 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
CREATE TABLE log_states (
  event_id    TEXT    NOT NULL,
  entity_kind TEXT    NOT NULL DEFAULT 'log' CHECK (entity_kind = 'log'),
  id          TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  ship_id     TEXT    NOT NULL,
  importance  INTEGER NOT NULL CHECK (importance BETWEEN 0 AND 5),
  source      TEXT    NOT NULL,
  message     TEXT    NOT NULL,
  PRIMARY KEY (event_id),
  UNIQUE (id, version),
  FOREIGN KEY (event_id, entity_kind) REFERENCES entity_events (event_id, entity_kind)
);

CREATE INDEX idx_log_states_ship_filter ON log_states (ship_id, importance, source);
CREATE INDEX idx_log_states_importance  ON log_states (importance);
CREATE INDEX idx_log_states_source      ON log_states (source);

CREATE TRIGGER trg_log_states_no_deleted BEFORE INSERT ON log_states
WHEN (SELECT event_type FROM entity_events WHERE event_id = NEW.event_id) = 'deleted'
BEGIN
  SELECT RAISE(ABORT, 'log_states: a deleted event carries no state row');
END;

CREATE TRIGGER trg_log_states_no_update BEFORE UPDATE ON log_states
BEGIN
  SELECT RAISE(ABORT, 'log_states is append-only');
END;

CREATE TRIGGER trg_log_states_no_delete BEFORE DELETE ON log_states
BEGIN
  SELECT RAISE(ABORT, 'log_states is append-only');
END;

-- ─────────────────── 5. current-state & history views ─────────────────────
-- Current-state views (`orders`, `budgets`, …) resolve the latest active state
-- in O(1) per entity via `entities.last_event_id = *_states.event_id`.

-- ═══ 5.1 ORDERS ═══
CREATE VIEW orders AS
SELECT
  e.entity_id     AS id,
  s.name          AS name,
  s.budget_id     AS budget_id,
  s.spec_id       AS spec_id,
  e.version       AS version,
  e.created_time  AS created_time,
  e.updated_time  AS updated_time,
  e.last_event_id AS last_event_id
FROM entities e
JOIN order_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'order'
  AND e.is_deleted = 0;

CREATE VIEW order_events AS
SELECT
  e.event_id,
  e.entity_id AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.name      AS name,
  s.budget_id AS budget_id,
  s.spec_id   AS spec_id
FROM entity_events e
LEFT JOIN order_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'order';

-- ═══ 5.2 BUDGETS ═══
CREATE VIEW budgets AS
SELECT
  e.entity_id          AS id,
  s.total_budget       AS total_budget,
  s.max_price_per_hour AS max_price_per_hour,
  s.lease_timeout      AS lease_timeout,
  s.health_timeout     AS health_timeout,
  e.version            AS version,
  e.created_time       AS created_time,
  e.updated_time       AS updated_time,
  e.last_event_id      AS last_event_id
FROM entities e
JOIN budget_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'budget'
  AND e.is_deleted = 0;

CREATE VIEW budget_events AS
SELECT
  e.event_id,
  e.entity_id          AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.total_budget       AS total_budget,
  s.max_price_per_hour AS max_price_per_hour,
  s.lease_timeout      AS lease_timeout,
  s.health_timeout     AS health_timeout
FROM entity_events e
LEFT JOIN budget_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'budget';

-- ═══ 5.3 SPECS ═══
CREATE VIEW specs AS
SELECT
  e.entity_id     AS id,
  s.gpu_name      AS gpu_name,
  s.num_gpus      AS num_gpus,
  s.min_cpu_cores AS min_cpu_cores,
  s.min_ram_mb    AS min_ram_mb,
  s.min_disk_gb   AS min_disk_gb,
  s.location      AS location,
  e.version       AS version,
  e.created_time  AS created_time,
  e.updated_time  AS updated_time,
  e.last_event_id AS last_event_id
FROM entities e
JOIN spec_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'spec'
  AND e.is_deleted = 0;

CREATE VIEW spec_events AS
SELECT
  e.event_id,
  e.entity_id     AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.gpu_name      AS gpu_name,
  s.num_gpus      AS num_gpus,
  s.min_cpu_cores AS min_cpu_cores,
  s.min_ram_mb    AS min_ram_mb,
  s.min_disk_gb   AS min_disk_gb,
  s.location      AS location
FROM entity_events e
LEFT JOIN spec_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'spec';

-- ═══ 5.4 KNOWN_MACHINES ═══
CREATE VIEW known_machines AS
SELECT
  e.entity_id     AS id,
  s.gpu_name      AS gpu_name,
  s.num_gpus      AS num_gpus,
  s.cpu_cores     AS cpu_cores,
  s.ram_mb        AS ram_mb,
  s.disk_gb       AS disk_gb,
  s.location      AS location,
  e.version       AS version,
  e.created_time  AS created_time,
  e.updated_time  AS updated_time,
  e.last_event_id AS last_event_id
FROM entities e
JOIN known_machine_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'known_machine'
  AND e.is_deleted = 0;

CREATE VIEW known_machine_events AS
SELECT
  e.event_id,
  e.entity_id AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.gpu_name  AS gpu_name,
  s.num_gpus  AS num_gpus,
  s.cpu_cores AS cpu_cores,
  s.ram_mb    AS ram_mb,
  s.disk_gb   AS disk_gb,
  s.location  AS location
FROM entity_events e
LEFT JOIN known_machine_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'known_machine';

-- ═══ 5.5 SHIPS ═══
CREATE VIEW ships AS
SELECT
  e.entity_id        AS id,
  s.order_id         AS order_id,
  s.known_machine_id AS known_machine_id,
  e.version          AS version,
  e.created_time     AS created_time,
  e.updated_time     AS updated_time,
  e.last_event_id    AS last_event_id
FROM entities e
JOIN ship_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'ship'
  AND e.is_deleted = 0;

CREATE VIEW ship_events AS
SELECT
  e.event_id,
  e.entity_id        AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.order_id         AS order_id,
  s.known_machine_id AS known_machine_id
FROM entity_events e
LEFT JOIN ship_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'ship';

-- ═══ 5.6 STATUSES ═══
CREATE VIEW statuses AS
SELECT
  e.entity_id     AS id,
  s.ship_id       AS ship_id,
  s.code          AS code,
  s.message       AS message,
  e.version       AS version,
  e.created_time  AS created_time,
  e.updated_time  AS updated_time,
  e.last_event_id AS last_event_id
FROM entities e
JOIN status_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'status'
  AND e.is_deleted = 0;

CREATE VIEW status_events AS
SELECT
  e.event_id,
  e.entity_id AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.ship_id   AS ship_id,
  s.code      AS code,
  s.message   AS message
FROM entity_events e
LEFT JOIN status_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'status';

-- ═══ 5.7 ALARMS ═══
CREATE VIEW alarms AS
SELECT
  e.entity_id     AS id,
  s.ship_id       AS ship_id,
  s.code          AS code,
  s.message       AS message,
  e.version       AS version,
  e.created_time  AS created_time,
  e.updated_time  AS updated_time,
  e.last_event_id AS last_event_id
FROM entities e
JOIN alarm_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'alarm'
  AND e.is_deleted = 0;

CREATE VIEW alarm_events AS
SELECT
  e.event_id,
  e.entity_id AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.ship_id   AS ship_id,
  s.code      AS code,
  s.message   AS message
FROM entity_events e
LEFT JOIN alarm_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'alarm';

-- ═══ 5.8 LOGS ═══
CREATE VIEW logs AS
SELECT
  e.entity_id     AS id,
  s.ship_id       AS ship_id,
  s.importance    AS importance,
  s.source        AS source,
  s.message       AS message,
  e.version       AS version,
  e.created_time  AS created_time,
  e.updated_time  AS updated_time,
  e.last_event_id AS last_event_id
FROM entities e
JOIN log_states s ON s.event_id = e.last_event_id
WHERE e.entity_kind = 'log'
  AND e.is_deleted = 0;

CREATE VIEW log_events AS
SELECT
  e.event_id,
  e.entity_id  AS id,
  e.version,
  e.event_type,
  e.time,
  e.reason,
  s.ship_id    AS ship_id,
  s.importance AS importance,
  s.source     AS source,
  s.message    AS message
FROM entity_events e
LEFT JOIN log_states s ON s.event_id = e.event_id
WHERE e.entity_kind = 'log';
