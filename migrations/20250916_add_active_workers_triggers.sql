-- Migration: add active_workers columns + triggers to maintain them from workers_seen

PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;

-- 1) Add columns if not present
ALTER TABLE user_stats RENAME TO _tmp_user_stats;

CREATE TABLE IF NOT EXISTS user_stats (
    address TEXT PRIMARY KEY,
    ts INTEGER,
    hashrate1m TEXT,
    hashrate5m TEXT,
    hashrate1hr TEXT,
    hashrate1d TEXT,
    hashrate7d TEXT,
    lastshare INTEGER,
    workers INTEGER,
    shares INTEGER,
    bestshare REAL,
    bestever REAL,
    authorised INTEGER,
    active_workers TEXT,         -- JSON array string, e.g. '["rig1","rig2"]'
    active_workers_count INTEGER DEFAULT 0
);

-- Copy back existing rows (preserve everything if existed)
INSERT OR REPLACE INTO user_stats(address, ts, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare, workers, shares, bestshare, bestever, authorised)
SELECT address, ts, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare, workers, shares, bestshare, bestever, authorised
FROM _tmp_user_stats;

DROP TABLE _tmp_user_stats;

-- 2) Helper: a deterministic way to build a JSON array string of active workers for a wallet.
-- We use group_concat to collect worker names separated by '||' then format into JSON.
CREATE TEMPORARY VIEW IF NOT EXISTS vw_active_workers AS
SELECT wallet AS wallet_key,
       group_concat(worker, '||') AS joined_workers
FROM workers_seen
WHERE active=1
GROUP BY wallet;

-- 3) Trigger AFTER INSERT on workers_seen -> mark active set for wallet
CREATE TRIGGER IF NOT EXISTS tr_workers_seen_after_insert
AFTER INSERT ON workers_seen
BEGIN
  -- rebuild JSON array from workers_seen for this wallet
  INSERT INTO user_stats(address, ts, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare, workers, shares, bestshare, bestever, authorised, active_workers, active_workers_count)
  SELECT NEW.wallet, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         CASE WHEN vw.joined_workers IS NULL THEN '[]' ELSE '["' || replace(vw.joined_workers,'||','","') || '"]' END,
         CASE WHEN vw.joined_workers IS NULL THEN 0 ELSE (length(vw.joined_workers) - length(replace(vw.joined_workers,'||','')))+1 END
  FROM vw_active_workers vw WHERE vw.wallet_key = NEW.wallet
  ON CONFLICT(address) DO UPDATE SET
     active_workers = (CASE WHEN (SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet) IS NULL THEN '[]' ELSE '["' || replace((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet),'||','","') || '"]' END),
     active_workers_count = (CASE WHEN (SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet) IS NULL THEN 0 ELSE (length((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet)) - length(replace((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet),'||','')))+1 END);
END;

-- 4) Trigger AFTER UPDATE on workers_seen (only when active or last_seen changes)
CREATE TRIGGER IF NOT EXISTS tr_workers_seen_after_update
AFTER UPDATE OF active, last_seen ON workers_seen
BEGIN
  -- rebuild JSON array for this wallet
  INSERT INTO user_stats(address, ts, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare, workers, shares, bestshare, bestever, authorised, active_workers, active_workers_count)
  SELECT NEW.wallet, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         CASE WHEN vw.joined_workers IS NULL THEN '[]' ELSE '["' || replace(vw.joined_workers,'||','","') || '"]' END,
         CASE WHEN vw.joined_workers IS NULL THEN 0 ELSE (length(vw.joined_workers) - length(replace(vw.joined_workers,'||','')))+1 END
  FROM vw_active_workers vw WHERE vw.wallet_key = NEW.wallet
  ON CONFLICT(address) DO UPDATE SET
     active_workers = (CASE WHEN (SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet) IS NULL THEN '[]' ELSE '["' || replace((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet),'||','","') || '"]' END),
     active_workers_count = (CASE WHEN (SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet) IS NULL THEN 0 ELSE (length((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet)) - length(replace((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = NEW.wallet),'||','')))+1 END);
END;

-- 5) Trigger AFTER DELETE on workers_seen
CREATE TRIGGER IF NOT EXISTS tr_workers_seen_after_delete
AFTER DELETE ON workers_seen
BEGIN
  -- rebuild JSON array for this wallet (use OLD.wallet)
  INSERT INTO user_stats(address, ts, hashrate1m, hashrate5m, hashrate1hr, hashrate1d, hashrate7d, lastshare, workers, shares, bestshare, bestever, authorised, active_workers, active_workers_count)
  SELECT OLD.wallet, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
         CASE WHEN vw.joined_workers IS NULL THEN '[]' ELSE '["' || replace(vw.joined_workers,'||','","') || '"]' END,
         CASE WHEN vw.joined_workers IS NULL THEN 0 ELSE (length(vw.joined_workers) - length(replace(vw.joined_workers,'||','')))+1 END
  FROM vw_active_workers vw WHERE vw.wallet_key = OLD.wallet
  ON CONFLICT(address) DO UPDATE SET
     active_workers = (CASE WHEN (SELECT joined_workers FROM vw_active_workers WHERE wallet_key = OLD.wallet) IS NULL THEN '[]' ELSE '["' || replace((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = OLD.wallet),'||','","') || '"]' END),
     active_workers_count = (CASE WHEN (SELECT joined_workers FROM vw_active_workers WHERE wallet_key = OLD.wallet) IS NULL THEN 0 ELSE (length((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = OLD.wallet)) - length(replace((SELECT joined_workers FROM vw_active_workers WHERE wallet_key = OLD.wallet),'||','')))+1 END);
END;

COMMIT;
PRAGMA foreign_keys=ON;
