ALTER TABLE heli_pandas
ADD COLUMN IF NOT EXISTS status_change UInt8 DEFAULT 0

ALTER TABLE heli_pandas
UPDATE status_change = 0
WHERE status_change != 0
SETTINGS allow_experimental_alter_update = 1

-- rtc_ops_check для group_by=1
WITH 
  D AS (
    SELECT min(dates) FROM flight_program_fl 
    WHERE (version_date, version_id) = (
      SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1
    )
  ),
  daily_today AS (
    SELECT aircraft_number, daily_hours 
    FROM flight_program_fl WHERE dates = (SELECT D) 
  ),
  daily_next AS (
    SELECT aircraft_number, daily_hours 
    FROM flight_program_fl WHERE dates = addDays((SELECT D), 1)
  ),
  mp1 AS (
    SELECT partno_comp, br, repair_time FROM md_components
  )
ALTER TABLE heli_pandas
UPDATE status_change = multiIf(
    -- LL: хватит на сегодня, не хватит на завтра → 6
    (ll - sne) >= dt.daily_hours AND (ll - sne) < (dt.daily_hours + coalesce(dn.daily_hours, 0)), 6,
    -- OH+BR: хватит на сегодня, не хватит на завтра и неремонтопригоден → 6
    (oh - ppr) >= dt.daily_hours AND (oh - ppr) < (dt.daily_hours + coalesce(dn.daily_hours, 0)) AND (sne + dt.daily_hours) >= coalesce(m1.br, 4294967295), 6,
    -- OH: хватит на сегодня, не хватит на завтра и ремонтопригоден → 4
    (oh - ppr) >= dt.daily_hours AND (oh - ppr) < (dt.daily_hours + coalesce(dn.daily_hours, 0)) AND (sne + dt.daily_hours) < coalesce(m1.br, 4294967295), 4,
    0
)
WHERE status_id = 2 AND status_change = 0 AND group_by = 1
  AND aircraft_number IN (SELECT aircraft_number FROM flight_program_fl)
  SETTINGS allow_experimental_alter_update = 1
AS hp
JOIN daily_today dt ON hp.aircraft_number = dt.aircraft_number
LEFT JOIN daily_next dn ON hp.aircraft_number = dn.aircraft_number
LEFT JOIN mp1 m1 ON m1.partno_comp = hp.partseqno_i

-- Диагностика host-триггера для group_by=1
WITH 
  D AS (
    SELECT min(dates) FROM flight_program_fl 
    WHERE (version_date, version_id) = (
      SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1
    )
  )
SELECT
  (SELECT count() FROM heli_pandas WHERE status_id=2 AND status_change=0 AND group_by=1) AS current_ops,
  (SELECT sum(ops_counter_mi8) FROM flight_program_ac WHERE dates=(SELECT D)) AS target_ops,
  (target_ops - current_ops) AS trigger_pr_final;

-- rtc_ops_check для group_by=2
WITH 
  D AS (
    SELECT min(dates) FROM flight_program_fl 
    WHERE (version_date, version_id) = (
      SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1
    )
  ),
  daily_today AS (
    SELECT aircraft_number, daily_hours 
    FROM flight_program_fl WHERE dates = (SELECT D) 
  ),
  daily_next AS (
    SELECT aircraft_number, daily_hours 
    FROM flight_program_fl WHERE dates = addDays((SELECT D), 1)
  ),
  mp1 AS (
    SELECT partno_comp, br, repair_time FROM md_components
  )
ALTER TABLE heli_pandas
UPDATE status_change = multiIf(
    -- LL: хватит на сегодня, не хватит на завтра → 6
    (ll - sne) >= dt.daily_hours AND (ll - sne) < (dt.daily_hours + coalesce(dn.daily_hours, 0)), 6,
    -- OH+BR: хватит на сегодня, не хватит на завтра и неремонтопригоден → 6
    (oh - ppr) >= dt.daily_hours AND (oh - ppr) < (dt.daily_hours + coalesce(dn.daily_hours, 0)) AND (sne + dt.daily_hours) >= coalesce(m1.br, 4294967295), 6,
    -- OH: хватит на сегодня, не хватит на завтра и ремонтопригоден → 4
    (oh - ppr) >= dt.daily_hours AND (oh - ppr) < (dt.daily_hours + coalesce(dn.daily_hours, 0)) AND (sne + dt.daily_hours) < coalesce(m1.br, 4294967295), 4,
    0
)
WHERE status_id = 2 AND status_change = 0 AND group_by = 2
  AND aircraft_number IN (SELECT aircraft_number FROM flight_program_fl)
  SETTINGS allow_experimental_alter_update = 1
AS hp
JOIN daily_today dt ON hp.aircraft_number = dt.aircraft_number
LEFT JOIN daily_next dn ON hp.aircraft_number = dn.aircraft_number
LEFT JOIN mp1 m1 ON m1.partno_comp = hp.partseqno_i

-- Диагностика host-триггера для group_by=2
WITH 
  D AS (
    SELECT min(dates) FROM flight_program_fl 
    WHERE (version_date, version_id) = (
      SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1
    )
  )
SELECT
  (SELECT count() FROM heli_pandas WHERE status_id=2 AND status_change=0 AND group_by=2) AS current_ops,
  (SELECT sum(ops_counter_mi17) FROM flight_program_ac WHERE dates=(SELECT D)) AS target_ops,
  (target_ops - current_ops) AS trigger_pr_final;
