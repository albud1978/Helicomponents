// Transitions rules graph (spec + states + rules)
MERGE (spec:TransitionSpec {id: "transitions_rules"})
SET spec.version = 8,
    spec.architecture = "LIMITER V8 (single-phase, RepairLine + Adaptive Steps)",
    spec.matrix_from = "state",
    spec.matrix_to = "state";

MERGE (s:State {id: 0}) SET s.name = "spawn (boundary)";
MERGE (s:State {id: 1}) SET s.name = "inactive";
MERGE (s:State {id: 2}) SET s.name = "operations";
MERGE (s:State {id: 3}) SET s.name = "serviceable";
MERGE (s:State {id: 4}) SET s.name = "repair";
MERGE (s:State {id: 5}) SET s.name = "reserve (unused)";
MERGE (s:State {id: 6}) SET s.name = "storage (terminal)";
MERGE (s:State {id: 7}) SET s.name = "unserviceable";

MATCH (spec:TransitionSpec {id: "transitions_rules"}), (s:State)
MERGE (spec)-[:HAS_STATE]->(s);

MERGE (r:Rule {id: "ops_to_storage_ll_v8"})
SET r.from = 2, r.to = 6, r.pre_expr = "sne_next >= ll",
    r.post_exprs = ["state == 6", "limiter == 0"],
    r.owner_module = "rtc_state_transitions_v8",
    r.notes = "V8: списание по LL (next-day dt). Приоритет 1.";
MERGE (r:Rule {id: "ops_to_storage_br_v8"})
SET r.from = 2, r.to = 6, r.pre_expr = "ppr_next >= oh AND br > 0 AND sne_next >= br",
    r.post_exprs = ["state == 6", "limiter == 0"],
    r.owner_module = "rtc_state_transitions_v8",
    r.notes = "V8: списание по BR (ремонт нерентабелен). Приоритет 2.";
MERGE (r:Rule {id: "ops_to_unsvc_v8"})
SET r.from = 2, r.to = 7, r.pre_expr = "ppr_next >= oh AND NOT (br > 0 AND sne_next >= br)",
    r.post_exprs = ["state == 7", "repair_line_id == 0xFFFFFFFF"],
    r.owner_module = "rtc_state_transitions_v8",
    r.notes = "V8: уход в unserviceable по OH; storage отфильтрован по BR.";
MERGE (r:Rule {id: "ops_hold_v8"})
SET r.from = 2, r.to = 2, r.pre_expr = "sne_next < ll AND ppr_next < oh AND limiter > 0",
    r.post_exprs = ["state == 2"],
    r.owner_module = "rtc_state_transitions_v8",
    r.notes = "V8: остаётся в operations (инкремент SNE/PPR, декремент limiter).";
MERGE (r:Rule {id: "demote_ops_to_svc_v8"})
SET r.from = 2, r.to = 3, r.pre_expr = "idx < demote_threshold",
    r.post_exprs = ["state == 3"],
    r.owner_module = "rtc_demote_ops_v7",
    r.notes = "Квоты: демоут ops → serviceable при избытке (по idx).";
MERGE (r:Rule {id: "p1_svc_to_ops_v8"})
SET r.from = 3, r.to = 2, r.pre_expr = "idx >= threshold_s3 AND quota_left > 0",
    r.post_exprs = ["state == 2"],
    r.owner_module = "rtc_svc_to_ops_v7",
    r.notes = "Квоты P1: serviceable → operations (threshold_s3).";
MERGE (r:Rule {id: "p2_unsvc_to_ops_v8"})
SET r.from = 7, r.to = 2, r.pre_expr = "repair_days == 0 AND repair_line_id == 0xFFFFFFFF AND free_days >= repair_time AND quota_left > 0",
    r.post_exprs = ["state == 2", "ppr == 0", "repair_line_id = line_id", "line.free_days = 0", "line.aircraft_number = acn"],
    r.owner_module = "rtc_promote_unsvc_v8",
    r.notes = "Квоты P2: unserviceable → operations (RepairLine).";
MERGE (r:Rule {id: "p3_inactive_to_ops_v8"})
SET r.from = 1, r.to = 2, r.pre_expr = "free_days >= repair_time AND quota_left > 0",
    r.post_exprs = ["state == 2", "repair_line_id = line_id", "line.free_days = 0"],
    r.owner_module = "rtc_promote_inactive_v8",
    r.notes = "Квоты P3: inactive → operations (RepairLine).";
MERGE (r:Rule {id: "repair_to_svc_v8"})
SET r.from = 4, r.to = 3, r.pre_expr = "current_day >= exit_date",
    r.post_exprs = ["state == 3", "repair_days == 0"],
    r.owner_module = "rtc_repair_to_svc_v7",
    r.notes = "Детерминированный выход из ремонта по exit_date.";
MERGE (r:Rule {id: "repair_hold_v8"})
SET r.from = 4, r.to = 4, r.pre_expr = "current_day < exit_date",
    r.post_exprs = ["state == 4"],
    r.owner_module = "rtc_repair_to_svc_v7",
    r.notes = "Остаётся в ремонте до exit_date.";
MERGE (r:Rule {id: "serviceable_hold_v8"})
SET r.from = 3, r.to = 3, r.pre_expr = "true",
    r.post_exprs = ["state == 3"],
    r.owner_module = "rtc_states_stub",
    r.notes = "Holding в serviceable.";
MERGE (r:Rule {id: "unserviceable_hold_v8"})
SET r.from = 7, r.to = 7, r.pre_expr = "true",
    r.post_exprs = ["state == 7"],
    r.owner_module = "rtc_states_stub",
    r.notes = "Holding в unserviceable (ожидание RepairLine).";

MATCH (spec:TransitionSpec {id: "transitions_rules"}), (r:Rule)
MERGE (spec)-[:HAS_RULE]->(r);

MATCH (r:Rule), (from:State), (to:State)
WHERE r.from = from.id AND r.to = to.id
MERGE (r)-[:FROM_STATE]->(from)
MERGE (r)-[:TO_STATE]->(to);
