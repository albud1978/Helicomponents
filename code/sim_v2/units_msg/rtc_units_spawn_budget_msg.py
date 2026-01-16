"""
HostFunction для расчёта spawn budget по группам (3/4).

Логика:
- Считаем total deficit = sum(max(0, 2 - slots)) по планерам в ops
- Budget = max(0, deficit - (svc_count + rsv_count))

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400
PLANER_MAX_DAYS = 4000


class SpawnBudgetHostFunction(fg.HostFunction):
    def run(self, FLAMEGPU):
        day = FLAMEGPU.getStepCounter()
        days_total = FLAMEGPU.environment.getPropertyUInt("days_total")
        if day >= days_total:
            return

        mp_budget = FLAMEGPU.environment.getMacroPropertyUInt32("mp_spawn_budget")
        mp_slots = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_slots")
        mp_ops = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_in_ops_history")
        mp_type = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_type")
        mp_svc = FLAMEGPU.environment.getMacroPropertyUInt32("mp_svc_count")
        mp_rsv = FLAMEGPU.environment.getMacroPropertyUInt32("mp_rsv_count")

        # reset
        for g in range(MAX_GROUPS):
            mp_budget[g] = 0

        # only groups 3/4
        for g, required_type in ((3, 1), (4, 2)):
            deficit = 0
            base = day * MAX_PLANERS
            for idx in range(MAX_PLANERS):
                if mp_ops[base + idx] == 0:
                    continue
                if mp_type[idx] != required_type:
                    continue
                slots = mp_slots[g * MAX_PLANERS + idx]
                if slots < 2:
                    deficit += (2 - slots)

            available = mp_svc[g] + mp_rsv[g]
            budget = deficit - available
            if budget > 0:
                mp_budget[g] = budget


def register_rtc(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_spawn_budget")
    layer.addHostFunction(SpawnBudgetHostFunction())
    print("  RTC модуль units_spawn_budget_msg зарегистрирован")
