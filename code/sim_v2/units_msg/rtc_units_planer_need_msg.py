"""
HostFunction: вычисление mp_planer_need по текущим slots и in_ops.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400


class PlanerNeedHostFunction(fg.HostFunction):
    def run(self, FLAMEGPU):
        day = FLAMEGPU.getStepCounter()
        days_total = FLAMEGPU.environment.getPropertyUInt("days_total")
        if day >= days_total:
            return

        mp_need = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_need")
        mp_slots = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_slots")
        mp_ops = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_in_ops_history")
        mp_type = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_type")

        # reset
        size = MAX_GROUPS * MAX_PLANERS
        for i in range(size):
            mp_need[i] = 0

        base = day * MAX_PLANERS
        for g, required_type in ((3, 1), (4, 2)):
            offset = g * MAX_PLANERS
            for idx in range(MAX_PLANERS):
                if mp_ops[base + idx] == 0:
                    continue
                if mp_type[idx] != required_type:
                    continue
                slots = mp_slots[offset + idx]
                if slots < 2:
                    mp_need[offset + idx] = 2 - slots


def register_rtc(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_planer_need")
    layer.addHostFunction(PlanerNeedHostFunction())
    print("  RTC модуль units_planer_need_msg зарегистрирован")
