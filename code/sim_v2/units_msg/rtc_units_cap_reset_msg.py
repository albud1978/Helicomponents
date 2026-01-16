"""
HostFunction: сброс mp_planer_cap каждый шаг.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400


class CapResetHostFunction(fg.HostFunction):
    def run(self, FLAMEGPU):
        mp_cap = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_cap")
        size = MAX_GROUPS * MAX_PLANERS
        for i in range(size):
            mp_cap[i] = 0


def register_rtc(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_cap_reset")
    layer.addHostFunction(CapResetHostFunction())
    print("  RTC модуль units_cap_reset_msg зарегистрирован")
