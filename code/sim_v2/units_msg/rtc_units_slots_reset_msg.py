"""
HostFunction: обнуление mp_planer_slots каждый шаг.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50
MAX_PLANERS = 400


class SlotsResetHostFunction(fg.HostFunction):
    def run(self, FLAMEGPU):
        mp_slots = FLAMEGPU.environment.getMacroPropertyUInt32("mp_planer_slots")
        size = MAX_GROUPS * MAX_PLANERS
        for i in range(size):
            mp_slots[i] = 0


def register_rtc(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_slots_reset")
    layer.addHostFunction(SlotsResetHostFunction())
    print("  RTC модуль units_slots_reset_msg зарегистрирован")
