"""
Debug: считает назначения (assign) по группам.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50


class ResetAssignHits(fg.HostFunction):
    def run(self, FLAMEGPU):
        hits = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_hits")
        attempts = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_attempts")
        called = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_called")
        for i in range(MAX_GROUPS):
            hits[i] = 0
            attempts[i] = 0
            called[i] = 0


class ReportAssignHits(fg.HostFunction):
    def run(self, FLAMEGPU):
        day = FLAMEGPU.getStepCounter()
        if day not in (3000, 3649):
            return
        hits = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_hits")
        attempts = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_attempts")
        called = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_called")
        print(f"   assign_hits: day={day} g3={int(hits[3])} g4={int(hits[4])}")
        print(f"   assign_attempts: day={day} g3={int(attempts[3])} g4={int(attempts[4])}")
        print(f"   assign_called: day={day} g3={int(called[3])} g4={int(called[4])}")


def register_reset(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_assign_reset")
    layer.addHostFunction(ResetAssignHits())
    print("  RTC модуль units_assign_debug_msg (reset) зарегистрирован")


def register_report(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_assign_report")
    layer.addHostFunction(ReportAssignHits())
    print("  RTC модуль units_assign_debug_msg (report) зарегистрирован")
