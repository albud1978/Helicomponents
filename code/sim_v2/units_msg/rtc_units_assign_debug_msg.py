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
        skip_repair = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_skip_repair")
        loop_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_loop_flag")
        hit_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_hit_flag")
        for i in range(MAX_GROUPS):
            hits[i] = 0
            attempts[i] = 0
            called[i] = 0
            skip_repair[i] = 0
            loop_flag[i] = 0
            hit_flag[i] = 0


class ReportAssignHits(fg.HostFunction):
    def run(self, FLAMEGPU):
        day = FLAMEGPU.getStepCounter()
        if day not in (3000, 3649):
            return
        hits = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_hits")
        attempts = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_attempts")
        called = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_called")
        skip_repair = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_skip_repair")
        loop_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_loop_flag")
        hit_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_hit_flag")
        print(f"   assign_hits: day={day} g3={int(hits[3])} g4={int(hits[4])}")
        print(f"   assign_attempts: day={day} g3={int(attempts[3])} g4={int(attempts[4])}")
        print(f"   assign_called: day={day} g3={int(called[3])} g4={int(called[4])}")
        print(f"   assign_skip_repair: day={day} g3={int(skip_repair[3])} g4={int(skip_repair[4])}")
        print(f"   assign_loop_flag: day={day} g3={int(loop_flag[3])} g4={int(loop_flag[4])}")
        print(f"   assign_hit_flag: day={day} g3={int(hit_flag[3])} g4={int(hit_flag[4])}")


def register_reset(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_assign_reset")
    layer.addHostFunction(ResetAssignHits())
    print("  RTC модуль units_assign_debug_msg (reset) зарегистрирован")


def register_report(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_assign_report")
    layer.addHostFunction(ReportAssignHits())
    print("  RTC модуль units_assign_debug_msg (report) зарегистрирован")
