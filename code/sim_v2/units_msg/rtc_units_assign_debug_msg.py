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
        ops_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_ops_flag")
        type_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_type_flag")
        need_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_need_flag")
        slot_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_slot_flag")
        ac_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_ac_flag")
        any_entry = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_any_entry")
        any_after = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_any_after")
        for i in range(MAX_GROUPS):
            hits[i] = 0
            attempts[i] = 0
            called[i] = 0
            skip_repair[i] = 0
            loop_flag[i] = 0
            hit_flag[i] = 0
            ops_flag[i] = 0
            type_flag[i] = 0
            need_flag[i] = 0
            slot_flag[i] = 0
            ac_flag[i] = 0
        any_entry[0] = 0
        any_after[0] = 0


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
        ops_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_ops_flag")
        type_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_type_flag")
        need_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_need_flag")
        slot_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_slot_flag")
        ac_flag = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_ac_flag")
        any_entry = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_any_entry")
        any_after = FLAMEGPU.environment.getMacroPropertyUInt32("mp_assign_any_after")
        print(f"   assign_hits: day={day} g3={int(hits[3])} g4={int(hits[4])}")
        print(f"   assign_attempts: day={day} g3={int(attempts[3])} g4={int(attempts[4])}")
        print(f"   assign_called: day={day} g3={int(called[3])} g4={int(called[4])}")
        print(f"   assign_skip_repair: day={day} g3={int(skip_repair[3])} g4={int(skip_repair[4])}")
        print(f"   assign_loop_flag: day={day} g3={int(loop_flag[3])} g4={int(loop_flag[4])}")
        print(f"   assign_hit_flag: day={day} g3={int(hit_flag[3])} g4={int(hit_flag[4])}")
        print(f"   assign_ops_flag: day={day} g3={int(ops_flag[3])} g4={int(ops_flag[4])}")
        print(f"   assign_type_flag: day={day} g3={int(type_flag[3])} g4={int(type_flag[4])}")
        print(f"   assign_need_flag: day={day} g3={int(need_flag[3])} g4={int(need_flag[4])}")
        print(f"   assign_slot_flag: day={day} g3={int(slot_flag[3])} g4={int(slot_flag[4])}")
        print(f"   assign_ac_flag: day={day} g3={int(ac_flag[3])} g4={int(ac_flag[4])}")
        print(f"   assign_any_entry: day={day} v={int(any_entry[0])}")
        print(f"   assign_any_after: day={day} v={int(any_after[0])}")


def register_reset(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_assign_reset")
    layer.addHostFunction(ResetAssignHits())
    print("  RTC модуль units_assign_debug_msg (reset) зарегистрирован")


def register_report(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_assign_report")
    layer.addHostFunction(ReportAssignHits())
    print("  RTC модуль units_assign_debug_msg (report) зарегистрирован")
