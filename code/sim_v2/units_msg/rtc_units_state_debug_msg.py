"""
RTC debug: считает агентов в reserve по группам.
"""
import pyflamegpu as fg

MAX_GROUPS = 50


def get_rtc_code() -> str:
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_mark_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    if (group_by < {MAX_GROUPS}u) {{
        auto mp_seen = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_reserve_seen");
        mp_seen[group_by] += 1u;
        auto mp_hits = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_assign_hits");
        mp_hits[group_by] += 1u;
    }}
    return flamegpu::ALIVE;
}}
"""


class ResetReserveSeen(fg.HostFunction):
    def run(self, FLAMEGPU):
        seen = FLAMEGPU.environment.getMacroPropertyUInt32("mp_reserve_seen")
        for i in range(MAX_GROUPS):
            seen[i] = 0


class ReportReserveSeen(fg.HostFunction):
    def run(self, FLAMEGPU):
        day = FLAMEGPU.getStepCounter()
        if day not in (3000, 3649):
            return
        seen = FLAMEGPU.environment.getMacroPropertyUInt32("mp_reserve_seen")
        print(f"   reserve_seen: day={day} g3={int(seen[3])} g4={int(seen[4])}")
        try:
            cnt_g3 = FLAMEGPU.environment.getMacroPropertyUInt32("mp_ops_count_g3")
            cnt_g4 = FLAMEGPU.environment.getMacroPropertyUInt32("mp_ops_count_g4")
            print(f"   ops_count: day={day} g3={int(cnt_g3[day])} g4={int(cnt_g4[day])}")
            base = day * 400
            list_g4 = FLAMEGPU.environment.getMacroPropertyUInt32("mp_ops_list_g4")
            if int(cnt_g4[day]) > 0:
                print(f"   ops_list_g4[0]={int(list_g4[base])}")
        except Exception as e:
            print(f"   ops_count debug error: {e}")


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_mark_reserve", rtc_code)
    fn.setInitialState("reserve")
    fn.setEndState("reserve")

    reset_layer = model.newLayer("layer_units_msg_reserve_seen_reset")
    reset_layer.addHostFunction(ResetReserveSeen())

    layer = model.newLayer("layer_units_msg_reserve_seen")
    layer.addAgentFunction(fn)

    report_layer = model.newLayer("layer_units_msg_reserve_seen_report")
    report_layer.addHostFunction(ReportReserveSeen())

    print("  RTC модуль units_state_debug_msg зарегистрирован")
