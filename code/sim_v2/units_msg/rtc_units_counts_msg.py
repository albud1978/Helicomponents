"""
RTC модуль подсчёта serviceable/reserve для приоритета назначения.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_GROUPS = 50


def get_rtc_code() -> str:
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_count_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    if (active == 1u && group_by < {MAX_GROUPS}u) {{
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_svc_count");
        svc_count[group_by] += 1u;
    }}
    return flamegpu::ALIVE;
}}

FLAMEGPU_AGENT_FUNCTION(rtc_units_count_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int active = FLAMEGPU->getVariable<unsigned int>("active");
    if (active == 1u && group_by < {MAX_GROUPS}u) {{
        auto rsv_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_count");
        rsv_count[group_by] += 1u;
    }}
    return flamegpu::ALIVE;
}}
"""


class ResetCountsHostFunction(fg.HostFunction):
    def run(self, FLAMEGPU):
        svc = FLAMEGPU.environment.getMacroPropertyUInt32("mp_svc_count")
        rsv = FLAMEGPU.environment.getMacroPropertyUInt32("mp_rsv_count")
        for i in range(MAX_GROUPS):
            svc[i] = 0
            rsv[i] = 0


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()

    fn_svc = agent.newRTCFunction("rtc_units_count_serviceable", rtc_code)
    fn_svc.setInitialState("serviceable")
    fn_svc.setEndState("serviceable")

    fn_rsv = agent.newRTCFunction("rtc_units_count_reserve", rtc_code)
    fn_rsv.setInitialState("reserve")
    fn_rsv.setEndState("reserve")

    reset_layer = model.newLayer("layer_units_msg_reset_counts")
    reset_layer.addHostFunction(ResetCountsHostFunction())

    count_layer = model.newLayer("layer_units_msg_count")
    count_layer.addAgentFunction(fn_svc)
    count_layer.addAgentFunction(fn_rsv)

    print("  RTC модуль units_counts_msg зарегистрирован")
