"""
RTC слой сохранения pre_state_id (L2 engines).

Ставит pre_state_id в текущий state до любых переходов.
Состояние агента не меняется.

Дата: 27.02.2026
"""

import pyflamegpu as fg


def _get_rtc_code(state_id: int) -> str:
    return f"""
// pre_state snapshot для state={state_id}
FLAMEGPU_AGENT_FUNCTION(rtc_units_save_pre_state_{state_id}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    FLAMEGPU->setVariable<unsigned int>("pre_state_id", {state_id}u);
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    states = {
        "operations": 2,
        "serviceable": 3,
        "repair": 4,
        "reserve": 5,
        "storage": 6,
        "unserviceable": 7,
    }

    layer = model.newLayer("layer_units_save_pre_state")

    for state_name, state_id in states.items():
        fn_name = f"rtc_units_save_pre_state_{state_id}"
        rtc_code = _get_rtc_code(state_id)
        fn = agent.newRTCFunction(fn_name, rtc_code)
        fn.setInitialState(state_name)
        fn.setEndState(state_name)
        layer.addAgentFunction(fn)

    print("  RTC модуль units_save_pre_state_v1 зарегистрирован (1 слой)")
