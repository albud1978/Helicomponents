"""
RTC модуль для operations: инкремент dt и установка intent_state.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000
PLANER_MAX_DAYS = 4000


def get_rtc_code() -> str:
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_ops_step, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int day = FLAMEGPU->getStepCounter();
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");

    unsigned int dt = 0u;
    if (aircraft_number > 0u && day < FLAMEGPU->environment.getProperty<unsigned int>("days_total")) {{
        auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
        auto mp_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_PLANERS}u * ({PLANER_MAX_DAYS}u + 1u)>("mp_planer_dt");
        const unsigned int idx = mp_ac_to_idx[aircraft_number];
        if (idx > 0u && idx < {MAX_PLANERS}u) {{
            const unsigned int pos = day * {MAX_PLANERS}u + idx;
            dt = mp_dt[pos];
        }}
    }}

    sne += dt;
    ppr += dt;
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);

    // Переходы как у планеров
    if (ll > 0u && sne >= ll) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    }} else if (oh > 0u && ppr >= oh && br > 0u && sne >= br) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    }} else if (oh > 0u && ppr >= oh && (br == 0u || sne < br)) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    }} else if (aircraft_number == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    }}

    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_units_ops_step", rtc_code)
    fn.setInitialState("operations")
    fn.setEndState("operations")

    layer = model.newLayer("layer_units_msg_ops")
    layer.addAgentFunction(fn)

    print("  RTC модуль units_ops_msg зарегистрирован")
