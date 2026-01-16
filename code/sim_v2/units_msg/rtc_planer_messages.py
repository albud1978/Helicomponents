"""
RTC модуль публикации сообщений планерами.

PlanerAgent читает mp_planer_dt и mp_planer_in_ops_history
и публикует PlanerMessage для агрегатов.

Дата: 15.01.2026
"""
import pyflamegpu as fg

MAX_PLANERS = 400
PLANER_MAX_DAYS = 4000


def get_rtc_code() -> str:
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_planer_send_message, flamegpu::MessageNone, flamegpu::MessageBruteForce) {{
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int planer_idx = FLAMEGPU->getVariable<unsigned int>("planer_idx");
    const unsigned int planer_type = FLAMEGPU->getVariable<unsigned int>("planer_type");
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");

    unsigned int dt = 0u;
    unsigned int in_ops = 0u;

    if (day < days_total && planer_idx < {MAX_PLANERS}u) {{
        const unsigned int pos = day * {MAX_PLANERS}u + planer_idx;
        auto mp_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_PLANERS}u * ({PLANER_MAX_DAYS}u + 1u)>("mp_planer_dt");
        auto mp_ops = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_PLANERS}u * ({PLANER_MAX_DAYS}u + 1u)>("mp_planer_in_ops_history");
        dt = mp_dt[pos];
        in_ops = mp_ops[pos];
    }}

    FLAMEGPU->setVariable<unsigned int>("dt", dt);
    FLAMEGPU->setVariable<unsigned int>("in_ops", in_ops);

    FLAMEGPU->message_out.setVariable<unsigned int>("aircraft_number", aircraft_number);
    FLAMEGPU->message_out.setVariable<unsigned int>("planer_idx", planer_idx);
    FLAMEGPU->message_out.setVariable<unsigned int>("dt", dt);
    FLAMEGPU->message_out.setVariable<unsigned int>("planer_type", planer_type);
    FLAMEGPU->message_out.setVariable<unsigned int>("in_ops", in_ops);

    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    rtc_code = get_rtc_code()
    fn = agent.newRTCFunction("rtc_planer_send_message", rtc_code)
    fn.setInitialState("planer")
    fn.setEndState("planer")
    fn.setMessageOutput("planer_message")

    layer = model.newLayer("layer_planer_messages")
    layer.addAgentFunction(fn)

    print("  RTC модуль planer_messages зарегистрирован")
