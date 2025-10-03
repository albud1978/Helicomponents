#!/usr/bin/env python3
"""
RTC спавн Ми‑17: менеджер и тикеты. Последние слои шага (после логгера/менеджера состояний).
"""

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


def register_rtc(model: 'fg.ModelDescription', agent: 'fg.AgentDescription'):
    frames_var = model.Environment().getPropertyUInt("frames_total")

    # Агенты-утилиты спавна
    spawn_mgr = model.newAgent("spawn_mgr")
    spawn_mgr.newVariableUInt("next_idx", 0)
    spawn_mgr.newVariableUInt("next_acn", 0)
    spawn_mgr.newVariableUInt("next_psn", 0)

    spawn_ticket = model.newAgent("spawn_ticket")
    spawn_ticket.newVariableUInt("ticket", 0)

    # MacroProperty для обмена (скаляры)
    env = model.Environment()
    env.newMacroPropertyUInt32("spawn_need_u32", 1)
    env.newMacroPropertyUInt32("spawn_base_idx_u32", 1)
    env.newMacroPropertyUInt32("spawn_base_acn_u32", 1)
    env.newMacroPropertyUInt32("spawn_base_psn_u32", 1)

    MAX_FRAMES = model.Environment().getPropertyUInt("frames_total")

    RTC_SPAWN_MGR = """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mgr, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        // Чтение планов будет через getProperty(index), чтобы избежать PropertyArray API

        // Читаем курсоры
        unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
        unsigned int next_acn = FLAMEGPU->getVariable<unsigned int>("next_acn");
        unsigned int next_psn = FLAMEGPU->getVariable<unsigned int>("next_psn");

        unsigned int need = 0u;
        if (day < days_total) {
            need = FLAMEGPU->environment.getProperty<unsigned int>("mp4_new_counter_mi17_seed", day);
        }

        // Клип по доступному хвосту индексов
        if (next_idx >= frames_total) need = 0u;
        unsigned int capacity = frames_total - next_idx;
        if (need > capacity) need = capacity;

        // Публикуем в MacroProperty скаляры
        auto need_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_need_u32");
        auto base_idx_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_idx_u32");
        auto base_acn_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_acn_u32");
        auto base_psn_out = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_psn_u32");

        need_out[0].exchange(need);
        base_idx_out[0].exchange(next_idx);
        base_acn_out[0].exchange(next_acn);
        base_psn_out[0].exchange(next_psn);

        // Сдвигаем курсоры для следующего дня
        FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx + need);
        FLAMEGPU->setVariable<unsigned int>("next_acn", next_acn + need);
        FLAMEGPU->setVariable<unsigned int>("next_psn", next_psn + need);

        return flamegpu::ALIVE;
    }
    """

    RTC_SPAWN_TICKET = """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mi17_atomic, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");

        auto need_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_need_u32");
        auto base_idx_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_idx_u32");
        auto base_acn_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_acn_u32");
        auto base_psn_in = FLAMEGPU->environment.getMacroProperty<unsigned int, 1>("spawn_base_psn_u32");

        const unsigned int need = need_in[0];
        const unsigned int base_idx = base_idx_in[0];
        const unsigned int base_acn = base_acn_in[0];
        const unsigned int base_psn = base_psn_in[0];

        const unsigned int t = FLAMEGPU->getVariable<unsigned int>("ticket");
        if (t >= need) return flamegpu::ALIVE;

        // Создаём нового агента (Mi-17) через agent_out
        const unsigned int idx = base_idx + t;
        if (idx >= frames_total) return flamegpu::ALIVE;

        FLAMEGPU->agent_out.setVariable<unsigned int>("idx", idx);
        FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", base_acn + t);
        FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 70482u); // Mi-17 partseqno
        FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);        // Mi-17

        // Нормативы для новорождённых из Env констант Mi-17
        const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
        const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
        const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
        FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
        FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
        FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);

        // Начальные наработки и статусные поля
        FLAMEGPU->agent_out.setVariable<unsigned int>("status_id", 2u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("sne", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("s6_started", 0u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 2u);
        FLAMEGPU->agent_out.setVariable<unsigned int>("prev_intent_state", 2u);

        // Дата производства: первый день месяца текущего day
        unsigned int mfg = 0u;
        if (day < days_total) {
            mfg = FLAMEGPU->environment.getProperty<unsigned int>("month_first_u32", day);
        }
        FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", mfg);

        // Времена сборки/ремонта для группы 17
        FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const"));
        FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const"));

        return flamegpu::ALIVE;
    }
    """

    fn_mgr = spawn_mgr.newRTCFunction("rtc_spawn_mgr", RTC_SPAWN_MGR)
    fn_mgr.setInitialState("default")
    fn_mgr.setEndState("default")

    fn_ticket = spawn_ticket.newRTCFunction("rtc_spawn_mi17_atomic", RTC_SPAWN_TICKET)
    # Важно: вывод новых агентов направляем в основного агента 'heli' в состояние 'operations'
    fn_ticket.setAgentOutput(agent, "operations")
    fn_ticket.setInitialState("default")
    fn_ticket.setEndState("default")

    # Слои: менеджер, потом тикеты (добавляем в конец пайплайна пользователя)
    layer_mgr = model.newLayer("spawn_mgr")
    layer_mgr.addAgentFunction(fn_mgr)
    layer_spawn = model.newLayer("spawn_ticket")
    layer_spawn.addAgentFunction(fn_ticket)


