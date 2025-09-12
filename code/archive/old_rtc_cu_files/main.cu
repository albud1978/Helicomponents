FLAMEGPU_AGENT_FUNCTION(rtc_main, flamegpu::MessageNone, flamegpu::MessageNone) {
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
    unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    FLAMEGPU->setVariable<unsigned int>("sne", sne + dt);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr + dt);
    return flamegpu::ALIVE;
}


