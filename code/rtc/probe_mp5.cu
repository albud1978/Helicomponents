FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {
    unsigned int day = FLAMEGPU->getStepCounter();
    unsigned int N   = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    unsigned int i   = FLAMEGPU->getVariable<unsigned int>("idx");
    unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    unsigned int dayp1 = (day + 1u < days_total ? day + 1u : day);
    unsigned int linT = day * N + i;
    unsigned int linN = dayp1 * N + i;
    unsigned int dt = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", linT);
    unsigned int dn = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", linN);
    if (FLAMEGPU->getVariable<unsigned int>("daily_today_u32") == 0u)
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    if (FLAMEGPU->getVariable<unsigned int>("daily_next_u32") == 0u)
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    return flamegpu::ALIVE;
}


