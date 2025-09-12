FLAMEGPU_AGENT_FUNCTION(rtc_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
    unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
    if (status_id == 4u) {
        unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
        FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
        unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
        if (rd >= rt) {
            FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
            FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        }
    }
    return flamegpu::ALIVE;
}


