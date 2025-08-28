FLAMEGPU_AGENT_FUNCTION(rtc_quota_init, flamegpu::MessageNone, flamegpu::MessageNone) {
    FLAMEGPU->setVariable<unsigned int>("ops_ticket", 0u);
    if (FLAMEGPU->getVariable<unsigned int>("idx") == 0u) {
        unsigned int day = FLAMEGPU->getStepCounter();
        unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        unsigned int dayp1 = (day + 1u < days_total ? day + 1u : day);
        unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
        unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
        {
            auto q8s  = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
            auto q17s = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi17");
            q8s.exchange(seed8);
            q17s.exchange(seed17);
        }
    }
    return flamegpu::ALIVE;
}


