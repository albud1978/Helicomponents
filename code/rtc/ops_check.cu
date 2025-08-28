FLAMEGPU_AGENT_FUNCTION(rtc_ops_check, flamegpu::MessageNone, flamegpu::MessageNone) {
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
    unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
    unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    unsigned int ll  = FLAMEGPU->getVariable<unsigned int>("ll");
    unsigned int oh  = FLAMEGPU->getVariable<unsigned int>("oh");
    unsigned int br  = FLAMEGPU->getVariable<unsigned int>("br");
    unsigned int sne_p = sne;
    unsigned int ppr_p = ppr;
    unsigned int rem_ll = (ll >= sne_p ? (ll - sne_p) : 0u);
    if (rem_ll < dn) {
        FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
        return flamegpu::ALIVE;
    }
    unsigned int rem_oh = (oh >= ppr_p ? (oh - ppr_p) : 0u);
    if (rem_oh < dn) {
        if (sne_p + dn < br) FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
        else FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
    }
    if (dt > 0u) {
        unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        if (gb == 1u) {
            auto q = FLAMEGPU->environment.getMacroProperty<unsigned int>("mp6_quota_mi8", FLAMEGPU->getStepCounter()+1u);
            unsigned int old = q--;
            if (old > 0u) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
        } else if (gb == 2u) {
            auto q = FLAMEGPU->environment.getMacroProperty<unsigned int>("mp6_quota_mi17", FLAMEGPU->getStepCounter()+1u);
            unsigned int old = q--;
            if (old > 0u) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
        }
    }
    return flamegpu::ALIVE;
}


