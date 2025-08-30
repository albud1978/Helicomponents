
// ===== rtc_quota_intent =====

        FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {
            static const unsigned int FRAMES = 279u;
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            if (idx >= FRAMES) return flamegpu::ALIVE;
            if (gb == 1u) { auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent"); i8[idx].exchange(1u); }
            else if (gb == 2u) { auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent"); i17[idx].exchange(1u); }
            return flamegpu::ALIVE;
        }
            
// ===== rtc_quota_approve_manager =====

        FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager, flamegpu::MessageNone, flamegpu::MessageNone) {
            static const unsigned int FRAMES = 279u;
            if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int last = (days_total > 0u ? days_total - 1u : 0u);
            const unsigned int dayp1 = (day < last ? day + 1u : last);
            auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
            auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
            auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
            auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
            for (unsigned int k=0u;k<FRAMES;++k) { a8[k].exchange(0u); a17[k].exchange(0u); }
            unsigned int left8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
            unsigned int left17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
            for (unsigned int k=0u;k<FRAMES && left8>0u;++k) { if (i8[k]) { a8[k].exchange(1u); --left8; } }
            for (unsigned int k=0u;k<FRAMES && left17>0u;++k) { if (i17[k]) { a17[k].exchange(1u); --left17; } }
            return flamegpu::ALIVE;
        }
            
// ===== rtc_quota_apply =====

        FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply, flamegpu::MessageNone, flamegpu::MessageNone) {
            static const unsigned int FRAMES = 279u;
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            if (idx >= FRAMES) return flamegpu::ALIVE;
            if (gb == 1u) { auto a8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve"); if (a8[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }
            else if (gb == 2u) { auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve"); if (a17[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }
            return flamegpu::ALIVE;
        }
            