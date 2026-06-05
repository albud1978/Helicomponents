#!/usr/bin/env python3
"""
Shared byte-identical RTC device include for inline limiter calculation.

Текст хранится с плейсхолдером __CUMSUM_SIZE__ (как в оригинале до выноса).
Каждый потребитель сам подставляет вычисляемый RTC_MAX_FRAMES*(MAX_DAYS+1)
через .replace(), чтобы размер mp5_cumsum не был захардкожен.
"""

DEVICE_FN_COMPUTE_LIMITER = """
FLAMEGPU_DEVICE_FUNCTION unsigned short compute_limiter_inline(
    flamegpu::DeviceAPI<flamegpu::MessageNone, flamegpu::MessageNone>* FLAMEGPU,
    const unsigned int sne,
    const unsigned int ppr,
    const unsigned int ll,
    const unsigned int oh,
    const unsigned int idx,
    const unsigned int current_day
) {
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    unsigned int remaining_ll = (sne < ll) ? (ll - sne) : 0u;
    unsigned int remaining_oh = (ppr < oh) ? (oh - ppr) : 0u;
    
    if (remaining_ll == 0u || remaining_oh == 0u) {
        return 0u;
    }
    
    auto cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, __CUMSUM_SIZE__u>("mp5_cumsum");
    const unsigned int base_cumsum = cumsum[current_day * frames + idx];
    
    unsigned int days_to_oh = end_day - current_day;
    bool found_oh = false;
    {
        unsigned int lo = current_day + 1u;
        unsigned int hi = end_day;
        while (lo < hi) {
            unsigned int mid = (lo + hi) / 2u;
            unsigned int accumulated = cumsum[mid * frames + idx] - base_cumsum;
            if (accumulated >= remaining_oh) {
                hi = mid;
            } else {
                lo = mid + 1u;
            }
        }
        if (lo <= end_day) {
            unsigned int final_accumulated = cumsum[lo * frames + idx] - base_cumsum;
            if (final_accumulated >= remaining_oh) {
                days_to_oh = lo - current_day;
                found_oh = true;
            }
        }
    }
    if (!found_oh) {
        days_to_oh = (end_day - current_day) + 1u;
    }
    
    unsigned int days_to_ll = end_day - current_day;
    bool found_ll = false;
    {
        unsigned int lo = current_day + 1u;
        unsigned int hi = end_day;
        while (lo < hi) {
            unsigned int mid = (lo + hi) / 2u;
            unsigned int accumulated = cumsum[mid * frames + idx] - base_cumsum;
            if (accumulated >= remaining_ll) {
                hi = mid;
            } else {
                lo = mid + 1u;
            }
        }
        if (lo <= end_day) {
            unsigned int final_accumulated = cumsum[lo * frames + idx] - base_cumsum;
            if (final_accumulated >= remaining_ll) {
                days_to_ll = lo - current_day;
                found_ll = true;
            }
        }
    }
    if (!found_ll) {
        days_to_ll = (end_day - current_day) + 1u;
    }
    
    unsigned int limiter = (days_to_oh < days_to_ll) ? days_to_oh : days_to_ll;
    
    if (limiter > 65535u) limiter = 65535u;
    if (limiter == 0u) limiter = 1u;
    
    return (unsigned short)limiter;
}

FLAMEGPU_DEVICE_FUNCTION unsigned short compute_limiter_inline(
    flamegpu::DeviceAPI<flamegpu::MessageNone, flamegpu::MessageNone>* FLAMEGPU
) {
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    return compute_limiter_inline(FLAMEGPU, sne, ppr, ll, oh, idx, current_day);
}
"""
