"""
HostFunction: формирование списка планеров в ops по типам (Mi-8/Mi-17).

Дата: 17.01.2026
"""
import pyflamegpu as fg

MAX_PLANERS = 400
PLANER_MAX_DAYS = 4000


class OpsListHostFunction(fg.HostFunction):
    def run(self, FLAMEGPU):
        day = FLAMEGPU.getStepCounter()
        days_total = FLAMEGPU.environment.getPropertyUInt("days_total")
        if day >= days_total:
            return

        mp_ops = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_in_ops_history")
        mp_type = FLAMEGPU.environment.getMacroPropertyUInt8("mp_planer_type")

        list_g3 = FLAMEGPU.environment.getMacroPropertyUInt32("mp_ops_list_g3")
        list_g4 = FLAMEGPU.environment.getMacroPropertyUInt32("mp_ops_list_g4")
        cnt = FLAMEGPU.environment.getMacroPropertyUInt32("mp_ops_count")

        cnt[3] = 0
        cnt[4] = 0
        base = day * MAX_PLANERS

        for idx in range(MAX_PLANERS):
            if mp_ops[base + idx] == 0:
                continue
            ptype = mp_type[idx]
            if ptype == 1:
                pos = int(cnt[3])
                if pos < MAX_PLANERS:
                    list_g3[pos] = int(idx)
                    cnt[3] = pos + 1
            elif ptype == 2:
                pos = int(cnt[4])
                if pos < MAX_PLANERS:
                    list_g4[pos] = int(idx)
                    cnt[4] = pos + 1

        if day in (3000, 3649):
            print(f"   ops_list: day={day} g3_count={int(cnt[3])} g4_count={int(cnt[4])}")


def register_rtc(model: fg.ModelDescription):
    layer = model.newLayer("layer_units_msg_ops_list")
    layer.addHostFunction(OpsListHostFunction())
    print("  RTC модуль units_ops_list_msg зарегистрирован")
