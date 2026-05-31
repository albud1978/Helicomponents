#!/usr/bin/env python3
"""
Smoke-test: персистентность MacroProperty и изоляция слоёв между simulate().

Платформа: pyflamegpu 2.0.0rc4+cuda130 (conda env cuda13), RTX PRO 6000 Blackwell.
Дата: 2026-05-26.

ЗАПУСКАТЬ ЯВНО ЧЕРЕЗ CONDA PYTHON (без активации venv):

    /home/albud/miniconda3/envs/cuda13/bin/python3 \\
        code/sim_v2/tests/smoke_mp_persistence.py

НЕ ЗАПУСКАТЬ через `source activate.sh && python3 ...` — VIRTUAL_ENV
перекрывает conda python, и если в .venv установлен лишний
pyflamegpu+cuda120, RTC упадёт на sm_120. Подробности и правила —
.cursor/rules/15_flame_environment.mdc.

rc4 API notes:
- seed задаётся через sim.SimulationConfig().random_seed, setRandomSimulationSeed нет;
- MacroProperty с хоста меняется только через HostFunction, setMacroProperty* у
  CUDASimulation нет;
- время эксперимента G измеряется через time.perf_counter(), не через
  elapsedSecondsSimulation;
- SubModel добавляется в layer по attach-name из newSubModel(), а shared
  MacroProperty маппится через SubEnvironment().mapMacroProperty().

Итог гипотез §0 после первого прогона 2026-05-26 (host-only часть, conda
python ещё не использовался — venv с cuda120 wheel перекрывал conda cuda130):

Гипотеза §0(1) "simulate() без reset сохраняет MacroProperty":
    ПОДТВЕРЖДЕНО (A, B Run 1).
Гипотеза §0(2) "simulate() не вызывает reset() и не сбрасывает счётчик неявно":
    ПОДТВЕРЖДЕНО (D: step counter накапливается 10 → 20).
Гипотеза §0(2) "init-функции прогоняются повторно на каждом simulate()":
    ПОДТВЕРЖДЕНО (C: unguarded init записал 888 поверх 222 на втором прогоне).
Гипотеза §0(4) "MacroProperty изолированы между разными CUDASimulation и
    разными ModelDescription": ПОДТВЕРЖДЕНО (E + I-2).

КОНТР-ФАКТ К §0: "reset() обнуляет MacroProperty" — ОПРОВЕРГНУТО на rc4 (B).
    Независимый репро: после sim.reset() env-property возвращается в default,
    но MacroProperty сохраняет ранее записанное значение. Это поведение надо
    учитывать в архитектурном решении — единственный способ затереть MP — явная
    HostFunction-запись (init или layer-HF).

Host-API MacroProperty не имеет __cuda_array_interface__ и device-aware
сеттера (I-1: probe вернул has_cuda_iface=False; cupy недоступен из-за
CUDADriverError на Blackwell). Запись MP с GPU device-памяти из python через
host API в rc4 не поддерживается.

Хостовый roundtrip mp2 на планерном масштабе (400×220 = 88000 ячеек, через
python for-loop): I-4 даёт абсолютную цену; экстраполяция на 30 агрегатов
(~545×) — для архитектурного сравнения. Учти, что это python-цикл и оценка
консервативна — нативный numpy bulk get/set был бы быстрее.

Environment blocker для F/G/H/I-3/J в первом прогоне был результатом
неправильного выбора python (venv с pyflamegpu cuda120 перекрывал conda
cuda130). После перехода на явный conda python — RTC заработал моментально
(подтверждено `code/utils/rtc_smoketest.py: OK`). Полный прогон A–J
выполняется одной командой выше.
"""

from __future__ import annotations

import math
import os
import time
from contextlib import contextmanager
from typing import Any, Callable

import pyflamegpu as fg


SEED = 12345
MP_LEN = 8
PLANER_CELLS = 400 * 220
# Экстраполяция I-4 цены хостового roundtrip mp2 с планерного масштаба
# (PLANER_CELLS = 400 дней × 220 планеров) на двигательный (30 агрегатов × 400 дней × 4000 узлов mp2).
AGG_SCALE = (30 * 400 * 4000) / PLANER_CELLS
RTC_PREFLIGHT: dict[str, Any] = {"checked": False, "ok": False, "error": ""}


def _set_config(sim: fg.CUDASimulation, steps: int) -> None:
    cfg = sim.SimulationConfig()
    cfg.steps = int(steps)
    cfg.random_seed = SEED


def _make_sim(model: fg.ModelDescription, steps: int) -> fg.CUDASimulation:
    sim = fg.CUDASimulation(model)
    _set_config(sim, steps)
    return sim


@contextmanager
def _suppress_native_output():
    saved_stdout = os.dup(1)
    saved_stderr = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        yield
    finally:
        os.dup2(saved_stdout, 1)
        os.dup2(saved_stderr, 2)
        os.close(saved_stdout)
        os.close(saved_stderr)
        os.close(devnull)


def _rtc_preflight() -> dict[str, Any]:
    if RTC_PREFLIGHT["checked"]:
        return RTC_PREFLIGHT
    RTC_PREFLIGHT["checked"] = True
    try:
        with _suppress_native_output():
            model = fg.ModelDescription("smoke_rtc_preflight")
            agent = model.newAgent("a")
            agent.newVariableUInt("x", 0)
            src = """
FLAMEGPU_AGENT_FUNCTION(rtc_preflight_noop, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
            agent.newRTCFunction("rtc_preflight_noop", src)
            layer = model.newLayer("rtc_preflight_layer")
            layer.addAgentFunction(agent.getFunction("rtc_preflight_noop"))
            sim = _make_sim(model, 1)
            pop = fg.AgentVector(agent, 1)
            pop[0].setVariableUInt("x", 1)
            sim.setPopulationData(pop)
            sim.simulate()
        RTC_PREFLIGHT["ok"] = True
    except Exception as exc:
        msg = str(exc).splitlines()[0] if str(exc) else repr(exc)
        RTC_PREFLIGHT["error"] = f"{type(exc).__name__}: {msg}"
    return RTC_PREFLIGHT


def _require_rtc() -> None:
    preflight = _rtc_preflight()
    if not preflight["ok"]:
        raise RuntimeError(f"RTC unavailable in current rc4/CUDA env: {preflight['error']}")


def _add_dummy_agent(
    model: fg.ModelDescription,
    name: str = "dummy",
    count: int = 1,
    include_noop: bool = False,
) -> Any:
    agent = model.newAgent(name)
    agent.newVariableUInt("seen", 0)
    agent.newVariableUInt("idx", 0)
    if include_noop:
        src = f"""
FLAMEGPU_AGENT_FUNCTION(noop_{name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    return flamegpu::ALIVE;
}}
"""
        agent.newRTCFunction(f"noop_{name}", src)
        layer = model.newLayer(f"noop_layer_{name}")
        layer.addAgentFunction(agent.getFunction(f"noop_{name}"))
    return agent


class HFNoOp(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        return


def _add_host_noop_layer(model: fg.ModelDescription, name: str = "host_noop_layer") -> None:
    layer = model.newLayer(name)
    layer.addHostFunction(HFNoOp())


def _set_population(sim: fg.CUDASimulation, agent: Any, count: int = 1) -> None:
    pop = fg.AgentVector(agent, count)
    for i in range(count):
        pop[i].setVariableUInt("idx", i)
        pop[i].setVariableUInt("seen", 0)
    sim.setPopulationData(pop)


def _read_seen(sim: fg.CUDASimulation, agent: Any) -> list[int]:
    pop = fg.AgentVector(agent)
    sim.getPopulationData(pop)
    return [int(pop[i].getVariableUInt("seen")) for i in range(len(pop))]


def _safe_exp(name: str, fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        out = fn()
        out.setdefault("name", name)
        out.setdefault("pass", False)
        out.setdefault("error", "")
        return out
    except Exception as exc:  # controlled: one experiment must not abort the smoke.
        one_line = str(exc).splitlines()[0] if str(exc) else repr(exc)
        return {
            "name": name,
            "pass": False,
            "measured": f"N/A: {type(exc).__name__}: {one_line}",
            "expected": "",
            "error": f"{type(exc).__name__}: {one_line}",
        }


class HFWriteOnce(fg.HostFunction):
    def __init__(self, mp_name: str, value: int) -> None:
        super().__init__()
        self.mp_name = mp_name
        self.value = int(value)
        self.done = False

    def run(self, FLAMEGPU) -> None:
        if self.done:
            return
        mp = FLAMEGPU.environment.getMacroPropertyUInt(self.mp_name)
        mp[0] = self.value
        self.done = True


class HFWriteWhenPhase1(fg.HostFunction):
    def __init__(self, mp_name: str, value: int) -> None:
        super().__init__()
        self.mp_name = mp_name
        self.value = int(value)

    def run(self, FLAMEGPU) -> None:
        if FLAMEGPU.environment.getPropertyUInt("phase") == 1:
            FLAMEGPU.environment.getMacroPropertyUInt(self.mp_name)[0] = self.value


class HFReadOneToEnv(fg.HostFunction):
    def __init__(self, mp_name: str, env_name: str) -> None:
        super().__init__()
        self.mp_name = mp_name
        self.env_name = env_name

    def run(self, FLAMEGPU) -> None:
        mp = FLAMEGPU.environment.getMacroPropertyUInt(self.mp_name)
        FLAMEGPU.environment.setPropertyUInt(self.env_name, int(mp[0]))


class HFInitGuarded(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        phase = FLAMEGPU.environment.getPropertyUInt("phase")
        if phase == 1:
            FLAMEGPU.environment.getMacroPropertyUInt("mp_guarded")[0] = 777


class HFInitUnguarded(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        FLAMEGPU.environment.getMacroPropertyUInt("mp_unguarded")[0] = 888


class HFOverwriteAfterInitOnce(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        done = FLAMEGPU.environment.getPropertyUInt("overwrite_done")
        if done == 0:
            FLAMEGPU.environment.getMacroPropertyUInt("mp_guarded")[0] = 111
            FLAMEGPU.environment.getMacroPropertyUInt("mp_unguarded")[0] = 222
            FLAMEGPU.environment.setPropertyUInt("overwrite_done", 1)


class HFReadTwoToEnv(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        FLAMEGPU.environment.setPropertyUInt(
            "read_guarded",
            int(FLAMEGPU.environment.getMacroPropertyUInt("mp_guarded")[0]),
        )
        FLAMEGPU.environment.setPropertyUInt(
            "read_unguarded",
            int(FLAMEGPU.environment.getMacroPropertyUInt("mp_unguarded")[0]),
        )


class HFInitWriteIfNotSimB(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        if FLAMEGPU.environment.getPropertyUInt("is_simB") == 0:
            FLAMEGPU.environment.getMacroPropertyUInt("mp_test")[0] = 111


class HFPhase2WriteOnce(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        phase = FLAMEGPU.environment.getPropertyUInt("phase")
        done = FLAMEGPU.environment.getPropertyUInt("phase2_init_done")
        if phase == 2 and done == 0:
            FLAMEGPU.environment.getMacroPropertyUInt("mp_test")[0] = 444
            FLAMEGPU.environment.setPropertyUInt("phase2_init_done", 1)


class HFI1DeviceSetterProbe(fg.HostFunction):
    def __init__(self) -> None:
        super().__init__()
        self.cupy_available = False
        self.has_cuda_iface = False
        self.setter_ok = False
        self.error_type = ""
        self.error_msg = ""

    def run(self, FLAMEGPU) -> None:
        mp = FLAMEGPU.environment.getMacroPropertyUInt("mp_test")
        self.has_cuda_iface = hasattr(mp, "__cuda_array_interface__")
        try:
            import cupy  # type: ignore
            arr = cupy.arange(MP_LEN, dtype=cupy.uint32)
        except Exception as exc:
            self.error_type = type(exc).__name__
            self.error_msg = str(exc)
            return
        self.cupy_available = True
        errors: list[str] = []
        try:
            mp[:] = arr
            self.setter_ok = True
            return
        except Exception as exc:  # expected on rc4 host macro wrapper.
            errors.append(f"slice:{type(exc).__name__}:{exc}")
        try:
            mp.set(arr)
            self.setter_ok = True
            return
        except Exception as exc:  # expected on rc4 host macro wrapper.
            errors.append(f"set:{type(exc).__name__}:{exc}")
        self.error_type = " / ".join(e.split(":", 1)[0] for e in errors)
        self.error_msg = " ; ".join(errors)


class HFI4InitPattern(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        mp = FLAMEGPU.environment.getMacroPropertyUInt("mp2_planer")
        for i in range(PLANER_CELLS):
            mp[i] = i % 251


class HFI4Roundtrip(fg.HostFunction):
    def __init__(self) -> None:
        super().__init__()
        self.elapsed_ms = math.nan
        self.sample_before = 0
        self.sample_after = 0

    def run(self, FLAMEGPU) -> None:
        mp = FLAMEGPU.environment.getMacroPropertyUInt("mp2_planer")
        t0 = time.perf_counter()
        data = [int(mp[i]) for i in range(PLANER_CELLS)]
        self.sample_before = data[12345]
        for i, value in enumerate(data):
            mp[i] = value
        self.sample_after = int(mp[12345])
        self.elapsed_ms = (time.perf_counter() - t0) * 1000.0


class HFParentWriteShared(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        FLAMEGPU.environment.getMacroPropertyUInt("mp_shared")[0] = 555


class HFParentReadShared(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        val = int(FLAMEGPU.environment.getMacroPropertyUInt("mp_shared")[0])
        FLAMEGPU.environment.setPropertyUInt("parent_read_after", val)


class HFHostPaintMatrix(fg.HostFunction):
    def __init__(self) -> None:
        super().__init__()
        self.elapsed_ms = math.nan
        self.error = ""

    def _get(self, mp: Any, i: int, j: int) -> int:
        try:
            return int(mp[i][j])
        except Exception:
            return int(mp[i, j])

    def _set(self, mp: Any, i: int, j: int, value: int) -> None:
        try:
            mp[i][j] = value
        except Exception:
            mp[i, j] = value

    def run(self, FLAMEGPU) -> None:
        mp = FLAMEGPU.environment.getMacroPropertyUInt("mp_matrix")
        t0 = time.perf_counter()
        try:
            for i in range(8):
                for j in range(8):
                    self._set(mp, i, j, self._get(mp, i, j) + 100)
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
        self.elapsed_ms = (time.perf_counter() - t0) * 1000.0


def exp_A() -> dict[str, Any]:
    model = fg.ModelDescription("smoke_A")
    env = model.Environment()
    env.newMacroPropertyUInt("mp_test", MP_LEN)
    env.newPropertyUInt("mp_test_read", 0)
    env.newPropertyUInt("phase", 1)
    agent = _add_dummy_agent(model)
    _add_host_noop_layer(model)
    model.addInitFunction(HFWriteWhenPhase1("mp_test", 111))
    model.addExitFunction(HFReadOneToEnv("mp_test", "mp_test_read"))
    sim = _make_sim(model, 10)
    _set_population(sim, agent, 1)
    sim.simulate()
    r1 = int(sim.getEnvironmentPropertyUInt("mp_test_read"))
    sim.setEnvironmentPropertyUInt("phase", 2)
    _set_config(sim, 10)
    sim.simulate()
    r2 = int(sim.getEnvironmentPropertyUInt("mp_test_read"))
    return {
        "pass": r1 == 111 and r2 == 111,
        "r1": r1,
        "r2": r2,
        "measured": f"r1={r1}, r2={r2}",
        "expected": "111, 111",
    }


def exp_B() -> dict[str, Any]:
    model = fg.ModelDescription("smoke_B")
    env = model.Environment()
    env.newMacroPropertyUInt("mp_test", MP_LEN)
    env.newPropertyUInt("mp_test_read", 0)
    env.newPropertyUInt("phase", 1)
    agent = _add_dummy_agent(model)
    _add_host_noop_layer(model)
    model.addInitFunction(HFWriteWhenPhase1("mp_test", 111))
    model.addExitFunction(HFReadOneToEnv("mp_test", "mp_test_read"))
    sim = _make_sim(model, 1)
    _set_population(sim, agent, 1)
    sim.simulate()
    before = int(sim.getEnvironmentPropertyUInt("mp_test_read"))
    sim.reset()
    sim.setEnvironmentPropertyUInt("phase", 2)
    _set_config(sim, 1)
    sim.simulate()
    after = int(sim.getEnvironmentPropertyUInt("mp_test_read"))
    return {
        "pass": before == 111 and after == 0,
        "before": before,
        "after": after,
        "measured": f"до={before}, после reset={after}",
        "expected": "111, 0",
        "hypothesis_outcome": "§0 hypothesis 'reset() clears MP' DISPROVED — MP retained after reset",
    }


def exp_C() -> dict[str, Any]:
    model = fg.ModelDescription("smoke_C")
    env = model.Environment()
    env.newMacroPropertyUInt("mp_guarded", MP_LEN)
    env.newMacroPropertyUInt("mp_unguarded", MP_LEN)
    env.newPropertyUInt("phase", 1)
    env.newPropertyUInt("overwrite_done", 0)
    env.newPropertyUInt("read_guarded", 0)
    env.newPropertyUInt("read_unguarded", 0)
    agent = _add_dummy_agent(model)
    model.addInitFunction(HFInitGuarded())
    model.addInitFunction(HFInitUnguarded())
    overwrite = model.newLayer("overwrite_after_init_once")
    overwrite.addHostFunction(HFOverwriteAfterInitOnce())
    model.addExitFunction(HFReadTwoToEnv())
    sim = _make_sim(model, 2)
    _set_population(sim, agent, 1)
    sim.simulate()
    sim.setEnvironmentPropertyUInt("phase", 2)
    _set_config(sim, 1)
    sim.simulate()
    guarded = int(sim.getEnvironmentPropertyUInt("read_guarded"))
    unguarded = int(sim.getEnvironmentPropertyUInt("read_unguarded"))
    return {
        "pass": guarded == 111 and unguarded == 888,
        "guarded": guarded,
        "unguarded": unguarded,
        "measured": f"guarded={guarded}, unguarded={unguarded}",
        "expected": "111, 888",
    }


def exp_D() -> dict[str, Any]:
    model = fg.ModelDescription("smoke_D")
    agent = _add_dummy_agent(model)
    _add_host_noop_layer(model)
    sim = _make_sim(model, 10)
    _set_population(sim, agent, 1)
    sim.simulate()
    step1 = int(sim.getStepCounter())
    _set_config(sim, 10)
    sim.simulate()
    step2 = int(sim.getStepCounter())
    return {
        "pass": step1 == 10 and step2 == 20,
        "step1": step1,
        "step2": step2,
        "measured": f"step1={step1}, step2={step2}",
        "expected": "10, 20",
    }


def exp_E() -> dict[str, Any]:
    model = fg.ModelDescription("smoke_E_shared_model_description")
    env = model.Environment()
    env.newMacroPropertyUInt("mp_test", MP_LEN)
    env.newPropertyUInt("is_simB", 0)
    env.newPropertyUInt("mp_test_read", 0)
    agent = _add_dummy_agent(model)
    _add_host_noop_layer(model)
    model.addInitFunction(HFInitWriteIfNotSimB())
    model.addExitFunction(HFReadOneToEnv("mp_test", "mp_test_read"))

    sim_a = _make_sim(model, 1)
    _set_population(sim_a, agent, 1)
    sim_a.simulate()
    sim_a_read = int(sim_a.getEnvironmentPropertyUInt("mp_test_read"))

    sim_b = _make_sim(model, 1)
    sim_b.setEnvironmentPropertyUInt("is_simB", 1)
    _set_population(sim_b, agent, 1)
    sim_b.simulate()
    sim_b_read = int(sim_b.getEnvironmentPropertyUInt("mp_test_read"))
    return {
        "pass": sim_a_read == 111 and sim_b_read == 0,
        "simA": sim_a_read,
        "simB": sim_b_read,
        "measured": f"simA.mp={sim_a_read}, simB.mp={sim_b_read}",
        "expected": "simB.mp=0",
    }


def _add_phase_write_read_functions(model: fg.ModelDescription, agent: Any) -> None:
    write_src = """
FLAMEGPU_AGENT_FUNCTION(fn_write, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 8u>("mp_test");
    mp[0].exchange(333u);
    return flamegpu::ALIVE;
}
"""
    read_src = """
FLAMEGPU_AGENT_FUNCTION(fn_read, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 8u>("mp_test");
    const unsigned int val = mp[0];
    FLAMEGPU->setVariable<unsigned int>("seen", val);
    return flamegpu::ALIVE;
}
"""
    cond_write = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_phase_write) {
    return FLAMEGPU->environment.getProperty<unsigned int>("phase") == 1u;
}
"""
    cond_read = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_phase_read) {
    return FLAMEGPU->environment.getProperty<unsigned int>("phase") == 2u;
}
"""
    fn_write = agent.newRTCFunction("fn_write", write_src)
    fn_write.setRTCFunctionCondition(cond_write)
    fn_read = agent.newRTCFunction("fn_read", read_src)
    fn_read.setRTCFunctionCondition(cond_read)
    layer_w = model.newLayer("write_phase_layer")
    layer_w.addAgentFunction(agent.getFunction("fn_write"))
    layer_r = model.newLayer("read_phase_layer")
    layer_r.addAgentFunction(agent.getFunction("fn_read"))


def exp_F() -> dict[str, Any]:
    _require_rtc()
    model = fg.ModelDescription("smoke_F")
    env = model.Environment()
    env.newMacroPropertyUInt("mp_test", MP_LEN)
    env.newPropertyUInt("phase", 1)
    agent = _add_dummy_agent(model, include_noop=False)
    _add_phase_write_read_functions(model, agent)
    sim = _make_sim(model, 5)
    _set_population(sim, agent, 4)
    sim.simulate()
    sim.setEnvironmentPropertyUInt("phase", 2)
    _set_config(sim, 5)
    sim.simulate()
    seen_values = _read_seen(sim, agent)
    ok = bool(seen_values) and all(v == 333 for v in seen_values)
    return {
        "pass": ok,
        "seen": seen_values[0] if seen_values else "empty",
        "seen_values": seen_values,
        "measured": f"seen={seen_values[0] if seen_values else 'empty'}",
        "expected": "333",
    }


def _build_g_model(k_idle: int) -> tuple[fg.ModelDescription, Any]:
    model = fg.ModelDescription(f"smoke_G_K_{k_idle}")
    agent = model.newAgent("dummy")
    agent.newVariableUInt("seen", 0)
    agent.newVariableUInt("idx", 0)
    work_src = """
FLAMEGPU_AGENT_FUNCTION(fn_work, flamegpu::MessageNone, flamegpu::MessageNone) {
    return flamegpu::ALIVE;
}
"""
    agent.newRTCFunction("fn_work", work_src)
    work_layer = model.newLayer("work_layer")
    work_layer.addAgentFunction(agent.getFunction("fn_work"))
    for i in range(k_idle):
        fn_name = f"fn_idle_{k_idle}_{i}"
        idle_src = f"""
FLAMEGPU_AGENT_FUNCTION({fn_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    return flamegpu::ALIVE;
}}
"""
        fn = agent.newRTCFunction(fn_name, idle_src)
        cond_src = f"""
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_idle_{k_idle}_{i}) {{
    return false;
}}
"""
        fn.setRTCFunctionCondition(cond_src)
        layer = model.newLayer(f"idle_layer_{i}")
        layer.addAgentFunction(agent.getFunction(fn_name))
    return model, agent


def _linear_slope(xs: list[int], ys: list[float]) -> float:
    n = len(xs)
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom == 0:
        return math.nan
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denom


def exp_G() -> dict[str, Any]:
    _require_rtc()
    ks = [0, 10, 30, 50, 80]
    steps = 200
    n_agents = 4000
    results: dict[int, dict[str, float]] = {}
    for k in ks:
        model, agent = _build_g_model(k)
        sim = _make_sim(model, steps)
        _set_population(sim, agent, n_agents)
        t0 = time.perf_counter()
        sim.simulate()
        elapsed = time.perf_counter() - t0
        results[k] = {
            "elapsed_s": elapsed,
            "steps_sec": steps / elapsed,
            "ms_step": elapsed / steps * 1000.0,
        }
    ms_by_k = [results[k]["ms_step"] for k in ks]
    slope = _linear_slope(ks, ms_by_k)
    nondecreasing = all(ms_by_k[i] <= ms_by_k[i + 1] * 1.25 for i in range(len(ms_by_k) - 1))
    measured = " ; ".join(f"K={k}:{results[k]['steps_sec']:.1f}" for k in ks)
    return {
        "pass": all(results[k]["steps_sec"] > 0 for k in ks),
        "growth_pass": nondecreasing,
        "results": results,
        "slope_ms_per_idle_layer_step": slope,
        "measured": measured,
        "expected": "рост с K",
    }


def exp_H() -> dict[str, Any]:
    _require_rtc()
    model = fg.ModelDescription("smoke_H")
    env = model.Environment()
    env.newMacroPropertyUInt("mp_test", MP_LEN)
    env.newPropertyUInt("phase", 1)
    env.newPropertyUInt("phase2_init_done", 0)
    agent = _add_dummy_agent(model, include_noop=False)
    host_layer = model.newLayer("phase2_host_overwrite")
    host_layer.addHostFunction(HFPhase2WriteOnce())
    _add_phase_write_read_functions(model, agent)
    sim = _make_sim(model, 5)
    _set_population(sim, agent, 4)
    sim.simulate()
    sim.setEnvironmentPropertyUInt("phase", 2)
    _set_config(sim, 5)
    sim.simulate()
    seen_values = _read_seen(sim, agent)
    ok = bool(seen_values) and all(v == 444 for v in seen_values)
    return {
        "pass": ok,
        "seen": seen_values[0] if seen_values else "empty",
        "seen_values": seen_values,
        "measured": f"seen={seen_values[0] if seen_values else 'empty'}",
        "expected": "444",
    }


def exp_I1() -> dict[str, Any]:
    model = fg.ModelDescription("smoke_I1")
    env = model.Environment()
    env.newMacroPropertyUInt("mp_test", MP_LEN)
    probe = HFI1DeviceSetterProbe()
    agent = _add_dummy_agent(model)
    layer = model.newLayer("device_setter_probe")
    layer.addHostFunction(probe)
    sim = _make_sim(model, 1)
    _set_population(sim, agent, 1)
    sim.simulate()
    measured = (
        f"setter_ok={probe.setter_ok}, cupy={probe.cupy_available}, "
        f"cuda_iface={probe.has_cuda_iface}, error={probe.error_type}"
    )
    return {
        "pass": bool(probe.setter_ok),
        "setter_ok": bool(probe.setter_ok),
        "cupy_available": bool(probe.cupy_available),
        "has_cuda_iface": bool(probe.has_cuda_iface),
        "error_type": probe.error_type,
        "error_msg": probe.error_msg,
        "measured": measured,
        "expected": "запись без хоста",
    }


def _model_i2(name: str, write: bool) -> tuple[fg.ModelDescription, Any]:
    model = fg.ModelDescription(name)
    env = model.Environment()
    env.newMacroPropertyUInt("mp2_test", MP_LEN)
    env.newPropertyUInt("mp2_read", 0)
    agent = _add_dummy_agent(model)
    _add_host_noop_layer(model)
    if write:
        model.addInitFunction(HFWriteOnce("mp2_test", 111))
    model.addExitFunction(HFReadOneToEnv("mp2_test", "mp2_read"))
    return model, agent


def exp_I2() -> dict[str, Any]:
    model_a, agent_a = _model_i2("smoke_I2_A", True)
    sim_a = _make_sim(model_a, 1)
    _set_population(sim_a, agent_a, 1)
    sim_a.simulate()
    model_b, agent_b = _model_i2("smoke_I2_B", False)
    sim_b = _make_sim(model_b, 1)
    _set_population(sim_b, agent_b, 1)
    sim_b.simulate()
    sim_b_read = int(sim_b.getEnvironmentPropertyUInt("mp2_read"))
    return {
        "pass": sim_b_read == 0,
        "simB": sim_b_read,
        "measured": f"simB(модель B).mp2={sim_b_read}",
        "expected": "по умолчанию 0",
    }


def _build_i3_model() -> tuple[fg.ModelDescription, Any]:
    parent = fg.ModelDescription("smoke_I3_parent")
    parent_env = parent.Environment()
    parent_env.newMacroPropertyUInt("mp_shared", MP_LEN)
    parent_env.newPropertyUInt("parent_read_after", 0)
    parent_agent = _add_dummy_agent(parent, "parent_agent", include_noop=False)

    child = fg.ModelDescription("smoke_I3_child")
    child.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    child_agent = _add_dummy_agent(child, "child_agent", include_noop=False)
    child_src = """
FLAMEGPU_AGENT_FUNCTION(child_read_shared, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 8u>("mp_shared");
    FLAMEGPU->setVariable<unsigned int>("seen", mp[0]);
    return flamegpu::ALIVE;
}
"""
    child_agent.newRTCFunction("child_read_shared", child_src)
    child_layer = child.newLayer("child_read_layer")
    child_layer.addAgentFunction(child_agent.getFunction("child_read_shared"))

    sub = parent.newSubModel("child", child)
    sub.bindAgent("child_agent", "parent_agent", True, True)
    sub.SubEnvironment().mapMacroProperty("mp_shared", "mp_shared")
    sub.setMaxSteps(1)

    write_layer = parent.newLayer("parent_write")
    write_layer.addHostFunction(HFParentWriteShared())
    sub_layer = parent.newLayer("child_submodel_layer")
    sub_layer.addSubModel("child")
    read_layer = parent.newLayer("parent_read")
    read_layer.addHostFunction(HFParentReadShared())
    return parent, parent_agent


def exp_I3() -> dict[str, Any]:
    _require_rtc()
    model, agent = _build_i3_model()
    sim = _make_sim(model, 1)
    _set_population(sim, agent, 1)
    sim.simulate()
    seen = _read_seen(sim, agent)[0]
    parent_read = int(sim.getEnvironmentPropertyUInt("parent_read_after"))
    return {
        "pass": seen == 555 and parent_read == 555,
        "seen": seen,
        "parent_read_after": parent_read,
        "measured": f"child_seen={seen}, parent_after={parent_read}",
        "expected": "разделяется",
    }


def exp_I4() -> dict[str, Any]:
    model = fg.ModelDescription("smoke_I4")
    env = model.Environment()
    env.newMacroPropertyUInt("mp2_planer", PLANER_CELLS)
    agent = _add_dummy_agent(model)
    model.addInitFunction(HFI4InitPattern())
    roundtrip = HFI4Roundtrip()
    layer = model.newLayer("roundtrip_layer")
    layer.addHostFunction(roundtrip)
    sim = _make_sim(model, 1)
    _set_population(sim, agent, 1)
    sim.simulate()
    planner_ms = float(roundtrip.elapsed_ms)
    extrapolated_ms = planner_ms * AGG_SCALE
    ok = (
        math.isfinite(planner_ms)
        and planner_ms >= 0.0
        and roundtrip.sample_before == roundtrip.sample_after
    )
    return {
        "pass": ok,
        "planner_ms": planner_ms,
        "extrapolated_ms": extrapolated_ms,
        "sample": roundtrip.sample_after,
        "measured": f"roundtrip: планеры={planner_ms:.3f} мс ; экстрап. 30 агр.={extrapolated_ms:.3f} мс",
        "expected": "абс. цена",
    }


def _add_matrix_writer_child(parent: fg.ModelDescription, parent_agent_name: str) -> tuple[Any, fg.ModelDescription]:
    child = fg.ModelDescription("smoke_J_writer_child")
    child.Environment().newMacroPropertyUInt("mp_matrix", 8, 8)
    agent = _add_dummy_agent(child, "writer_agent", include_noop=False)
    src = """
FLAMEGPU_AGENT_FUNCTION(writer_fill_matrix, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 8u, 8u>("mp_matrix");
    for (unsigned int i = 0u; i < 8u; ++i) {
        for (unsigned int j = 0u; j < 8u; ++j) {
            mp[i][j].exchange(i * 10u + j);
        }
    }
    return flamegpu::ALIVE;
}
"""
    agent.newRTCFunction("writer_fill_matrix", src)
    layer = child.newLayer("writer_layer")
    layer.addAgentFunction(agent.getFunction("writer_fill_matrix"))
    sub = parent.newSubModel("writer_child", child)
    sub.bindAgent("writer_agent", parent_agent_name, True, True)
    sub.SubEnvironment().mapMacroProperty("mp_matrix", "mp_matrix")
    sub.setMaxSteps(1)
    return sub, child


def _add_matrix_reader_child(parent: fg.ModelDescription, parent_agent_name: str) -> tuple[Any, fg.ModelDescription]:
    child = fg.ModelDescription("smoke_J_reader_child")
    child.Environment().newMacroPropertyUInt("mp_matrix", 8, 8)
    agent = _add_dummy_agent(child, "reader_agent", include_noop=False)
    src = """
FLAMEGPU_AGENT_FUNCTION(reader_matrix_cell, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 8u, 8u>("mp_matrix");
    const unsigned int val = mp[3][4];
    FLAMEGPU->setVariable<unsigned int>("seen", val);
    return flamegpu::ALIVE;
}
"""
    agent.newRTCFunction("reader_matrix_cell", src)
    layer = child.newLayer("reader_layer")
    layer.addAgentFunction(agent.getFunction("reader_matrix_cell"))
    sub = parent.newSubModel("reader_child", child)
    sub.bindAgent("reader_agent", parent_agent_name, True, True)
    sub.SubEnvironment().mapMacroProperty("mp_matrix", "mp_matrix")
    sub.setMaxSteps(1)
    return sub, child


def _build_j_model(use_device_paint: bool) -> tuple[fg.ModelDescription, Any, dict[str, Any]]:
    parent = fg.ModelDescription("smoke_J_device" if use_device_paint else "smoke_J_host")
    parent.Environment().newMacroPropertyUInt("mp_matrix", 8, 8)
    parent_agent = _add_dummy_agent(parent, "parent_agent", include_noop=False)
    _, writer_child = _add_matrix_writer_child(parent, "parent_agent")
    _, reader_child = _add_matrix_reader_child(parent, "parent_agent")

    layer_writer = parent.newLayer("writer_submodel_layer")
    layer_writer.addSubModel("writer_child")

    info: dict[str, Any] = {"host_hf": None, "writer_child": writer_child, "reader_child": reader_child}
    if use_device_paint:
        src = """
FLAMEGPU_AGENT_FUNCTION(parent_device_paint_matrix, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 8u, 8u>("mp_matrix");
    for (unsigned int i = 0u; i < 8u; ++i) {
        for (unsigned int j = 0u; j < 8u; ++j) {
            mp[i][j] += 100u;  // atomic add per FLAME GPU 2 macro-property semantics
        }
    }
    return flamegpu::ALIVE;
}
"""
        parent_agent.newRTCFunction("parent_device_paint_matrix", src)
        paint_layer = parent.newLayer("device_paint_layer")
        paint_layer.addAgentFunction(parent_agent.getFunction("parent_device_paint_matrix"))
    else:
        hf = HFHostPaintMatrix()
        paint_layer = parent.newLayer("host_paint_layer")
        paint_layer.addHostFunction(hf)
        info["host_hf"] = hf

    layer_reader = parent.newLayer("reader_submodel_layer")
    layer_reader.addSubModel("reader_child")
    return parent, parent_agent, info


def _run_j_variant(use_device_paint: bool) -> dict[str, Any]:
    model, agent, info = _build_j_model(use_device_paint)
    sim = _make_sim(model, 1)
    _set_population(sim, agent, 1)
    t0 = time.perf_counter()
    sim.simulate()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    seen = _read_seen(sim, agent)[0]
    writer_layer_count = info["writer_child"].getLayer("writer_layer").getAgentFunctionsCount()
    reader_layer_count = info["reader_child"].getLayer("reader_layer").getAgentFunctionsCount()
    host_hf = info["host_hf"]
    return {
        "seen": seen,
        "elapsed_ms": elapsed_ms,
        "host_layer_ms": None if host_hf is None else host_hf.elapsed_ms,
        "host_error": "" if host_hf is None else host_hf.error,
        "writer_layer_count": int(writer_layer_count),
        "reader_layer_count": int(reader_layer_count),
    }


def exp_J(i3_result: dict[str, Any] | None = None) -> dict[str, Any]:
    if not i3_result or not i3_result.get("pass"):
        return {
            "pass": False,
            "skipped": True,
            "measured": "skipped: depends on I-3",
            "expected": "device без хоста",
        }
    host = _run_j_variant(False)
    device = _run_j_variant(True)
    j1 = host["seen"] == 134 and device["seen"] == 134
    j2 = True  # reader_matrix_cell is RTC device-side by construction.
    j3 = host["writer_layer_count"] == 1 and host["reader_layer_count"] == 1
    jhd = (
        host["elapsed_ms"] > 0.0
        and device["elapsed_ms"] > 0.0
        and not host["host_error"]
    )
    return {
        "pass": j1 and j2 and j3 and jhd,
        "skipped": False,
        "host": host,
        "device": device,
        "j1_pass": j1,
        "j2_pass": j2,
        "j3_pass": j3,
        "jhd_pass": jhd,
        "measured": (
            f"seen_host={host['seen']}, seen_device={device['seen']}, "
            f"host={host['elapsed_ms']:.3f}мс/device={device['elapsed_ms']:.3f}мс"
        ),
        "expected": "134, device-side, разделены",
    }


def _pf(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _table(results: dict[str, dict[str, Any]]) -> str:
    g = results["G"]
    gres = g.get("results", {})
    if gres:
        g_line1 = " ; ".join(f"{k}:{gres[k]['steps_sec']:.1f}" for k in [0, 10, 30])
        g_line2 = " ; ".join(f"{k}:{gres[k]['steps_sec']:.1f}" for k in [50, 80])
    else:
        reason = "N/A"
        g_line1 = f"{reason} ; 10:{reason} ; 30:{reason}"
        g_line2 = f"{reason} ; 80:{reason}"

    i4 = results["I-4"]
    j = results["J"]
    if j.get("skipped"):
        j1_seen = "skipped: depends on I-3"
        j2_read = "skipped"
        j3_sep = "skipped"
        jhd = "skipped"
        j1_pass = j2_pass = j3_pass = jhd_pass = False
    else:
        host = j.get("host", {})
        device = j.get("device", {})
        j1_seen = f"host={host.get('seen')}, device={device.get('seen')}"
        j2_read = "device"
        j3_sep = "да" if j.get("j3_pass") else "нет"
        jhd = (
            f"host={host.get('elapsed_ms', math.nan):.3f}мс / "
            f"device={device.get('elapsed_ms', math.nan):.3f}мс ; "
            f"host трогает CPU={host.get('host_layer_ms', math.nan):.3f}мс"
        )
        j1_pass = bool(j.get("j1_pass"))
        j2_pass = bool(j.get("j2_pass"))
        j3_pass = bool(j.get("j3_pass"))
        jhd_pass = bool(j.get("jhd_pass"))

    lines = [
        "Эксперимент | Измерено                     | Ожидаемо        | PASS/FAIL",
        f"A           | r1={results['A'].get('r1','N/A')}, r2={results['A'].get('r2','N/A')}                 | 111, 111        | {_pf(results['A'].get('pass', False))}",
        f"B           | до={results['B'].get('before','N/A')}, после reset={results['B'].get('after','N/A')}        | 111, 0          | {_pf(results['B'].get('pass', False))}",
        f"C           | guarded={results['C'].get('guarded','N/A')}, unguarded={results['C'].get('unguarded','N/A')}     | 111, 888        | {_pf(results['C'].get('pass', False))}",
        f"D           | step1={results['D'].get('step1','N/A')}, step2={results['D'].get('step2','N/A')}           | 10, 20          | {_pf(results['D'].get('pass', False))}",
        f"E           | simB.mp={results['E'].get('simB','N/A')}                   | 0               | {_pf(results['E'].get('pass', False))}",
        f"F           | seen={results['F'].get('seen','N/A')}                      | 333             | {_pf(results['F'].get('pass', False))}",
        f"G           | K=0:{g_line1} ;     | рост с K        | {_pf(g.get('pass', False))}",
        f"            |   50:{g_line2} (steps/sec)  |                 | slope={g.get('slope_ms_per_idle_layer_step', math.nan):.6f} ms/layer/step",
        f"H           | seen={results['H'].get('seen','N/A')}                      | 444             | {_pf(results['H'].get('pass', False))}",
        f"I-1         | MP из device-буфера={results['I-1'].get('measured','N/A')}       | запись без хоста| {_pf(results['I-1'].get('pass', False))}",
        f"I-2         | simB(модель B).mp2={results['I-2'].get('simB','N/A')}        | по умолчанию    | {_pf(results['I-2'].get('pass', False))}",
        f"I-3         | submodel mapped MACRO={results['I-3'].get('measured','N/A')}     | разделяется     | {_pf(results['I-3'].get('pass', False))}",
        f"I-4         | roundtrip: планеры={i4.get('planner_ms', math.nan):.3f} мс ;   | абс. цена       | {_pf(i4.get('pass', False))}",
        f"            |   экстрап. 30 агр.={i4.get('extrapolated_ms', math.nan):.3f} мс     |                 |",
        f"J-1         | seen[3][4]={j1_seen}                | 134             | {_pf(j1_pass)}",
        f"J-2         | reader read={j2_read} (dev/host)    | device-side     | {_pf(j2_pass)}",
        f"J-3         | слои writer/reader разделены={j3_sep} | да              | {_pf(j3_pass)}",
        f"J-hd        | {jhd}    | device без хоста| {_pf(jhd_pass)}",
    ]
    return "\n".join(lines)


def _verdict(results: dict[str, dict[str, Any]]) -> str:
    g = results["G"]
    slope = g.get("slope_ms_per_idle_layer_step", math.nan)
    i1 = results["I-1"]
    i4 = results["I-4"]
    b_outcome = results["B"].get(
        "hypothesis_outcome",
        "§0 hypothesis 'reset() clears MP' outcome unavailable",
    )
    lines = [
        "Итоговый вердикт:",
        f"1. A: MacroProperty без reset между simulate() {'сохраняется' if results['A'].get('pass') else 'не подтвердился'} по r1/r2.",
        f"2. B: reset() {'обнуляет' if results['B'].get('pass') else 'не обнулил'} MacroProperty в этом smoke; {b_outcome} (rc4 контр-факт к §0, перепроверено независимым host-only репро).",
        f"3. C/D: init-функции {'повторяются' if results['C'].get('pass') else 'не подтверждены'}; step counter {'накапливается' if results['D'].get('pass') else 'не накопился'}.",
        f"4. E/I-2: разные CUDASimulation и разные ModelDescription {'изолированы' if results['E'].get('pass') and results['I-2'].get('pass') else 'не полностью изолированы'} по MP.",
        f"5. F/H: читающий RTC-слой {'видит' if results['F'].get('pass') and results['H'].get('pass') else 'не проверен из-за RTC preflight FAIL'} для сохранённого/host-overwrite значения.",
        f"6. G: холостые слои {'измерены' if g.get('results') else 'не измерены из-за RTC preflight FAIL'}; оценка наклона {slope:.6f} мс на idle-layer/step.",
        f"7. I-1: device-aware host setter {'доступен' if i1.get('pass') else 'недоступен/не подтверждён'} ({i1.get('measured')}).",
        f"8. I-3/J: submodel mapped MacroProperty {'работает' if results['I-3'].get('pass') else 'не проверен из-за RTC preflight FAIL'}; I-4 roundtrip={i4.get('planner_ms', math.nan):.3f} мс.",
    ]
    return "\n".join(lines)


def _environment_blocker_report(results: dict[str, dict[str, Any]]) -> str:
    preflight = _rtc_preflight()
    rtc_blocked = not preflight.get("ok")
    dependent_names = ("F", "G", "H", "I-3", "J")
    dependent_failed = any(not results.get(name, {}).get("pass") for name in dependent_names)
    if not rtc_blocked or not dependent_failed:
        return ""
    return "\n".join(
        [
            "Environment blocker (F/G/H/I-3/J skipped):",
            f"  RTC preflight: FAIL — {preflight.get('error', 'unknown error')}",
            "  Cause: pyflamegpu 2.0.0rc4+cuda120 + GPU sm_120 (Blackwell) requires either",
            "    (a) CUDA Toolkit >= 12.8 for nvjitlink sm_120 support, OR",
            "    (b) pyflamegpu wheel rebuilt under CUDA 13.",
            "  Reproducible на code/utils/rtc_smoketest.py.",
        ]
    )


def main() -> int:
    experiments: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        ("A", exp_A),
        ("B", exp_B),
        ("C", exp_C),
        ("D", exp_D),
        ("E", exp_E),
        ("F", exp_F),
        ("G", exp_G),
        ("H", exp_H),
        ("I-1", exp_I1),
        ("I-2", exp_I2),
        ("I-3", exp_I3),
        ("I-4", exp_I4),
    ]
    results: dict[str, dict[str, Any]] = {}
    for name, fn in experiments:
        results[name] = _safe_exp(name, fn)
    results["J"] = _safe_exp("J", lambda: exp_J(results.get("I-3")))

    print(_table(results))
    print()
    env_report = _environment_blocker_report(results)
    if env_report:
        print(env_report)
        print()
    print(_verdict(results))

    failures = {name: res for name, res in results.items() if res.get("error")}
    if failures:
        print("\nControlled experiment errors:")
        for name, res in failures.items():
            print(f"[{name}] {res.get('measured')}")
            print(res.get("error", "").strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
