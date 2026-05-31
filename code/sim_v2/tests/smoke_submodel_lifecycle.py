#!/usr/bin/env python3
"""
Smoke-test: lifecycle многошагового FLAME GPU submodel для Варианта D.

Цель: проверить, умеет ли pyflamegpu 2.0.0rc4+cuda130 выполнять child
submodel как многошаговый adaptive runtime внутри одного parent step, с
наблюдаемым step counter и mapped MacroProperty.

ЗАПУСКАТЬ ЯВНО ЧЕРЕЗ CONDA PYTHON (без активации venv):

    /home/albud/miniconda3/envs/cuda13/bin/python3 \\
        code/sim_v2/tests/smoke_submodel_lifecycle.py

НЕ ЗАПУСКАТЬ через `source activate.sh && python3 ...`. Подробности:
.cursor/rules/15_flame_environment.mdc.

rc4 API note: runtime exit-condition задаётся через addExitCondition() +
HostCondition, который возвращает pyflamegpu.EXIT или pyflamegpu.CONTINUE.
addExitFunction() в rc4 является финальной host-функцией, не условием выхода.
"""

from __future__ import annotations

import ctypes
import math
import os
import statistics
import time
from pathlib import Path
from typing import Any, Callable


CUDA13_ENV = Path("/home/albud/miniconda3/envs/cuda13")
CUDA13_TARGET = CUDA13_ENV / "targets" / "x86_64-linux"
SEED = 12345
TARGET_DAY = 50
DAY_INCREMENT = 7
EXPECTED_CHILD_STEPS = math.ceil(TARGET_DAY / DAY_INCREMENT)
MP_LEN = 128


def _preload_cuda13_runtime() -> None:
    """Make the smoke runnable via explicit conda python in non-activated shells."""
    os.environ.setdefault("CUDA_PATH", str(CUDA13_TARGET))
    for lib_dir in (CUDA13_ENV / "lib", CUDA13_TARGET / "lib"):
        for lib_name in (
            "libnvrtc.so.13",
            "libnvrtc-builtins.so.13.0",
            "libnvJitLink.so.13",
            "libcudart.so.13",
        ):
            lib_path = lib_dir / lib_name
            if lib_path.exists():
                ctypes.CDLL(str(lib_path), mode=ctypes.RTLD_GLOBAL)


_preload_cuda13_runtime()

import pyflamegpu as fg  # noqa: E402


def _set_config(sim: fg.CUDASimulation, steps: int) -> None:
    cfg = sim.SimulationConfig()
    cfg.steps = int(steps)
    cfg.random_seed = SEED


def _make_sim(model: fg.ModelDescription, steps: int) -> fg.CUDASimulation:
    sim = fg.CUDASimulation(model)
    _set_config(sim, steps)
    return sim


def _new_common_agent(model: fg.ModelDescription, name: str) -> Any:
    agent = model.newAgent(name)
    agent.newVariableUInt("idx", 0)
    agent.newVariableUInt("seen_initial", 0)
    agent.newVariableUInt("b_seen_count", 0)
    return agent


def _set_population(sim: fg.CUDASimulation, agent: Any, count: int = 1) -> None:
    pop = fg.AgentVector(agent, count)
    for i in range(count):
        pop[i].setVariableUInt("idx", i)
        pop[i].setVariableUInt("seen_initial", 0)
        pop[i].setVariableUInt("b_seen_count", 0)
    sim.setPopulationData(pop)


def _read_agent_vars(sim: fg.CUDASimulation, agent: Any) -> dict[str, int]:
    pop = fg.AgentVector(agent)
    sim.getPopulationData(pop)
    return {
        "seen_initial": int(pop[0].getVariableUInt("seen_initial")),
        "b_seen_count": int(pop[0].getVariableUInt("b_seen_count")),
    }


def _add_lifecycle_env(
    model: fg.ModelDescription,
    prefix: str = "child",
    target_day: int = TARGET_DAY,
) -> None:
    env = model.Environment()
    env.newPropertyUInt(f"{prefix}_day", 0)
    env.newPropertyUInt(f"{prefix}_target_day", int(target_day))
    env.newPropertyUInt(f"{prefix}_hf_calls", 0)
    env.newPropertyUInt(f"{prefix}_steps_seen", 0)


def _add_parent_lifecycle_env(
    model: fg.ModelDescription,
    prefix: str = "child",
    target_day: int = TARGET_DAY,
) -> None:
    _add_lifecycle_env(model, prefix, target_day)
    env = model.Environment()
    env.newPropertyUInt(f"{prefix}_snap_1", 0)
    env.newPropertyUInt(f"{prefix}_snap_2", 0)
    env.newPropertyUInt(f"{prefix}_day_snap_1", 0)
    env.newPropertyUInt(f"{prefix}_day_snap_2", 0)


class HFIncrementDay(fg.HostFunction):
    def __init__(self, prefix: str = "child", increment: int = DAY_INCREMENT) -> None:
        super().__init__()
        self.prefix = prefix
        self.increment = int(increment)

    def run(self, FLAMEGPU) -> None:
        day_name = f"{self.prefix}_day"
        calls_name = f"{self.prefix}_hf_calls"
        steps_name = f"{self.prefix}_steps_seen"
        day = FLAMEGPU.environment.getPropertyUInt(day_name)
        calls = FLAMEGPU.environment.getPropertyUInt(calls_name)
        FLAMEGPU.environment.setPropertyUInt(day_name, day + self.increment)
        FLAMEGPU.environment.setPropertyUInt(calls_name, calls + 1)
        FLAMEGPU.environment.setPropertyUInt(steps_name, FLAMEGPU.getStepCounter() + 1)


class ExitAtTarget(fg.HostCondition):
    def __init__(self, prefix: str = "child") -> None:
        super().__init__()
        self.prefix = prefix

    def run(self, FLAMEGPU) -> int:
        day = FLAMEGPU.environment.getPropertyUInt(f"{self.prefix}_day")
        target = FLAMEGPU.environment.getPropertyUInt(f"{self.prefix}_target_day")
        return fg.EXIT if day >= target else fg.CONTINUE


class HFParentSnapshot(fg.HostFunction):
    def __init__(self, prefix: str = "child") -> None:
        super().__init__()
        self.prefix = prefix

    def run(self, FLAMEGPU) -> None:
        parent_step = FLAMEGPU.getStepCounter()
        slot = 1 if parent_step == 0 else 2
        calls = FLAMEGPU.environment.getPropertyUInt(f"{self.prefix}_hf_calls")
        day = FLAMEGPU.environment.getPropertyUInt(f"{self.prefix}_day")
        FLAMEGPU.environment.setPropertyUInt(f"{self.prefix}_snap_{slot}", calls)
        FLAMEGPU.environment.setPropertyUInt(f"{self.prefix}_day_snap_{slot}", day)


class HFParentWriteInitialMacro(fg.HostFunction):
    def run(self, FLAMEGPU) -> None:
        FLAMEGPU.environment.getMacroPropertyUInt("mp_shared")[0] = 999


class HFReadMacroSlots(fg.HostFunction):
    def __init__(self, slots: list[int]) -> None:
        super().__init__()
        self.slots = slots
        self.values: list[int] = []

    def run(self, FLAMEGPU) -> None:
        mp = FLAMEGPU.environment.getMacroPropertyUInt("mp_shared")
        self.values = [int(mp[i]) for i in self.slots]


def _add_child_noop(agent: Any, model: fg.ModelDescription, fn_name: str) -> None:
    src = f"""
FLAMEGPU_AGENT_FUNCTION({fn_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    return flamegpu::ALIVE;
}}
"""
    agent.newRTCFunction(fn_name, src)
    layer = model.newLayer(f"{fn_name}_layer")
    layer.addAgentFunction(agent.getFunction(fn_name))


def _add_child_writer(agent: Any, model: fg.ModelDescription, fn_name: str, prefix: str = "child") -> None:
    read_fn = f"{fn_name}_read_initial"
    read_src = f"""
FLAMEGPU_AGENT_FUNCTION({read_fn}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP_LEN}u>("mp_shared");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("{prefix}_day");
    const unsigned int initial = mp[0];
    if (day == 0u && initial == 999u) {{
        FLAMEGPU->setVariable<unsigned int>("seen_initial", 999u);
    }}
    return flamegpu::ALIVE;
}}
"""
    write_fn = f"{fn_name}_write_day"
    write_src = f"""
FLAMEGPU_AGENT_FUNCTION({write_fn}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP_LEN}u>("mp_shared");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("{prefix}_day");
    const unsigned int slot = day % {MP_LEN}u;
    mp[slot].exchange(day);
    return flamegpu::ALIVE;
}}
"""
    agent.newRTCFunction(read_fn, read_src)
    read_layer = model.newLayer(f"{read_fn}_layer")
    read_layer.addAgentFunction(agent.getFunction(read_fn))
    agent.newRTCFunction(write_fn, write_src)
    write_layer = model.newLayer(f"{write_fn}_layer")
    write_layer.addAgentFunction(agent.getFunction(write_fn))


def _add_child_reader(agent: Any, model: fg.ModelDescription, fn_name: str) -> None:
    expected_slots = [i * DAY_INCREMENT for i in range(EXPECTED_CHILD_STEPS)]
    checks = "\n".join(
        f"    const unsigned int value_{slot} = mp[{slot}u];\n"
        f"    count += value_{slot} == {slot}u ? 1u : 0u;"
        for slot in expected_slots
    )
    src = f"""
FLAMEGPU_AGENT_FUNCTION({fn_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP_LEN}u>("mp_shared");
    unsigned int count = 0u;
{checks}
    FLAMEGPU->setVariable<unsigned int>("b_seen_count", count);
    return flamegpu::ALIVE;
}}
"""
    agent.newRTCFunction(fn_name, src)
    layer = model.newLayer(f"{fn_name}_layer")
    layer.addAgentFunction(agent.getFunction(fn_name))


def _add_increment_and_exit(model: fg.ModelDescription, prefix: str = "child") -> None:
    layer = model.newLayer(f"{prefix}_increment_layer")
    layer.addHostFunction(HFIncrementDay(prefix))
    model.addExitCondition(ExitAtTarget(prefix))


def _map_lifecycle(sub: Any, prefix: str = "child", map_day: bool = True) -> None:
    sub_env = sub.SubEnvironment()
    names = [f"{prefix}_target_day", f"{prefix}_hf_calls", f"{prefix}_steps_seen"]
    if map_day:
        names.insert(0, f"{prefix}_day")
    for name in names:
        sub_env.mapProperty(name, name)


def _safe_run(name: str, fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        out = fn()
        out.setdefault("name", name)
        out.setdefault("pass", False)
        out.setdefault("error", "")
        return out
    except Exception as exc:  # requested: one Ki failure must not abort K1-K6.
        one_line = str(exc).splitlines()[0] if str(exc) else repr(exc)
        return {
            "name": name,
            "pass": False,
            "measured": f"N/A: {type(exc).__name__}: {one_line}",
            "expected": "",
            "error": f"{type(exc).__name__}: {one_line}",
        }


def _build_k1_parent(map_day: bool = True, parent_steps: int = 1) -> tuple[Any, Any]:
    parent = fg.ModelDescription(f"smoke_K1_parent_map_day_{int(map_day)}_steps_{parent_steps}")
    _add_parent_lifecycle_env(parent)
    parent_agent = _new_common_agent(parent, "parent_agent")

    child = fg.ModelDescription(f"smoke_K1_child_map_day_{int(map_day)}_steps_{parent_steps}")
    _add_lifecycle_env(child)
    child_agent = _new_common_agent(child, "child_agent")
    _add_child_noop(child_agent, child, f"k1_noop_{int(map_day)}_{parent_steps}")
    _add_increment_and_exit(child)

    sub = parent.newSubModel("child", child)
    sub.bindAgent("child_agent", "parent_agent", True, True)
    _map_lifecycle(sub, map_day=map_day)
    sub.setMaxSteps(0)

    sub_layer = parent.newLayer("child_submodel_layer")
    sub_layer.addSubModel("child")
    snap_layer = parent.newLayer("parent_snapshot_layer")
    snap_layer.addHostFunction(HFParentSnapshot())
    return parent, parent_agent


def k1_multistep_child() -> dict[str, Any]:
    model, agent = _build_k1_parent(map_day=True, parent_steps=1)
    sim = _make_sim(model, 1)
    _set_population(sim, agent)
    t0 = time.perf_counter()
    sim.simulate()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    child_steps = int(sim.getEnvironmentPropertyUInt("child_hf_calls"))
    final_day = int(sim.getEnvironmentPropertyUInt("child_day"))
    ok = child_steps == EXPECTED_CHILD_STEPS and final_day >= TARGET_DAY
    return {
        "pass": ok,
        "child_steps": child_steps,
        "final_day": final_day,
        "elapsed_ms": elapsed_ms,
        "measured": f"child_steps={child_steps}, final_day={final_day}, elapsed={elapsed_ms:.3f} ms",
        "expected": f"{EXPECTED_CHILD_STEPS} шагов, day>={TARGET_DAY}",
    }


def k2_step_counter_observable() -> dict[str, Any]:
    model, agent = _build_k1_parent(map_day=True, parent_steps=1)
    sim = _make_sim(model, 1)
    _set_population(sim, agent)
    sim.simulate()
    observed = int(sim.getEnvironmentPropertyUInt("child_steps_seen"))
    external_api = "not exposed on ModelDescription/SubModelDescription"
    ok = observed == EXPECTED_CHILD_STEPS
    return {
        "pass": ok,
        "child_steps_observed": observed,
        "external_child_get_step_counter": external_api,
        "measured": f"child_steps_observed={observed}; external={external_api}",
        "expected": str(EXPECTED_CHILD_STEPS),
    }


def _build_k3_parent() -> tuple[Any, Any, HFReadMacroSlots]:
    parent = fg.ModelDescription("smoke_K3_parent")
    _add_parent_lifecycle_env(parent)
    parent.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    parent_agent = _new_common_agent(parent, "parent_agent")

    child = fg.ModelDescription("smoke_K3_child")
    _add_lifecycle_env(child)
    child.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    child_agent = _new_common_agent(child, "child_agent")
    _add_child_writer(child_agent, child, "k3_writer")
    _add_increment_and_exit(child)

    sub = parent.newSubModel("child", child)
    sub.bindAgent("child_agent", "parent_agent", True, True)
    _map_lifecycle(sub)
    sub.SubEnvironment().mapMacroProperty("mp_shared", "mp_shared")
    sub.setMaxSteps(0)

    write_layer = parent.newLayer("parent_write_initial_macro")
    write_layer.addHostFunction(HFParentWriteInitialMacro())
    sub_layer = parent.newLayer("child_submodel_layer")
    sub_layer.addSubModel("child")
    slots = [i * DAY_INCREMENT for i in range(EXPECTED_CHILD_STEPS)]
    reader = HFReadMacroSlots(slots)
    read_layer = parent.newLayer("parent_read_macro_layer")
    read_layer.addHostFunction(reader)
    return parent, parent_agent, reader


def k3_macro_mapping_multistep() -> dict[str, Any]:
    model, agent, reader = _build_k3_parent()
    sim = _make_sim(model, 1)
    _set_population(sim, agent)
    sim.simulate()
    agent_vars = _read_agent_vars(sim, agent)
    expected = [i * DAY_INCREMENT for i in range(EXPECTED_CHILD_STEPS)]
    values = reader.values
    ok = values == expected and agent_vars["seen_initial"] == 999
    return {
        "pass": ok,
        "values": values,
        "seen_initial": agent_vars["seen_initial"],
        "measured": f"mp_shared values={values}, child_seen_initial={agent_vars['seen_initial']}",
        "expected": f"{expected}, initial=999",
    }


def _run_k4_variant(map_day: bool) -> dict[str, int]:
    model, agent = _build_k1_parent(map_day=map_day, parent_steps=2)
    sim = _make_sim(model, 2)
    _set_population(sim, agent)
    sim.simulate()
    snap1 = int(sim.getEnvironmentPropertyUInt("child_snap_1"))
    snap2 = int(sim.getEnvironmentPropertyUInt("child_snap_2"))
    day1 = int(sim.getEnvironmentPropertyUInt("child_day_snap_1"))
    day2 = int(sim.getEnvironmentPropertyUInt("child_day_snap_2"))
    return {
        "step1_delta": snap1,
        "step2_delta": snap2 - snap1,
        "step1_total": snap1,
        "step2_total": snap2,
        "day1": day1,
        "day2": day2,
    }


def k4_submodel_twice() -> dict[str, Any]:
    unmapped = _run_k4_variant(map_day=False)
    mapped = _run_k4_variant(map_day=True)
    finding = (
        "unmapped child_day re-init; mapped child_day persists via parent mapping "
        "and exit is evaluated after one child step"
    )
    ok = (
        unmapped["step1_delta"] == EXPECTED_CHILD_STEPS
        and unmapped["step2_delta"] == EXPECTED_CHILD_STEPS
        and mapped["step1_delta"] == EXPECTED_CHILD_STEPS
        and mapped["step2_delta"] == 1
    )
    return {
        "pass": ok,
        "unmapped": unmapped,
        "mapped": mapped,
        "finding": finding,
        "measured": (
            f"unmapped deltas={unmapped['step1_delta']}/{unmapped['step2_delta']}; "
            f"mapped deltas={mapped['step1_delta']}/{mapped['step2_delta']}"
        ),
        "expected": "unmapped 8/8; mapped 8/1 on rc4 post-step exit",
    }


def _build_k5_parent() -> tuple[Any, Any, HFReadMacroSlots]:
    parent = fg.ModelDescription("smoke_K5_parent")
    _add_parent_lifecycle_env(parent, "child_a")
    _add_parent_lifecycle_env(parent, "child_b", target_day=3)
    parent.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    parent_agent = _new_common_agent(parent, "parent_agent")

    child_a = fg.ModelDescription("smoke_K5_child_A")
    _add_lifecycle_env(child_a, "child_a")
    child_a.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    child_a_agent = _new_common_agent(child_a, "child_a_agent")
    _add_child_writer(child_a_agent, child_a, "k5_writer_a", "child_a")
    _add_increment_and_exit(child_a, "child_a")

    sub_a = parent.newSubModel("child_A", child_a)
    sub_a.bindAgent("child_a_agent", "parent_agent", True, True)
    _map_lifecycle(sub_a, "child_a")
    sub_a.SubEnvironment().mapMacroProperty("mp_shared", "mp_shared")
    sub_a.setMaxSteps(0)

    child_b = fg.ModelDescription("smoke_K5_child_B")
    _add_lifecycle_env(child_b, "child_b", target_day=3)
    child_b.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    child_b_agent = _new_common_agent(child_b, "child_b_agent")
    _add_child_reader(child_b_agent, child_b, "k5_reader_b")
    inc_layer = child_b.newLayer("child_b_increment_layer")
    inc_layer.addHostFunction(HFIncrementDay("child_b", 1))
    child_b.addExitCondition(ExitAtTarget("child_b"))

    sub_b = parent.newSubModel("child_B", child_b)
    sub_b.bindAgent("child_b_agent", "parent_agent", True, True)
    _map_lifecycle(sub_b, "child_b")
    sub_b.SubEnvironment().mapMacroProperty("mp_shared", "mp_shared")
    sub_b.setMaxSteps(0)

    layer_a = parent.newLayer("child_A_layer")
    layer_a.addSubModel("child_A")
    layer_b = parent.newLayer("child_B_layer")
    layer_b.addSubModel("child_B")
    slots = [i * DAY_INCREMENT for i in range(EXPECTED_CHILD_STEPS)]
    reader = HFReadMacroSlots(slots)
    read_layer = parent.newLayer("parent_read_after_children")
    read_layer.addHostFunction(reader)
    return parent, parent_agent, reader


def k5_multiple_submodels_barrier() -> dict[str, Any]:
    model, agent, reader = _build_k5_parent()
    sim = _make_sim(model, 1)
    _set_population(sim, agent)
    sim.simulate()
    agent_vars = _read_agent_vars(sim, agent)
    child_a_steps = int(sim.getEnvironmentPropertyUInt("child_a_hf_calls"))
    child_b_steps = int(sim.getEnvironmentPropertyUInt("child_b_hf_calls"))
    expected_values = [i * DAY_INCREMENT for i in range(EXPECTED_CHILD_STEPS)]
    ok = (
        reader.values == expected_values
        and agent_vars["b_seen_count"] == EXPECTED_CHILD_STEPS
        and child_a_steps == EXPECTED_CHILD_STEPS
        and child_b_steps == 3
    )
    return {
        "pass": ok,
        "child_a_steps": child_a_steps,
        "child_b_steps": child_b_steps,
        "child_b_seen_count": agent_vars["b_seen_count"],
        "values": reader.values,
        "measured": (
            f"child_A_steps={child_a_steps}, child_B_steps={child_b_steps}, "
            f"child_B_seen={agent_vars['b_seen_count']}/{EXPECTED_CHILD_STEPS}"
        ),
        "expected": "barrier yes; A=8, B=3",
    }


def _build_direct_model() -> tuple[Any, Any]:
    model = fg.ModelDescription("smoke_K6_direct")
    _add_lifecycle_env(model)
    model.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    agent = _new_common_agent(model, "direct_agent")
    _add_child_writer(agent, model, "k6_direct_writer")
    _add_increment_and_exit(model)
    return model, agent


def _build_submodel_model() -> tuple[Any, Any]:
    parent = fg.ModelDescription("smoke_K6_parent")
    _add_parent_lifecycle_env(parent)
    parent.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    parent_agent = _new_common_agent(parent, "parent_agent")

    child = fg.ModelDescription("smoke_K6_child")
    _add_lifecycle_env(child)
    child.Environment().newMacroPropertyUInt("mp_shared", MP_LEN)
    child_agent = _new_common_agent(child, "child_agent")
    _add_child_writer(child_agent, child, "k6_submodel_writer")
    _add_increment_and_exit(child)

    sub = parent.newSubModel("child", child)
    sub.bindAgent("child_agent", "parent_agent", True, True)
    _map_lifecycle(sub)
    sub.SubEnvironment().mapMacroProperty("mp_shared", "mp_shared")
    sub.setMaxSteps(0)
    layer = parent.newLayer("child_submodel_layer")
    layer.addSubModel("child")
    return parent, parent_agent


def _time_model(factory: Callable[[], tuple[Any, Any]], steps: int) -> float:
    model, agent = factory()
    sim = _make_sim(model, steps)
    _set_population(sim, agent)
    t0 = time.perf_counter()
    sim.simulate()
    return (time.perf_counter() - t0) * 1000.0


def k6_submodel_overhead() -> dict[str, Any]:
    _time_model(_build_direct_model, 100)
    _time_model(_build_submodel_model, 1)
    direct_samples = [_time_model(_build_direct_model, 100) for _ in range(5)]
    submodel_samples = [_time_model(_build_submodel_model, 1) for _ in range(5)]
    direct_ms = statistics.median(direct_samples)
    submodel_ms = statistics.median(submodel_samples)
    overhead_pct = ((submodel_ms - direct_ms) / direct_ms * 100.0) if direct_ms > 0 else math.inf
    ok = overhead_pct < 50.0
    return {
        "pass": ok,
        "direct_ms": direct_ms,
        "submodel_ms": submodel_ms,
        "overhead_pct": overhead_pct,
        "direct_samples": direct_samples,
        "submodel_samples": submodel_samples,
        "measured": f"direct={direct_ms:.3f} ms, submodel={submodel_ms:.3f} ms, overhead={overhead_pct:.1f}%",
        "expected": "overhead < 50%",
    }


def _pf(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _table(results: dict[str, dict[str, Any]]) -> str:
    k4 = results["K4"]
    k6 = results["K6"]
    lines = [
        "Проверка | Измерено                                      | Ожидаемо                         | PASS/FAIL",
        (
            f"K1       | child_steps={results['K1'].get('child_steps', 'N/A')}, "
            f"final_day={results['K1'].get('final_day', 'N/A')}, "
            f"{results['K1'].get('elapsed_ms', math.nan):.3f} ms"
            f" | {EXPECTED_CHILD_STEPS} шагов, day>={TARGET_DAY}          | {_pf(results['K1'].get('pass', False))}"
        ),
        (
            f"K2       | child_steps_observed={results['K2'].get('child_steps_observed', 'N/A')}"
            f"                  | {EXPECTED_CHILD_STEPS} через mapped env            | {_pf(results['K2'].get('pass', False))}"
        ),
        (
            f"K3       | mp_shared values={results['K3'].get('values', 'N/A')}"
            f" | [0,7,14,21,28,...] + initial=999 | {_pf(results['K3'].get('pass', False))}"
        ),
        (
            f"K4       | unmapped step1/step2="
            f"{k4.get('unmapped', {}).get('step1_delta', 'N/A')}/"
            f"{k4.get('unmapped', {}).get('step2_delta', 'N/A')}"
            f"                  | 8/8 если child_day re-init       | {_pf(k4.get('pass', False))}"
        ),
        (
            f"         | mapped step1/step2="
            f"{k4.get('mapped', {}).get('step1_delta', 'N/A')}/"
            f"{k4.get('mapped', {}).get('step2_delta', 'N/A')}"
            f"                    | 8/1 если mapped state persists  | finding"
        ),
        (
            f"         | finding={k4.get('finding', 'N/A')}"
            f" | факт rc4                         |"
        ),
        (
            f"K5       | child_B видит записи child_A: "
            f"{results['K5'].get('child_b_seen_count', 'N/A')}/{EXPECTED_CHILD_STEPS}; "
            f"A/B steps={results['K5'].get('child_a_steps', 'N/A')}/"
            f"{results['K5'].get('child_b_steps', 'N/A')}"
            f" | да; оба child до своих exit       | {_pf(results['K5'].get('pass', False))}"
        ),
        (
            f"K6       | direct_ms={k6.get('direct_ms', math.nan):.3f}, "
            f"submodel_ms={k6.get('submodel_ms', math.nan):.3f}"
            f"        | overhead < 50%                  | {_pf(k6.get('pass', False))}"
        ),
        (
            f"         | overhead={k6.get('overhead_pct', math.nan):.1f}%"
            f"                              |                                  |"
        ),
    ]
    return "\n".join(lines)


def _gate_verdict(results: dict[str, dict[str, Any]]) -> str:
    core = all(results[name].get("pass", False) for name in ("K1", "K2", "K3", "K5"))
    if not results["K1"].get("pass", False):
        return "FAIL"
    if core and results["K6"].get("pass", False):
        return "PASS"
    if core:
        return "PARTIAL"
    return "FAIL"


def _verdict(results: dict[str, dict[str, Any]]) -> str:
    gate = _gate_verdict(results)
    k4 = results["K4"]
    k6 = results["K6"]
    lines = [
        "Итоговый вердикт:",
        f"1. K1: многошаговый child submodel с setMaxSteps(0) и собственной exit condition {'реализуем' if results['K1'].get('pass') else 'не подтверждён'}.",
        f"2. K2: child step counter наблюдаем через mapped env-property; внешнего child.getStepCounter() API у SubModelDescription не видно.",
        f"3. K3: mapped MacroProperty {'накапливает' if results['K3'].get('pass') else 'не подтвердил накопление'} записи через много child-шагов.",
        f"4. K4: без mapping child_day re-init; с mapped child_day состояние сохраняется через parent, а exit проверяется после одного шага ({k4.get('measured', 'N/A')}).",
        f"5. K5: sequential child submodel barrier {'подтверждён' if results['K5'].get('pass') else 'не подтверждён'} для многошагового child_A -> child_B.",
        f"6. K6: overhead submodel vs direct = {k6.get('overhead_pct', math.nan):.1f}% ({'OK' if k6.get('pass') else 'выше порога/не измерен'}).",
        f"7. Главный вывод для D: {gate}.",
    ]
    return "\n".join(lines)


def main() -> int:
    experiments: list[tuple[str, Callable[[], dict[str, Any]]]] = [
        ("K1", k1_multistep_child),
        ("K2", k2_step_counter_observable),
        ("K3", k3_macro_mapping_multistep),
        ("K4", k4_submodel_twice),
        ("K5", k5_multiple_submodels_barrier),
        ("K6", k6_submodel_overhead),
    ]
    results = {name: _safe_run(name, fn) for name, fn in experiments}
    print(_table(results))
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
