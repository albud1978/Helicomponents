---
name: flame-rtc-guard
description: Guardrails for FLAME GPU / RTC sim runs — conda env, seatbelts, runbook paths.
---

# flame-rtc-guard

## Env (обязательно)

| Режим | Env | Интерпретатор |
|---|---|---|
| **Дефолт** (single-run, CUDAEnsemble, канон, замеры) | `cuda13_nosb` | `/home/albud/miniconda3/envs/cuda13_nosb/bin/python3` |
| Отладка device-assert | `cuda13` | `/home/albud/miniconda3/envs/cuda13/bin/python3` |

Решение Алексея 2026-07-23: `cuda13_nosb` — дефолт для **всех** sim-прогонов, не только ансамблей.

Перед запуском:

```bash
export CUDA_PATH="$HOME/miniconda3/targets/x86_64-linux"
export LD_LIBRARY_PATH="$HOME/miniconda3/lib:$LD_LIBRARY_PATH"
export AGENT_KG_WORKFLOW_ID=<активный workflow>
```

Запрещено: `python3` из `.venv`, `conda run -n cuda13*`, `source activate.sh && python3` для FLAME.

## Команды

SSoT: `docs/runbook_sim_launch.md`. Правила env: `.cursor/rules/15_flame_environment.mdc`.

## Sanity

```bash
/home/albud/miniconda3/envs/cuda13_nosb/bin/python3 -c \
  "import pyflamegpu as fg; print(fg.__version__, fg.SEATBELTS)"
# ожидаемо: 2.0.0rc4+cuda130 False
```
