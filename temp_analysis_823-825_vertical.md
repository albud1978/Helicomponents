# Анализ слоёв и переменных (дни 823-825, динамический spawn)

## День 823

| # | RTC функция | Слой | Переменные и действия |
|---|-------------|------|-----------------------|
| ВХОД | - | Агенты в памяти | 285 агентов (idx 0-284), 115 в operations, intent=2 |
| 1 | `rtc_spawn_mgr` | `spawn_mgr_v2` | - |
| 2 | `rtc_spawn_ticket` | `spawn_ticket_v2` | - |
| 3 | `rtc_state_2_operations` | `state_2_operations` | 115 агентов: intent=2 для 114, intent=4 для 1 |
| 4 | `rtc_states_stub` | `states_stub_layer` | - |
| 5 | `rtc_reset_quota_buffers` | `reset_quota_buffers` | ops_count[*]=0, approve[*]=0, spawn_pending[*]=0 |
| 6 | `rtc_count_ops` | `count_ops` | Фильтр: state=ops AND intent=2, Подсчёт: 114, ops_count[0..284]=114 |
| 7 | `rtc_count_serviceable` | `count_serviceable` | - |
| 8 | `rtc_count_reserve` | `count_reserve` | - |
| 9 | `rtc_count_inactive` | `count_inactive` | - |
| 10 | `rtc_quota_demount` | `quota_demount` | target[824]=115, Curr=114, Excess=0 |
| 11 | `rtc_quota_promote_serviceable` | `quota_promote_serviceable` | target[824]=115, Curr=114, Need=1, Нет serviceable, approve_s3[*]=0 |
| 12 | `rtc_quota_promote_reserve` | `quota_promote_reserve` | Need=1, Нет reserve, approve_s5[*]=0 |
| 13 | `rtc_quota_promote_inactive` | `quota_promote_inactive` | Need=1, Нет inactive, approve_s1[*]=0 |
| 14 | `rtc_spawn_dynamic_mgr` | `spawn_dynamic_mgr` | target[824]=115, Curr=114, Used=0, **Deficit=1**, Публикует: need[823]=1, idx=285, ACN=100006 |
| 15 | `rtc_spawn_dynamic_ticket` | `spawn_dynamic_ticket` | need[823]=1, **agent_out: idx=285, ACN=100006, state=operations, intent=2, spawn_pending[285]=1** ✅ |
| 16 | `rtc_apply_2_to_2` | `transition_2_to_2` | 114 агентов: ops → ops |
| 21 | `rtc_apply_2_to_4` | `transition_2_to_4` | 1 агент: ops → repair |
| 29 | `rtc_mp2_write_*` | `mp2_write_snapshot` | Запись в MP2: 114 в ops, intent=2 |

---

## День 824

| # | RTC функция | Слой | Переменные и действия |
|---|-------------|------|-----------------------|
| ВХОД | - | Агенты в памяти | 286 агентов (idx 0-285), **agent[285]: state=operations, intent=2**, 114 в operations, intent=2 |
| 1 | `rtc_spawn_mgr` | `spawn_mgr_v2` | - |
| 2 | `rtc_spawn_ticket` | `spawn_ticket_v2` | - |
| 3 | `rtc_state_2_operations` | `state_2_operations` | 114 агентов: intent=2, **agent[285] ПРОПУЩЕН (фильтр state)** |
| 4 | `rtc_states_stub` | `states_stub_layer` | - |
| 5 | `rtc_reset_quota_buffers` | `reset_quota_buffers` | ops_count[*]=0, approve[*]=0, **spawn_pending[285]=0** ❌ |
| 6 | `rtc_count_ops` | `count_ops` | Фильтр: state=ops AND intent=2, **agent[285] ПРОПУЩЕН (фильтр state)** ❌, Подсчёт: 114, ops_count[0..284]=114 |
| 7 | `rtc_count_serviceable` | `count_serviceable` | - |
| 8 | `rtc_count_reserve` | `count_reserve` | - |
| 9 | `rtc_count_inactive` | `count_inactive` | - |
| 10 | `rtc_quota_demount` | `quota_demount` | target[825]=115, Curr=114, Excess=0 |
| 11 | `rtc_quota_promote_serviceable` | `quota_promote_serviceable` | target[825]=115, Curr=114, Need=1, Нет serviceable, approve_s3[*]=0 |
| 12 | `rtc_quota_promote_reserve` | `quota_promote_reserve` | Need=1, Нет reserve, approve_s5[*]=0 |
| 13 | `rtc_quota_promote_inactive` | `quota_promote_inactive` | Need=1, Нет inactive, approve_s1[*]=0 |
| 14 | `rtc_spawn_dynamic_mgr` | `spawn_dynamic_mgr` | target[825]=115, Curr=114, Used=0, **spawn_pending[285]=0!** ❌, **Deficit=1** ❌, Публикует: need[824]=1, idx=285, ACN=100006 |
| 15 | `rtc_spawn_dynamic_ticket` | `spawn_dynamic_ticket` | need[824]=1, **agent_out: idx=285, ACN=100006, state=operations, intent=2, spawn_pending[285]=1** ✅ |
| 16 | `rtc_apply_2_to_2` | `transition_2_to_2` | 114 агентов: ops → ops |
| 29 | `rtc_mp2_write_*` | `mp2_write_snapshot` | Запись в MP2: 115 в ops, intent=2 (включая agent[285]) |

---

## День 825

| # | RTC функция | Слой | Переменные и действия |
|---|-------------|------|-----------------------|
| ВХОД | - | Агенты в памяти | 287 агентов (idx 0-286), **agent[285,286]: state=operations, intent=2**, 115 в operations, intent=2 |
| 1 | `rtc_spawn_mgr` | `spawn_mgr_v2` | - |
| 2 | `rtc_spawn_ticket` | `spawn_ticket_v2` | - |
| 3 | `rtc_state_2_operations` | `state_2_operations` | 115 агентов: intent=2, **agent[286] ПРОПУЩЕН** |
| 4 | `rtc_states_stub` | `states_stub_layer` | - |
| 5 | `rtc_reset_quota_buffers` | `reset_quota_buffers` | ops_count[*]=0, approve[*]=0, **spawn_pending[286]=0** ❌ |
| 6 | `rtc_count_ops` | `count_ops` | Фильтр: state=ops AND intent=2, **agent[286] ПРОПУЩЕН** ❌, Подсчёт: 115 |
| 7 | `rtc_count_serviceable` | `count_serviceable` | - |
| 8 | `rtc_count_reserve` | `count_reserve` | - |
| 9 | `rtc_count_inactive` | `count_inactive` | - |
| 10 | `rtc_quota_demount` | `quota_demount` | target[826]=115, Curr=115, Excess=0 |
| 11 | `rtc_quota_promote_serviceable` | `quota_promote_serviceable` | target[826]=115, Curr=115, Need=0, approve_s3[*]=0 |
| 12 | `rtc_quota_promote_reserve` | `quota_promote_reserve` | Need=0, approve_s5[*]=0 |
| 13 | `rtc_quota_promote_inactive` | `quota_promote_inactive` | Need=0, approve_s1[*]=0 |
| 14 | `rtc_spawn_dynamic_mgr` | `spawn_dynamic_mgr` | target[826]=115, Curr=115, Used=0, **spawn_pending[286]=0!** ❌, Deficit=0 ✅ |
| 15 | `rtc_spawn_dynamic_ticket` | `spawn_dynamic_ticket` | need=0, Нет тикетов |
| 16 | `rtc_apply_2_to_2` | `transition_2_to_2` | 115 агентов: ops → ops |
| 29 | `rtc_mp2_write_*` | `mp2_write_snapshot` | Запись в MP2: 116 в ops, intent=2 (agent[285,286]) |

---

## Ключевая проблема

**День 824, слой 6 (`rtc_count_ops`):**
- Агент 285 (ACN=100006) существует в памяти с `state=operations, intent=2`
- НО: `rtc_count_ops` имеет фильтр `setInitialState("operations")` 
- Агент 285 **ПРОПУЩЕН** этим фильтром (не попадает в выборку)
- Результат: `mi17_ops_count` = 114 (без агента 285)

**День 824, слой 5 (`rtc_reset_quota_buffers`):**
- Сбрасывает `spawn_pending[285]=0`
- Агент 285 был создан в день 823, но флаг уже сброшен!

**День 824, слой 14 (`rtc_spawn_dynamic_mgr`):**
- Читает `mi17_ops_count` = 114
- Читает `spawn_pending[285]` = 0 (сброшен в слое 5!)
- Вычисляет: Deficit = 115 - 114 - 0 = **1**
- **ОШИБОЧНО создаёт агента 100007!**

## Вывод

**Проблема:** Агент из `agent_out` не попадает в фильтр `setInitialState("operations")` в том же дне, когда был создан.

**Решение:** Убрать сброс `spawn_pending` из `rtc_reset_quota_buffers` → сбрасывать в `rtc_spawn_dynamic_mgr` ПОСЛЕ чтения.

