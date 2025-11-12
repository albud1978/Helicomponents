# Анализ слоёв и переменных (дни 823-825, динамический spawn)

## Таблица: RTC функции, слои и переменные

| # | RTC функция | Слой | День 823 | День 824 | День 825 |
|---|-------------|------|----------|----------|----------|
| **ВХОД** | - | Агенты в памяти | 285 агентов (idx 0-284)<br>115 в operations, intent=2 | 286 агентов (idx 0-285)<br>agent[285]: state=operations, intent=2<br>114 в operations, intent=2 | 287 агентов (idx 0-286)<br>agent[285,286]: state=operations, intent=2<br>115 в operations, intent=2 |
| 1 | `rtc_spawn_mgr` | `spawn_mgr_v2` | - | - | - |
| 2 | `rtc_spawn_ticket` | `spawn_ticket_v2` | - | - | - |
| 3 | `rtc_state_2_operations` | `state_2_operations` | 115 агентов:<br>intent=2 для 114<br>intent=4 для 1 | 114 агентов: intent=2<br>agent[285] ПРОПУЩЕН (фильтр state) | 115 агентов: intent=2<br>agent[286] ПРОПУЩЕН |
| 4 | `rtc_states_stub` | `states_stub_layer` | - | - | - |
| 5 | `rtc_reset_quota_buffers` | `reset_quota_buffers` | ops_count[*]=0<br>approve[*]=0<br>spawn_pending[*]=0 | ops_count[*]=0<br>approve[*]=0<br>spawn_pending[285]=0 | ops_count[*]=0<br>approve[*]=0<br>spawn_pending[286]=0 |
| 6 | `rtc_count_ops` | `count_ops` | Фильтр: state=ops AND intent=2<br>Подсчёт: 114<br>ops_count[0..284]=114 | Фильтр: state=ops AND intent=2<br>agent[285] ПРОПУЩЕН (фильтр state)<br>Подсчёт: 114<br>ops_count[0..284]=114 | Фильтр: state=ops AND intent=2<br>agent[286] ПРОПУЩЕН<br>Подсчёт: 115 |
| 7 | `rtc_count_serviceable` | `count_serviceable` | - | - | - |
| 8 | `rtc_count_reserve` | `count_reserve` | - | - | - |
| 9 | `rtc_count_inactive` | `count_inactive` | - | - | - |
| 10 | `rtc_quota_demount` | `quota_demount` | target[824]=115<br>Curr=114<br>Excess=0 | target[825]=115<br>Curr=114<br>Excess=0 | target[826]=115<br>Curr=115<br>Excess=0 |
| 11 | `rtc_quota_promote_serviceable` | `quota_promote_serviceable` | target[824]=115<br>Curr=114, Need=1<br>Нет serviceable<br>approve_s3[*]=0 | target[825]=115<br>Curr=114, Need=1<br>Нет serviceable<br>approve_s3[*]=0 | target[826]=115<br>Curr=115, Need=0<br>approve_s3[*]=0 |
| 12 | `rtc_quota_promote_reserve` | `quota_promote_reserve` | Need=1<br>Нет reserve<br>approve_s5[*]=0 | Need=1<br>Нет reserve<br>approve_s5[*]=0 | Need=0<br>approve_s5[*]=0 |
| 13 | `rtc_quota_promote_inactive` | `quota_promote_inactive` | Need=1<br>Нет inactive<br>approve_s1[*]=0 | Need=1<br>Нет inactive<br>approve_s1[*]=0 | Need=0<br>approve_s1[*]=0 |
| 14 | `rtc_spawn_dynamic_mgr` | `spawn_dynamic_mgr` | target[824]=115<br>Curr=114, Used=0<br>Deficit=1<br>Неактивен (day<repair_time) | target[825]=115<br>Curr=114, Used=0<br>spawn_pending[285]=0!<br>Deficit=1<br>Публикует: need[824]=1<br>idx=285, ACN=100006 | target[826]=115<br>Curr=115, Used=0<br>spawn_pending[286]=0!<br>Deficit=0 |
| 15 | `rtc_spawn_dynamic_ticket` | `spawn_dynamic_ticket` | need=0<br>Нет тикетов | need[824]=1<br>agent_out: idx=285<br>ACN=100006<br>state=operations<br>intent=2<br>spawn_pending[285]=1 | need=0<br>Нет тикетов |
| 16 | `rtc_apply_2_to_2` | `transition_2_to_2` | 114 агентов:<br>ops → ops | 114 агентов:<br>ops → ops | 115 агентов:<br>ops → ops |
| 17 | `rtc_apply_2_to_3` | `transition_2_to_3` | - | - | - |
| 18 | `rtc_apply_3_to_2` | `transition_3_to_2` | - | - | - |
| 19 | `rtc_apply_5_to_2` | `transition_5_to_2` | - | - | - |
| 20 | `rtc_apply_1_to_2` | `transition_1_to_2` | - | - | - |
| 21 | `rtc_apply_2_to_4` | `transition_2_to_4` | 1 агент:<br>ops → repair | - | - |
| 22 | `rtc_apply_2_to_6` | `transition_2_to_6` | - | - | - |
| 23 | `rtc_serviceable_holding_confirm` | `serviceable_holding_confirm` | - | - | - |
| 24 | `rtc_apply_1_to_1` | `transition_1_to_1` | - | - | - |
| 25 | `rtc_apply_4_to_4` | `transition_4_to_4` | - | - | - |
| 26 | `rtc_apply_4_to_5` | `transition_4_to_5` | - | - | - |
| 27 | `rtc_apply_5_to_5` | `transition_5_to_5` | - | - | - |
| 28 | `rtc_apply_6_to_6` | `transition_6_to_6` | - | - | - |
| 29 | `rtc_mp2_write_*` | `mp2_write_snapshot` | Запись в MP2:<br>114 в ops, intent=2 | Запись в MP2:<br>115 в ops, intent=2<br>(включая agent[285]) | Запись в MP2:<br>116 в ops, intent=2<br>(agent[285,286]) |
| 30 | `mp2_drain_host` | `mp2_final_drain` | - | - | - |

## Ключевая проблема

**День 824, слой 6 (`rtc_count_ops`):**
- Агент 285 (ACN=100006) существует в памяти с `state=operations, intent=2`
- НО: `rtc_count_ops` имеет фильтр `setInitialState("operations")` 
- Агент 285 **ПРОПУЩЕН** этим фильтром (не попадает в выборку)
- Результат: `mi17_ops_count` = 114 (без агента 285)

**День 824, слой 14 (`rtc_spawn_dynamic_mgr`):**
- Читает `mi17_ops_count` = 114
- Читает `mi17_spawn_pending[285]` = 0 (сброшен в слое 5!)
- Вычисляет: Deficit = 115 - 114 - 0 = **1**
- **ОШИБОЧНО создаёт агента 100007!**

## Вывод

Агент из `agent_out` не попадает в фильтр `setInitialState("operations")` в том же дне, когда был создан.

