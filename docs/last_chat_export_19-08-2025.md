# Экспорт чата от 19-08-2025

## Основные темы чата
- Перенос Transform на GPU (FLAME GPU 2)
- RTC слои и балансировка на сообщениях
- Пакетный экспорт MP2

## Решенные задачи
- Реализованы RTC (CUDA) для: repair, ops_check, main, change, pass
- Раннер переводится на однократную загрузку MP1/3/4/5 и day-by-day env
- Подготовлен каркас контроллера и сообщений (без лишних переменных)

## Проблемы и их решения
- HostFunction не имеет доступа к популяции → отказ от host-RTC в пользу newRTCFunction (CUDA)
- Исправлены вызовы env API (setEnvironmentProperty*) и AgentVector/setPopulationData

## Изменения в коде
- code/flame_gpu_helicopter_model.py — добавлены RTC-функции (newRTCFunction), агент controller, типы сообщений (каркас)
- code/flame_gpu_gpu_runner.py — предзагрузка MP4/MP5 в память, AgentVector, пакетный экспорт MP2

## Статус по FLAME GPU
- СДЕЛАНО: RTC для repair/ops_check/main/change/pass на GPU; исключён host-RTC
- В ПРОЦЕССЕ: балансировка на сообщениях (persist/add/cut) и controller_balance
- ПЛАН: завершить controller_balance и подключить слои; прогон 7 дней, валидация MP2

## Следующие шаги
- Реализовать publish_ops_persist, add_candidate_p1/p2/p3, cut_candidate и controller_balance
- Запустить GPU 7 дней; сверить инкременты sne/ppr и цели ops_counter_*
- Обновить документацию после прогона
