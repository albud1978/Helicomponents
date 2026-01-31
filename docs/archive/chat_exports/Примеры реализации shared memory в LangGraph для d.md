<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Примеры реализации shared memory в LangGraph для dev агентов

В LangGraph shared memory для dev‑агентов — это по сути **общий graph‑state**, к которому имеют доступ все узлы (агенты), плюс долгосрочная память поверх него. Приведу типичные практики и примеры именно для сценариев разработки.[^1][^2][^3][^4]

***

## 1. Общий graph‑state как shared memory

В LangGraph вся мультиагентная система крутится вокруг **единого объекта состояния** (TypedDict / Pydantic‑модель), который «протекает» через вершины графа.[^4][^1]

Для dev‑агентов этот state обычно включает:

- `messages` / `conversation` — история диалога и reasoning.
- `plan` / `tasks` — текущий план работ и подзадачи.
- `files` / `diffs` — какие файлы трогаем, какие изменения предложены.
- `tool_outputs` / `tests` — результаты запуска инструментов (линтеры, тесты, CI).

Ключевая вещь — **reducers**: каждое поле Annotated с функцией слияния (`add`, свой редьюсер), чтобы несколько агентов могли безопасно писать в одну и ту же память (например, дописывать логи, добавлять подзадачи).[^2]

Это и есть shared memory:

- Planner‑агент пишет в `plan` и `tasks`.
- Coder‑агент добавляет `diffs` и `tool_outputs`.
- Reviewer‑агент — замечания и статусы.
Все они читают один и тот же state, который последовательно обновляется.

***

## 2. Краткоживущая shared memory (short‑term) для команды агентов

Практики из гайдов и разборов LangGraph:[^5][^2][^4]

- **Short‑term память** — это сам graph‑state в рамках одного «run» или одного thread (session).
- В мультиагентной разработке обычно заводят state‑класс типа:
    - `messages`: список шагов reasoning и диалога;
    - `work_items`: список задач/файлов, над которыми сейчас работает команда агентов;
    - `artifacts`: артефакты (планы, схемы, диффы) как объекты.

Каждый агент‑узел:

- принимает state;
- читает только интересующие ему поля (например, coder смотрит `plan` и `work_items`);
- возвращает **частичное обновление** state (например, добавляет элемент в `artifacts` и новый шаг в `messages`).[^2]

Reducers обеспечивают:

- `messages` → аккумулируется история (append);
- `work_items` → дописываются/обновляются задачи;
- `status` → последний агент может перезаписать (overwrite).[^2]

В параллельных ветках (несколько агентов одновременно) редьюсеры определяют, как объединить их обновления — это core‑механизм shared memory в LangGraph.[^4][^2]

***

## 3. Долговременная shared memory (long‑term) между сессиями

LangGraph добавил отдельную поддержку **long‑term memory**, которая особенно важна для dev‑агентов:[^3][^6][^4]

- Краткоживущая память (state) чекпоинтится как **thread state**;
- Long‑term хранится в внешнем сторе (БД, вектор‑хранилище, KV) и связывается с thread’ами/проектами.

Типичные примеры для dev‑сценариев:

- запоминать **принятые архитектурные решения** и резюме прошлых задач;
- хранить историю работы с конкретным репозиторием (важные файлы, контракты, паттерны);
- конфигурировать агентов (их роли и привычное поведение) через persistent‑memory.[^6][^4]

Практика:

- Short‑term state графа → источник «сыра» (деталей шагов).
- После завершения работы специальный узел‑summarizer сжимает это в **long‑term запись** (summary, ADR, lessons learned) и пишет в долговременное хранилище.[^6][^4]
- В следующих сессиях dev‑агенты по ключу проекта/репо подтягивают релевантную long‑term память (например, «как мы обычно конфигурируем CI здесь»).[^6]

***

## 4. Сценарии shared memory именно для dev‑агентов

Судя по туториалам и статьям по LangGraph для мультиагентных систем, используются несколько устойчивых шаблонов:[^7][^5][^4]

1. **Planner + Coder + Reviewer с общим state.**
    - Planner‑узел создаёт `plan` и `work_items`.
    - Несколько Coder‑узлов (можно параллельно) по очереди берут задачи из `work_items`, дописывают `diffs` и `tool_outputs`.
    - Reviewer‑узел читает `plan`, `diffs`, `tool_outputs` и пишет `review_comments` и итоговый статус.
2. **Supervisor + под‑графы.**
    - Supervisor‑граф (агент) решает, какой под‑граф агентов вызвать (бэкенд, фронтенд, тесты).
    - Все под‑графы работают с одним глобальным state, но смотрят только нужные поля:
        - бэкенд‑подграф — свои `backend_tasks`, `backend_diffs`;
        - фронтенд — свои;
        - тестовый — общие `tool_outputs` и `test_statuses`.[^8][^9][^1]
3. **Task‑centric shared memory.**
    - В state есть коллекция задач (issues/PR/тикеты); каждый узел‑агент обогащает **конкретную задачу**: описание, план, изменения, результаты тестов.
    - Shared memory выступает как «карта задач» для всей команды агентов.[^10][^8]

***

## 5. На что смотреть, если хочешь «правильную» shared memory в LangGraph под разработку

Сводка best‑practice из статей и дискуссий:[^8][^5][^4][^2]

- **Чётко спроектировать State‑тип.**
    - Не валить всё в один `dict`; завести структурированный TypedDict с ключами под твои роли (plan, tasks, diffs, reviews, tools).
    - Для каждого поля решить: append/merge/overwrite (reducers).
- **Разделять short‑term и long‑term.**
    - Short‑term — execution state в графе;
    - Long‑term — отдельное хранилище (решения, project‑knowledge), подключаемое узлами (особенно planner’ом и reviewer’ом).
- **Использовать reducers для параллельных агентов.**
    - Если несколько dev‑агентов могут одновременно писать в `logs`, `messages`, `tasks`, обязательно поставить редьюсер (append/merge), чтобы не терять обновления.
- **Поддерживать thread‑scoped чекпоинты.**
    - Для длинных задач (дни/недели разработки) использовать checkpointing: можно остановить/возобновить работу команды агентов без потери общих памяти state.[^3][^4]
- **Явно документировать структуру памяти.**
    - В проекте иметь документ «State schema» (по сути, контракт общей памяти), чтобы при добавлении новых агентов не приходилось гадать, какие ключи есть и как их интерпретировать.

Если нужно, дальше могу:

- разобрать **конкретную схему State** под твой сценарий (Planner / PBM‑Coder / Sim‑Runner / Reviewer) именно в терминах полей shared memory LangGraph;
- или пройтись по паттерну «Supervisor + под‑графы» и как там организуют общую память для разных веток разработки.
<span style="display:none">[^11][^12][^13][^14][^15][^16]</span>

<div align="center">⁂</div>

[^1]: https://langchain-ai.github.io/langgraph/concepts/multi_agent/

[^2]: https://dev.to/sreeni5018/the-architecture-of-agent-memory-how-langgraph-really-works-59ne

[^3]: https://docs.langchain.com/oss/python/langgraph/memory

[^4]: https://langchain-ai.github.io/langgraph/concepts/memory/

[^5]: https://dev.to/jamiu__tijani/implementing-langgraph-for-multi-agent-ai-systems-4fck

[^6]: https://blog.langchain.dev/launching-long-term-memory-support-in-langgraph/

[^7]: https://aws.amazon.com/blogs/machine-learning/build-multi-agent-systems-with-langgraph-and-amazon-bedrock/

[^8]: https://www.reddit.com/r/LangGraph/comments/1n867pe/managing_shared_state_in_langgraph_multiagent/

[^9]: https://github.com/langchain-ai/langgraph/discussions/2318

[^10]: https://www.vellum.ai/blog/multi-agent-systems-building-with-context-engineering

[^11]: https://github.com/langchain-ai/langgraph/discussions/1821

[^12]: https://www.reddit.com/r/LangChain/comments/1fnh7mh/how_can_i_setup_memory_for_my_multi_agent_system/

[^13]: https://www.reddit.com/r/AI_Agents/comments/1lj7fy1/custom_memory_configuration_using_multiagent/

[^14]: https://docs.langchain.com/oss/python/langgraph/graph-api

[^15]: https://docs.langchain.com/oss/javascript/langgraph/memory

[^16]: https://langchain-ai.github.io/langgraph/concepts/low_level/
