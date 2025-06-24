/**
 * Пример агента Flame GPU для системы AMOS
 * Демонстрация работы с оптимизированными данными компонентов
 */

#include "flamegpu/flamegpu.h"
#include <cuda_runtime.h>

// Структура данных компонента (точно соответствует ClickHouse схеме)
struct ComponentAgent {
    uint16_t partno_id;                    // 2 bytes - ID партномера
    uint32_t serialno_id;                  // 4 bytes - ID серийного номера
    uint8_t ac_type_mask;                  // 1 byte  - Битовая маска типов ВС
    uint16_t location_id;                  // 2 bytes - ID местоположения
    uint32_t oh;                          // 4 bytes - Наработка в минутах
    uint32_t oh_threshold;                // 4 bytes - Порог ТО в минутах
    uint8_t condition_mask;               // 1 byte  - Битовая маска состояния
    uint16_t interchangeable_group_id;    // 2 bytes - Группа взаимозаменяемых
    // Итого: 20 bytes per agent - идеально для GPU memory coalescing
};

// Битовые константы для типов ВС
constexpr uint8_t AC_TYPE_MI26   = 128;  // 0b10000000
constexpr uint8_t AC_TYPE_MI17   = 64;   // 0b01000000
constexpr uint8_t AC_TYPE_MI8T   = 32;   // 0b00100000
constexpr uint8_t AC_TYPE_KA32   = 16;   // 0b00010000
constexpr uint8_t AC_TYPE_AS350  = 8;    // 0b00001000
constexpr uint8_t AC_TYPE_AS355  = 4;    // 0b00000100
constexpr uint8_t AC_TYPE_R44    = 2;    // 0b00000010

// Битовые константы для состояний
constexpr uint8_t CONDITION_OPERATIONAL = 7;  // 0b111 - исправный
constexpr uint8_t CONDITION_BROKEN      = 4;  // 0b100 - неисправный
constexpr uint8_t CONDITION_NOT_INSTALLED = 6; // 0b110 - не установлен
constexpr uint8_t CONDITION_DONOR       = 1;  // 0b001 - донор

// Пороги для технического обслуживания (в минутах)
constexpr uint32_t CRITICAL_THRESHOLD = 6000;   // 100 часов
constexpr uint32_t WARNING_THRESHOLD  = 30000;  // 500 часов

/**
 * Агентная функция: мониторинг технического состояния
 * Каждый агент (компонент) проверяет свое состояние и генерирует сообщения
 */
FLAMEGPU_AGENT_FUNCTION(monitor_component_status, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
    // Получение данных агента
    const uint16_t partno_id = FLAMEGPU->getVariable<uint16_t>("partno_id");
    const uint32_t serialno_id = FLAMEGPU->getVariable<uint32_t>("serialno_id");
    const uint8_t ac_type_mask = FLAMEGPU->getVariable<uint8_t>("ac_type_mask");
    const uint16_t location_id = FLAMEGPU->getVariable<uint16_t>("location_id");
    const uint32_t oh = FLAMEGPU->getVariable<uint32_t>("oh");
    const uint32_t oh_threshold = FLAMEGPU->getVariable<uint32_t>("oh_threshold");
    const uint8_t condition_mask = FLAMEGPU->getVariable<uint8_t>("condition_mask");
    const uint16_t group_id = FLAMEGPU->getVariable<uint16_t>("interchangeable_group_id");
    
    // Вычисление оставшегося ресурса
    const uint32_t remaining_life = oh_threshold - oh;
    const float utilization_ratio = static_cast<float>(oh) / oh_threshold;
    
    // Определение уровня критичности (битовые операции)
    uint8_t urgency_level = 0;
    if (remaining_life < CRITICAL_THRESHOLD) {
        urgency_level = 4;  // 0b100 - критическое состояние
    } else if (remaining_life < WARNING_THRESHOLD) {
        urgency_level = 2;  // 0b010 - предупреждение
    } else {
        urgency_level = 1;  // 0b001 - нормальное состояние
    }
    
    // Проверка операционного состояния (битовые операции)
    const bool is_operational = (condition_mask & 0b100) != 0;
    const bool needs_maintenance = (condition_mask & 0b010) != 0;
    const bool counters_active = (condition_mask & 0b001) != 0;
    
    // Генерация сообщения о состоянии только для критических случаев
    if (urgency_level >= 2 || needs_maintenance) {
        FLAMEGPU->message_out.setVariable<uint16_t>("source_partno", partno_id);
        FLAMEGPU->message_out.setVariable<uint32_t>("source_serial", serialno_id);
        FLAMEGPU->message_out.setVariable<uint8_t>("ac_types", ac_type_mask);
        FLAMEGPU->message_out.setVariable<uint16_t>("location", location_id);
        FLAMEGPU->message_out.setVariable<uint32_t>("remaining_hours", remaining_life / 60);
        FLAMEGPU->message_out.setVariable<uint8_t>("urgency", urgency_level);
        FLAMEGPU->message_out.setVariable<uint16_t>("group", group_id);
        FLAMEGPU->message_out.setVariable<uint8_t>("operational", is_operational ? 1 : 0);
    }
    
    // Обновление внутреннего состояния агента
    FLAMEGPU->setVariable<uint8_t>("maintenance_urgency", urgency_level);
    FLAMEGPU->setVariable<float>("utilization_ratio", utilization_ratio);
    
    return flamegpu::ALIVE;
}

/**
 * Агентная функция: поиск взаимозаменяемых компонентов
 * Агенты ищут доступные замены в своей группе совместимости
 */
FLAMEGPU_AGENT_FUNCTION(find_replacements, flamegpu::MessageBruteForce, flamegpu::MessageSpatial2D) {
    const uint16_t my_partno = FLAMEGPU->getVariable<uint16_t>("partno_id");
    const uint8_t my_ac_types = FLAMEGPU->getVariable<uint8_t>("ac_type_mask");
    const uint16_t my_group = FLAMEGPU->getVariable<uint16_t>("interchangeable_group_id");
    const uint8_t my_urgency = FLAMEGPU->getVariable<uint8_t>("maintenance_urgency");
    const uint16_t my_location = FLAMEGPU->getVariable<uint16_t>("location_id");
    
    // Только компоненты, требующие замены, ищут альтернативы
    if (my_urgency < 2) {
        return flamegpu::ALIVE;  // Нет срочной необходимости в замене
    }
    
    uint16_t best_replacement = 0;
    uint32_t best_remaining_life = 0;
    uint16_t best_location = 0;
    uint16_t replacements_found = 0;
    
    // Поиск среди всех сообщений о состоянии
    for (const auto &message : FLAMEGPU->message_in) {
        const uint16_t candidate_partno = message.getVariable<uint16_t>("source_partno");
        const uint8_t candidate_ac_types = message.getVariable<uint8_t>("ac_types");
        const uint16_t candidate_group = message.getVariable<uint16_t>("group");
        const uint32_t candidate_remaining = message.getVariable<uint32_t>("remaining_hours");
        const uint16_t candidate_location = message.getVariable<uint16_t>("location");
        const uint8_t candidate_operational = message.getVariable<uint8_t>("operational");
        
        // Проверка совместимости (битовые операции)
        const bool ac_compatible = (my_ac_types & candidate_ac_types) != 0;
        const bool group_compatible = (my_group == candidate_group) || (candidate_group == 0);
        const bool is_available = candidate_operational == 1;
        const bool has_life_remaining = candidate_remaining > 1000; // > ~17 часов
        
        if (ac_compatible && group_compatible && is_available && has_life_remaining) {
            replacements_found++;
            
            // Выбор лучшей замены (наибольший остаток ресурса)
            if (candidate_remaining > best_remaining_life) {
                best_replacement = candidate_partno;
                best_remaining_life = candidate_remaining;
                best_location = candidate_location;
            }
        }
    }
    
    // Генерация пространственного сообщения с результатами поиска
    if (replacements_found > 0) {
        // Используем location_id как координаты для пространственного поиска
        const float x = static_cast<float>(my_location % 1000);  // Условные координаты
        const float y = static_cast<float>(my_location / 1000);
        
        FLAMEGPU->message_out.setLocation(x, y);
        FLAMEGPU->message_out.setVariable<uint16_t>("requesting_partno", my_partno);
        FLAMEGPU->message_out.setVariable<uint16_t>("best_replacement", best_replacement);
        FLAMEGPU->message_out.setVariable<uint32_t>("replacement_life", best_remaining_life);
        FLAMEGPU->message_out.setVariable<uint16_t>("replacement_location", best_location);
        FLAMEGPU->message_out.setVariable<uint16_t>("total_options", replacements_found);
        FLAMEGPU->message_out.setVariable<uint8_t>("urgency_level", my_urgency);
    }
    
    // Обновление состояния агента
    FLAMEGPU->setVariable<uint16_t>("available_replacements", replacements_found);
    FLAMEGPU->setVariable<uint16_t>("best_replacement_partno", best_replacement);
    
    return flamegpu::ALIVE;
}

/**
 * Агентная функция: планирование технического обслуживания
 * Обработка запросов на замену и планирование работ
 */
FLAMEGPU_AGENT_FUNCTION(schedule_maintenance, flamegpu::MessageSpatial2D, flamegpu::MessageNone) {
    const uint16_t my_partno = FLAMEGPU->getVariable<uint16_t>("partno_id");
    const uint16_t my_location = FLAMEGPU->getVariable<uint16_t>("location_id");
    const uint8_t my_condition = FLAMEGPU->getVariable<uint8_t>("condition_mask");
    
    // Только исправные компоненты могут быть использованы как замена
    const bool can_be_replacement = (my_condition & 0b100) != 0;  // операционный
    if (!can_be_replacement) {
        return flamegpu::ALIVE;
    }
    
    uint16_t maintenance_requests = 0;
    uint8_t highest_urgency = 0;
    
    // Координаты для пространственного поиска
    const float my_x = static_cast<float>(my_location % 1000);
    const float my_y = static_cast<float>(my_location / 1000);
    
    // Поиск запросов на замену в радиусе (пространственный поиск)
    for (const auto &message : FLAMEGPU->message_in(my_x, my_y)) {
        const uint16_t requested_partno = message.getVariable<uint16_t>("best_replacement");
        const uint8_t request_urgency = message.getVariable<uint8_t>("urgency_level");
        const uint16_t requester_location = message.getVariable<uint16_t>("replacement_location");
        
        // Проверка, что запрос относится к этому компоненту
        if (requested_partno == my_partno) {
            maintenance_requests++;
            
            if (request_urgency > highest_urgency) {
                highest_urgency = request_urgency;
            }
        }
    }
    
    // Обновление приоритета в зависимости от запросов
    if (maintenance_requests > 0) {
        FLAMEGPU->setVariable<uint8_t>("replacement_priority", highest_urgency);
        FLAMEGPU->setVariable<uint16_t>("pending_requests", maintenance_requests);
        
        // Логирование для анализа
        // В реальной системе здесь может быть вызов host function для записи в лог
    }
    
    return flamegpu::ALIVE;
}

/**
 * Host function: сбор статистики симуляции
 */
FLAMEGPU_HOST_FUNCTION(collect_simulation_stats) {
    // Получение доступа к агентам
    auto agent_data = FLAMEGPU->agent("Component");
    
    // Подсчет статистики по срочности
    uint32_t critical_count = 0;
    uint32_t warning_count = 0;
    uint32_t normal_count = 0;
    uint32_t total_replacements_available = 0;
    
    // Агрегация данных на GPU
    for (auto agent : agent_data) {
        uint8_t urgency = agent.getVariable<uint8_t>("maintenance_urgency");
        uint16_t replacements = agent.getVariable<uint16_t>("available_replacements");
        
        switch (urgency) {
            case 4: critical_count++; break;
            case 2: warning_count++; break;
            default: normal_count++; break;
        }
        
        total_replacements_available += replacements;
    }
    
    // Логирование статистики
    printf("=== Simulation Step Statistics ===\n");
    printf("Critical components: %u\n", critical_count);
    printf("Warning components: %u\n", warning_count);
    printf("Normal components: %u\n", normal_count);
    printf("Total replacement options: %u\n", total_replacements_available);
    printf("====================================\n");
    
    // Установка environment переменных для следующего шага
    FLAMEGPU->environment.setProperty<uint32_t>("critical_components", critical_count);
    FLAMEGPU->environment.setProperty<uint32_t>("warning_components", warning_count);
    FLAMEGPU->environment.setProperty<uint32_t>("replacement_options", total_replacements_available);
}

/**
 * Основная функция настройки модели
 */
int main(int argc, const char** argv) {
    // Создание модели
    flamegpu::ModelDescription model("AMOS_Component_Management");
    
    // Определение агента компонента
    flamegpu::AgentDescription agent = model.newAgent("Component");
    
    // Переменные агента (точно соответствуют ClickHouse схеме)
    agent.newVariable<uint16_t>("partno_id");
    agent.newVariable<uint32_t>("serialno_id");
    agent.newVariable<uint8_t>("ac_type_mask");
    agent.newVariable<uint16_t>("location_id");
    agent.newVariable<uint32_t>("oh");
    agent.newVariable<uint32_t>("oh_threshold");
    agent.newVariable<uint8_t>("condition_mask");
    agent.newVariable<uint16_t>("interchangeable_group_id");
    
    // Дополнительные переменные для симуляции
    agent.newVariable<uint8_t>("maintenance_urgency", 1);
    agent.newVariable<float>("utilization_ratio", 0.0f);
    agent.newVariable<uint16_t>("available_replacements", 0);
    agent.newVariable<uint16_t>("best_replacement_partno", 0);
    agent.newVariable<uint8_t>("replacement_priority", 0);
    agent.newVariable<uint16_t>("pending_requests", 0);
    
    // Определение сообщений
    flamegpu::MessageBruteForce::Description status_message = 
        model.newMessage<flamegpu::MessageBruteForce>("ComponentStatus");
    status_message.newVariable<uint16_t>("source_partno");
    status_message.newVariable<uint32_t>("source_serial");
    status_message.newVariable<uint8_t>("ac_types");
    status_message.newVariable<uint16_t>("location");
    status_message.newVariable<uint32_t>("remaining_hours");
    status_message.newVariable<uint8_t>("urgency");
    status_message.newVariable<uint16_t>("group");
    status_message.newVariable<uint8_t>("operational");
    
    flamegpu::MessageSpatial2D::Description replacement_message = 
        model.newMessage<flamegpu::MessageSpatial2D>("ReplacementRequest");
    replacement_message.setMin(0, 0);
    replacement_message.setMax(1000, 1000);  // Координатное пространство локаций
    replacement_message.setRadius(50.0f);    // Радиус поиска
    replacement_message.newVariable<uint16_t>("requesting_partno");
    replacement_message.newVariable<uint16_t>("best_replacement");
    replacement_message.newVariable<uint32_t>("replacement_life");
    replacement_message.newVariable<uint16_t>("replacement_location");
    replacement_message.newVariable<uint16_t>("total_options");
    replacement_message.newVariable<uint8_t>("urgency_level");
    
    // Определение функций агентов
    auto monitor_fn = agent.newFunction("monitor_component_status", monitor_component_status);
    monitor_fn.setMessageOutput("ComponentStatus");
    
    auto find_fn = agent.newFunction("find_replacements", find_replacements);
    find_fn.setMessageInput("ComponentStatus");
    find_fn.setMessageOutput("ReplacementRequest");
    
    auto schedule_fn = agent.newFunction("schedule_maintenance", schedule_maintenance);
    schedule_fn.setMessageInput("ReplacementRequest");
    
    // Environment переменные
    auto env = model.Environment();
    env.newProperty<uint32_t>("critical_components", 0);
    env.newProperty<uint32_t>("warning_components", 0);
    env.newProperty<uint32_t>("replacement_options", 0);
    env.newProperty<uint32_t>("simulation_step", 0);
    
    // Определение порядка выполнения (слои)
    auto layer1 = model.newLayer();
    layer1.addAgentFunction(monitor_fn);
    
    auto layer2 = model.newLayer();
    layer2.addAgentFunction(find_fn);
    
    auto layer3 = model.newLayer();
    layer3.addAgentFunction(schedule_fn);
    layer3.addHostFunction(collect_simulation_stats);
    
    // Создание симуляции
    flamegpu::CUDASimulation simulation(model, argc, argv);
    
    // Загрузка начальных данных агентов из подготовленных файлов
    // (В реальной системе данные загружаются из numpy файлов, созданных Python модулем)
    
    printf("🔥 AMOS Flame GPU Simulation initialized\n");
    printf("📊 Ready to load agent data from cuDF integration\n");
    printf("⚡ Optimized for GPU memory layout: 20 bytes per agent\n");
    
    // Запуск симуляции
    simulation.simulate();
    
    return 0;
} 