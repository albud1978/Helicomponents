import pyflamegpu
import sys
import time
import pandas as pd

# Константы симуляции
REPAIR_THRESHOLD_HOURS = 4500.0
REPAIR_DURATION_DAYS = 180
DAILY_FLIGHT_HOURS = 8.0
TARGET_TOTAL_HOURS = 30000.0
STATUS_FLYING = 0
STATUS_UNDER_REPAIR = 1

# --- Функции Агента (FLAME GPU Agent Function) ---

# Обратите внимание: этот код выполняется на GPU
process_day_func = r"""
FLAMEGPU_AGENT_FUNCTION(process_day, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Получаем константы из окружения
    const float repair_thresh = FLAMEGPU->environment.getProperty<float>("REPAIR_THRESHOLD_HOURS");
    const int repair_duration = FLAMEGPU->environment.getProperty<int>("REPAIR_DURATION_DAYS");
    const float daily_hours = FLAMEGPU->environment.getProperty<float>("DAILY_FLIGHT_HOURS");
    // Получаем текущие значения переменных агента
    float current_total_hours = FLAMEGPU->getVariable<float>("total_hours");
    float current_hours_since_repair = FLAMEGPU->getVariable<float>("hours_since_repair");
    int current_status = FLAMEGPU->getVariable<int>("status");
    int current_repair_days_left = FLAMEGPU->getVariable<int>("repair_days_left");
    unsigned int agent_id = FLAMEGPU->getID(); // Для логирования

    if (current_status == 0 /* STATUS_FLYING */) {
        current_total_hours += daily_hours;
        current_hours_since_repair += daily_hours;

        if (current_hours_since_repair >= repair_thresh) {
            // Пора на ремонт
            current_status = 1 /* STATUS_UNDER_REPAIR */;
            current_repair_days_left = repair_duration;
            current_hours_since_repair = 0.0f; // Сброс ППР
             printf("Step %u: Helicopter %u starting repair. Total hours: %.1f\n", FLAMEGPU->getStepCounter(), agent_id, current_total_hours);
        }
    } else { // status == 1 /* STATUS_UNDER_REPAIR */
        current_repair_days_left -= 1;

        if (current_repair_days_left <= 0) {
            // Ремонт завершен
            current_status = 0 /* STATUS_FLYING */;
             printf("Step %u: Helicopter %u finished repair. Total hours: %.1f\n", FLAMEGPU->getStepCounter(), agent_id, current_total_hours);
        }
    }

    // Сохраняем обновленные значения
    FLAMEGPU->setVariable<float>("total_hours", current_total_hours);
    FLAMEGPU->setVariable<float>("hours_since_repair", current_hours_since_repair);
    FLAMEGPU->setVariable<int>("status", current_status);
    FLAMEGPU->setVariable<int>("repair_days_left", current_repair_days_left);

    return flamegpu::ALIVE;
}
"""

# --- Основная часть скрипта ---

def main():
    # 1. Определяем модель FLAME GPU
    model = pyflamegpu.ModelDescription("Helicopter_Lifecycle_Test")

    # 2. Определяем окружение и его свойства (константы)
    env = model.Environment()
    env.newPropertyFloat("REPAIR_THRESHOLD_HOURS", REPAIR_THRESHOLD_HOURS)
    env.newPropertyInt("REPAIR_DURATION_DAYS", REPAIR_DURATION_DAYS)
    env.newPropertyFloat("DAILY_FLIGHT_HOURS", DAILY_FLIGHT_HOURS)

    # 3. Определяем агента "Helicopter"
    agent = model.newAgent("Helicopter")
    agent.newVariableFloat("total_hours", 0.0)
    agent.newVariableFloat("hours_since_repair", 0.0)
    agent.newVariableInt("status", STATUS_FLYING)
    agent.newVariableInt("repair_days_left", 0)

    # 4. Определяем функцию агента
    agent_func = agent.newRTCFunction("process_day", process_day_func)

    # 5. Определяем слои выполнения
    layer = model.newLayer("Default Layer")
    layer.addAgentFunction(agent_func)

    # 6. Создаем симуляцию CUDA
    cuda_simulation = pyflamegpu.CUDASimulation(model, sys.argv)
    # Устанавливаем максимальное количество шагов
    max_steps = 10000
    cuda_simulation.SimulationConfig().steps = max_steps
    cuda_simulation.SimulationConfig().verbose = False
    cuda_simulation.SimulationConfig().gpu_sync_printf = True

    # 8. Инициализируем популяцию агентов и свойства окружения
    cuda_simulation.setEnvironmentPropertyFloat("REPAIR_THRESHOLD_HOURS", REPAIR_THRESHOLD_HOURS)
    cuda_simulation.setEnvironmentPropertyInt("REPAIR_DURATION_DAYS", REPAIR_DURATION_DAYS)
    cuda_simulation.setEnvironmentPropertyFloat("DAILY_FLIGHT_HOURS", DAILY_FLIGHT_HOURS)

    initial_population_df = pd.DataFrame({
        'total_hours': [0.0],
        'hours_since_repair': [0.0],
        'status': [STATUS_FLYING],
        'repair_days_left': [0]
    })
    agent_vector = pyflamegpu.AgentVector(agent, initial_population_df.shape[0])
    for index, row in initial_population_df.iterrows():
        agent_instance = agent_vector[index]
        agent_instance.setVariableFloat("total_hours", float(row['total_hours']))
        agent_instance.setVariableFloat("hours_since_repair", float(row['hours_since_repair']))
        agent_instance.setVariableInt("status", int(row['status']))
        agent_instance.setVariableInt("repair_days_left", int(row['repair_days_left']))
    cuda_simulation.setPopulationData(agent_vector)

    # 9. Запускаем симуляцию в цикле Python
    print("Starting simulation...")
    start_time = time.time()
    final_df = None
    steps_taken = 0
    # Порог для следующего логирования (каждые 1000 часов налёта)
    next_log_hours_threshold = 1000.0
    # Создаем AgentVector для получения данных из симуляции
    agent_vector_out = pyflamegpu.AgentVector(agent)

    for step in range(max_steps):
        # Выполняем один шаг симуляции
        cuda_simulation.step()
        steps_taken = step + 1 # Учитываем текущий шаг

        # Получаем данные популяции в наш AgentVector
        cuda_simulation.getPopulationData(agent_vector_out)

        # Проверяем условие остановки и логируем
        # Работаем с agent_vector_out
        if agent_vector_out.size() > 0:
            # Доступ к переменным через AgentVector
            # Получаем первого (и единственного) агента
            agent_instance = agent_vector_out[0]
            # Получаем переменные у этого агента
            total_hours = agent_instance.getVariableFloat("total_hours")
            status_val = agent_instance.getVariableInt("status")
            hours_since_rep = agent_instance.getVariableFloat("hours_since_repair")

            # Логируем каждые 1000 часов налёта
            if total_hours >= next_log_hours_threshold:
                elapsed_now = time.time() - start_time
                print(
                    f"Step {steps_taken}: Total Hours: {total_hours:.1f}, Hours PPR: {hours_since_rep:.1f}, "
                    f"Status: {'FLYING' if status_val == STATUS_FLYING else 'REPAIR'}, "
                    f"Elapsed: {elapsed_now:.2f}s"
                )
                # Увеличиваем порог до следующей 1000-часовой отметки
                while total_hours >= next_log_hours_threshold:
                    next_log_hours_threshold += 1000.0

            # Проверяем условие выхода
            if total_hours >= TARGET_TOTAL_HOURS:
                print(f"\nTarget hours {TARGET_TOTAL_HOURS} reached at step {steps_taken}.")
                # Копируем данные из agent_vector_out в DataFrame вручную
                agent_data_list = []
                for ag_instance in agent_vector_out:
                    agent_data_list.append({
                        "id": ag_instance.getID(),
                        "total_hours": ag_instance.getVariableFloat("total_hours"),
                        "hours_since_repair": ag_instance.getVariableFloat("hours_since_repair"),
                        "status": ag_instance.getVariableInt("status"),
                        "repair_days_left": ag_instance.getVariableInt("repair_days_left")
                    })
                final_df = pd.DataFrame(agent_data_list)
                break # Выходим из цикла
        else:
             # Если агентов нет, тоже выходим
             print(f"No agents left at step {steps_taken}. Exiting.")
             break

    # Конец цикла симуляции
    end_time = time.time()
    elapsed = end_time - start_time
    steps_per_sec = steps_taken / elapsed if elapsed > 0 else float('inf')
    print(f"Simulation finished after {steps_taken} steps.")
    print(f"Simulation took {elapsed:.2f} seconds (~{steps_per_sec:.2f} steps/sec).")

    # 10. Выводим финальное состояние
    if final_df is not None:
         print("\nFinal Helicopter State:")
         print(final_df)
    elif agent_vector_out.size() > 0: # Если вышли по max_steps, используем agent_vector_out
         print("\nState at Max Steps:")
         # Копируем данные из agent_vector_out в DataFrame вручную
         agent_data_list = []
         for ag_instance in agent_vector_out:
             agent_data_list.append({
                 "id": ag_instance.getID(),
                 "total_hours": ag_instance.getVariableFloat("total_hours"),
                 "hours_since_repair": ag_instance.getVariableFloat("hours_since_repair"),
                 "status": ag_instance.getVariableInt("status"),
                 "repair_days_left": ag_instance.getVariableInt("repair_days_left")
             })
         print(pd.DataFrame(agent_data_list))
    else:
         print("\nNo final agent state available.")

    # 11. Очистка
    # cuda_simulation.release() # В Python обычно не требуется явно

if __name__ == "__main__":
    main() 