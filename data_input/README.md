# Папка data_input/

## Назначение
Входные данные: выгрузки, мастер-данные, справочники.

## Структура

### master_data/
Мастер-данные для загрузки в СУБД:
- `MD_Сomponents.xlsx` - справочник компонентов
- `MD_Dictionary.xlsx` - словари и справочники

### source_data/
Исходные данные для расчетов:
- `Program.xlsx` - программные данные
- `Status_Overhaul.xlsx` - статусы капитального ремонта
- `Status_Components.xlsx` - статусы компонентов

### analytics/
Аналитические файлы:
- `Agents.xlsx` - данные для агентного моделирования
- `OLAP MultiBOM.xlsx` - MultiBOM аналитика 