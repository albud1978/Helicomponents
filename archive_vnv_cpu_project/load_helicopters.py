import os
import pandas as pd
from clickhouse_driver import Client
import logging
from datetime import datetime

# Настройки логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Настройки подключения к ClickHouse
clickhouse_host = '10.95.19.132'
clickhouse_port = 9000
clickhouse_user = 'default'
clickhouse_password = 'quie1ahpoo5Su0wohpaedae8keeph6bi'
database_name = 'default'
table_name = 'Helicopters'

# Путь к файлу Excel
excel_file = './Выгрузка ВС/Standard Table Report30140512042025 rev BAN clean.xlsx'

# Определение типов для известных столбцов
specified_columns = {
    'll': 'Nullable(Float32)',
    'll_ind': 'Nullable(String)',
    'oh': 'Nullable(Float32)',
    'oh_ind': 'Nullable(String)',
    'sne': 'Nullable(Float32)',
    'ppr': 'Nullable(Float32)',
    'shop_visit_counter': 'Nullable(Float32)',
    'mfg_date': 'Nullable(Date)',
    'oh_at_date': 'Nullable(Date)',
    'removal_date': 'Nullable(Date)',
    'repair_date': 'Nullable(Date)',
    'serialno': 'Nullable(UInt32)',
    'partno': 'Nullable(String)',
    'ac_typ': 'Nullable(String)',
    'location': 'Nullable(String)',
    'owner': 'Nullable(String)',
    'condition': 'Nullable(String)'
}

def main():
    try:
        # Проверка существования файла
        if not os.path.exists(excel_file):
            logging.error(f"Файл {excel_file} не найден")
            return
            
        # Загрузка данных из Excel
        logging.info(f"Загрузка данных из файла: {excel_file}")
        df = pd.read_excel(excel_file, engine='openpyxl')
        logging.info(f"Считаны столбцы: {list(df.columns)}")
        
        # Получение всех столбцов из Excel файла
        all_columns = df.columns.tolist()
        
        # Формирование списка столбцов с типами для ClickHouse
        columns = []
        for col in all_columns:
            if col in specified_columns:
                columns.append((col, specified_columns[col]))
            else:
                columns.append((col, 'Nullable(String)'))
        
        # Подключение к ClickHouse
        client = Client(host=clickhouse_host, port=clickhouse_port,
                       user=clickhouse_user, password=clickhouse_password)
        
        # Удаление существующей таблицы (если нужно)
        client.execute(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
        
        # Создание таблицы с явными типами данных
        fields = ', '.join([f"`{name}` {dtype}" for name, dtype in columns])
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            {fields}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        
        client.execute(create_table_query)
        logging.info(f"Создана таблица {table_name}")
        
        # Преобразование данных
        data = df.copy()
        
        # Обработка столбцов дат
        date_columns = ['mfg_date', 'oh_at_date', 'removal_date', 'repair_date']
        for col in date_columns:
            if col in data.columns:
                data[col] = pd.to_datetime(data[col], dayfirst=True, errors='coerce').dt.date
                data[col] = data[col].where(data[col].notnull(), None)
        
        # Обработка числовых столбцов
        float_columns = ['ll', 'oh', 'sne', 'ppr', 'shop_visit_counter', 'serialno']
        for col in float_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
                data[col] = data[col].where(data[col].notnull(), None)
        
        # Обработка строковых столбцов
        string_columns = [col for col in data.columns if col not in float_columns + date_columns]
        for col in string_columns:
            if col in data.columns:
                data[col] = data[col].astype(str).where(data[col].notnull(), None)
        
        # Заполнение пропущенных значений
        data.fillna(value=pd.NA, inplace=True)
        
        # Преобразование данных в список словарей
        records = data.to_dict('records')
        
        # Вставка данных в таблицу
        try:
            client.execute(f"INSERT INTO {database_name}.{table_name} VALUES", 
                          [tuple(record.values()) for record in records])
            logging.info(f"Загружено {len(records)} записей в таблицу {table_name}")
            logging.info("Данные успешно загружены в таблицу ClickHouse")
        except Exception as e:
            logging.error(f"Произошла ошибка при вставке данных: {e}", exc_info=True)
        
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}", exc_info=True)

if __name__ == "__main__":
    main() 