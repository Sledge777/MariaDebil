import mysql.connector
import pandas as pd
from decimal import Decimal

# Подключение к базе данных
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

# Функция для создания таблицы plane_ms_phone
def create_plane_ms_phone(cursor, conn):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plane_ms_phone (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store TEXT,
            total_net DECIMAL(14,2),
            На_связи_quantity DECIMAL(12,3),
            На_связи_amount DECIMAL(14,2),
            Аксессуары_quantity DECIMAL(12,3),
            Аксессуары_amount DECIMAL(14,2),
            LifeStyle_quantity DECIMAL(12,3),
            LifeStyle_amount DECIMAL(14,2),
            MS_Home_quantity DECIMAL(12,3),
            MS_Home_amount DECIMAL(14,2),
            Подарки_quantity DECIMAL(12,3),
            Подарки_amount DECIMAL(14,2),
            ТВ_quantity DECIMAL(12,3),
            ТВ_amount DECIMAL(14,2),
            Настройки_amount DECIMAL(14,2),
            Страховки_amount DECIMAL(14,2),
            Сим_карты_quantity DECIMAL(12,3)
        )
    """)
    conn.commit()

# Функция загрузки данных из Excel в таблицу plane_ms_phone
def load_data_from_excel(cursor, conn, file_path):
    # Читаем Excel файл с помощью pandas
    df = pd.read_excel(file_path)

    # Преобразуем данные в тип Decimal для точности
    df['total_net'] = df['total_net'].apply(Decimal)
    df['На_связи_quantity'] = df['На_связи_quantity'].apply(Decimal)
    df['На_связи_amount'] = df['На_связи_amount'].apply(Decimal)
    df['Аксессуары_quantity'] = df['Аксессуары_quantity'].apply(Decimal)
    df['Аксессуары_amount'] = df['Аксессуары_amount'].apply(Decimal)
    df['LifeStyle_quantity'] = df['LifeStyle_quantity'].apply(Decimal)
    df['LifeStyle_amount'] = df['LifeStyle_amount'].apply(Decimal)
    df['MS_Home_quantity'] = df['MS_Home_quantity'].apply(Decimal)
    df['MS_Home_amount'] = df['MS_Home_amount'].apply(Decimal)
    df['Подарки_quantity'] = df['Подарки_quantity'].apply(Decimal)
    df['Подарки_amount'] = df['Подарки_amount'].apply(Decimal)
    df['ТВ_quantity'] = df['ТВ_quantity'].apply(Decimal)
    df['ТВ_amount'] = df['ТВ_amount'].apply(Decimal)
    df['Настройки_amount'] = df['Настройки_amount'].apply(Decimal)
    df['Страховки_amount'] = df['Страховки_amount'].apply(Decimal)
    df['Сим_карты_quantity'] = df['Сим_карты_quantity'].apply(Decimal)

    # Очищаем таблицу перед загрузкой новых данных
    cursor.execute("DELETE FROM plane_ms_phone")
    conn.commit()

    total_records = 0

    # Загружаем данные в таблицу
    for _, row in df.iterrows():
        columns = ['store', 'total_net', 'На_связи_quantity', 'На_связи_amount', 'Аксессуары_quantity', 'Аксессуары_amount',
                   'LifeStyle_quantity', 'LifeStyle_amount', 'MS_Home_quantity', 'MS_Home_amount', 'Подарки_quantity',
                   'Подарки_amount', 'ТВ_quantity', 'ТВ_amount', 'Настройки_amount', 'Страховки_amount', 'Сим_карты_quantity']
        values = [row['store'], row['total_net'], row['На_связи_quantity'], row['На_связи_amount'], row['Аксессуары_quantity'],
                  row['Аксессуары_amount'], row['LifeStyle_quantity'], row['LifeStyle_amount'], row['MS_Home_quantity'],
                  row['MS_Home_amount'], row['Подарки_quantity'], row['Подарки_amount'], row['ТВ_quantity'], row['ТВ_amount'],
                  row['Настройки_amount'], row['Страховки_amount'], row['Сим_карты_quantity']]

        # Экранируем имена колонок через обратные кавычки
        columns_safe = [f"`{col}`" for col in columns]
        placeholders = ', '.join(['%s'] * len(values))

        cursor.execute(f"""
            INSERT INTO plane_ms_phone ({', '.join(columns_safe)})
            VALUES ({placeholders})
        """, values)

        total_records += 1

    conn.commit()
    print(f"✅ Таблица plane_ms_phone успешно создана и загружена. Загружено {total_records} строк.")

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    # Создание таблицы plane_ms_phone
    create_plane_ms_phone(cursor, conn)

    # Загрузка данных из Excel файла
    file_path = r"C:\Users\root\Desktop\Final\reports\plane_05_25.xlsx"
    load_data_from_excel(cursor, conn, file_path)

    cursor.close()
    conn.close()
