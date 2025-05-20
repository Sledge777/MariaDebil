import mysql.connector
import csv
from datetime import datetime

#--------------------------------------
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )
#--------------------------------------

def create_tables(cursor):
    print("Создание таблицы company_structure...", end='')
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS company_structure (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store TEXT,
            short_name TEXT,
            store_city TEXT,
            regional_director TEXT,
            bi_email TEXT,
            record_count INT
        )
    """)
    print("OK")

def load_data_from_txt(file_path, cursor, conn):
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter='\t')
        if reader.fieldnames:
            reader.fieldnames[0] = reader.fieldnames[0].replace('\ufeff', '')
        print("🔍 Заголовки в файле:", reader.fieldnames)

        count = 0

        for row in reader:
            try:
                if 'Итого' in row.get('Склад (складская территория)', ''):
                    continue

                record_count = None
                if row.get('Количество записей'):
                    record_count = int(row['Количество записей'].replace('\xa0', '').replace(' ', '').strip())

                cursor.execute("""
                    INSERT INTO company_structure (
                        store, short_name, store_city, regional_director, bi_email, record_count
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row['Склад (складская территория)'],
                    row['short_name (Склады и магазины)'],
                    row['store_city (Склады и магазины)'],
                    row['Региональный директор (Склады и магазины)'],
                    row['bi_email (Склады и магазины)'],
                    record_count
                ))
                count += 1
            except Exception as e:
                print("❌ Ошибка в строке:", row)
                print(e)

        conn.commit()
        commit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"✅ Загружено {count} записей в таблицу company_structure. Коммит выполнен в: {commit_time}")

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    create_tables(cursor)

    file_path = r'C:\Users\root\Desktop\Final\reports\company_structure.txt'
    load_data_from_txt(file_path, cursor, conn)

    cursor.close()
    conn.close()
