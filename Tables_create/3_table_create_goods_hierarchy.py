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
    print("Создание таблицы goods_hierarchy...", end='')
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS goods_hierarchy (
            goods_id VARCHAR(255) PRIMARY KEY,
            goods_name TEXT,
            parent_group_id_fk TEXT
        )
    """)
    print("OK")

def load_data_from_txt(file_path, cursor, conn):
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        # Автоопределение правильного разделителя
        sample = f.read(1024)
        delimiter = '\t' if '\t' in sample else ';' if ';' in sample else ','
        f.seek(0)

        reader = csv.DictReader(f, delimiter=delimiter)
        if reader.fieldnames:
            reader.fieldnames[0] = reader.fieldnames[0].replace('\ufeff', '')

        print("🔍 Заголовки в файле:", reader.fieldnames)

        for row in reader:
            try:
                cursor.execute("""
                    INSERT IGNORE INTO goods_hierarchy (
                        goods_id, goods_name, parent_group_id_fk
                    ) VALUES (%s, %s, %s)
                """, (
                    row.get('goods_id'),
                    row.get('goods_name'),
                    row.get('parent_group_id_fk')
                ))
            except Exception as e:
                print("❌ Ошибка в строке:", row)
                print(e)

        conn.commit()
    #----------------------------------------------------------------------------------
    commit_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("✅ Данные успешно загружены в MariaDB.")
    print(f"Запись в таблицу goods_hierarchy выполнена в: {commit_time}")
    #----------------------------------------------------------------------------------

# Основной блок
if __name__ == "__main__":
    conn = create_connection()
    cursor = conn.cursor()

    create_tables(cursor)

    file_path = r'C:\Users\root\Desktop\Final\reports\goods_hierarhy.txt'
    load_data_from_txt(file_path, cursor, conn)

    cursor.close()
    conn.close()
