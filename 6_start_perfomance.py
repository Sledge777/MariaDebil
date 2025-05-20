import mysql.connector
from decimal import Decimal

# Подключение к базе данных
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Ms_phone_fox62052!",
        database="ms_phone"
    )

# Функция для создания таблицы ms_phone_performance
def create_ms_phone_performance(cursor, conn):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ms_phone_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store VARCHAR(255),
            total_net_percentage DECIMAL(7,2),
            На_связи_quantity_percentage DECIMAL(7,2),
            На_связи_amount_percentage DECIMAL(7,2),
            Аксессуары_quantity_percentage DECIMAL(7,2),
            Аксессуары_amount_percentage DECIMAL(7,2),
            LifeStyle_quantity_percentage DECIMAL(7,2),
            LifeStyle_amount_percentage DECIMAL(7,2),
            MS_Home_quantity_percentage DECIMAL(7,2),
            MS_Home_amount_percentage DECIMAL(7,2),
            Подарки_quantity_percentage DECIMAL(7,2),
            Подарки_amount_percentage DECIMAL(7,2),
            ТВ_quantity_percentage DECIMAL(7,2),
            ТВ_amount_percentage DECIMAL(7,2),
            Настройки_amount_percentage DECIMAL(7,2),
            Страховки_amount_percentage DECIMAL(7,2),
            Сим_карты_quantity_percentage DECIMAL(7,2)
        )
    """)
    conn.commit()
    print("✅ Таблица ms_phone_performance успешно создана.")

# Функция для вычисления процентов выполнения и загрузки данных с проверкой на допустимость значений
def calculate_performance(cursor, conn):
    # Получаем данные из таблицы fact_ms_phone
    cursor.execute("""
        SELECT store, total_net, На_связи_quantity, На_связи_amount, Аксессуары_quantity, Аксессуары_amount,
               LifeStyle_quantity, LifeStyle_amount, MS_Home_quantity, MS_Home_amount, Подарки_quantity, Подарки_amount,
               ТВ_quantity, ТВ_amount, Настройки_amount, Страховки_amount, Сим_карты_quantity
        FROM fact_ms_phone
    """)
    fact_data = cursor.fetchall()

    # Получаем данные из таблицы plane_ms_phone
    cursor.execute("""
        SELECT store, total_net, На_связи_quantity, На_связи_amount, Аксессуары_quantity, Аксессуары_amount,
               LifeStyle_quantity, LifeStyle_amount, MS_Home_quantity, MS_Home_amount, Подарки_quantity, Подарки_amount,
               ТВ_quantity, ТВ_amount, Настройки_amount, Страховки_amount, Сим_карты_quantity
        FROM plane_ms_phone
    """)
    plane_data = cursor.fetchall()

    # Очищаем таблицу перед загрузкой новых данных
    cursor.execute("DELETE FROM ms_phone_performance")
    conn.commit()

    total_records = 0

    # Сопоставляем данные из факта и плана, и рассчитываем процент выполнения
    for fact_row in fact_data:
        store = fact_row[0]

        # Находим плановую строку для магазина
        plane_row = next((row for row in plane_data if row[0] == store), None)
        if not plane_row:
            continue  # Если нет плановых данных для магазина, пропускаем

        # Расчет процентов для каждой категории с проверкой на допустимость
        def calculate_percentage(actual, plan):
            percentage = (actual / plan * 100) if plan != 0 else Decimal(0)
            return min(percentage, Decimal(999.99))  # Ограничиваем максимальное значение

        total_net_percentage = calculate_percentage(fact_row[1], plane_row[1])

        # Для каждой категории рассчитываем процент выполнения для количества и оборота
        На_связи_quantity_percentage = calculate_percentage(fact_row[2], plane_row[2])
        На_связи_amount_percentage = calculate_percentage(fact_row[3], plane_row[3])
        
        Аксессуары_quantity_percentage = calculate_percentage(fact_row[4], plane_row[4])
        Аксессуары_amount_percentage = calculate_percentage(fact_row[5], plane_row[5])
        
        LifeStyle_quantity_percentage = calculate_percentage(fact_row[6], plane_row[6])
        LifeStyle_amount_percentage = calculate_percentage(fact_row[7], plane_row[7])
        
        MS_Home_quantity_percentage = calculate_percentage(fact_row[8], plane_row[8])
        MS_Home_amount_percentage = calculate_percentage(fact_row[9], plane_row[9])
        
        Подарки_quantity_percentage = calculate_percentage(fact_row[10], plane_row[10])
        Подарки_amount_percentage = calculate_percentage(fact_row[11], plane_row[11])
        
        ТВ_quantity_percentage = calculate_percentage(fact_row[12], plane_row[12])
        ТВ_amount_percentage = calculate_percentage(fact_row[13], plane_row[13])
        
        Настройки_amount_percentage = calculate_percentage(fact_row[14], plane_row[14])
        Страховки_amount_percentage = calculate_percentage(fact_row[15], plane_row[15])
        
        Сим_карты_quantity_percentage = calculate_percentage(fact_row[16], plane_row[16])

        # Формируем список столбцов и значений для вставки
        columns = ['store', 'total_net_percentage', 'На_связи_quantity_percentage', 'На_связи_amount_percentage',
                   'Аксессуары_quantity_percentage', 'Аксессуары_amount_percentage', 'LifeStyle_quantity_percentage',
                   'LifeStyle_amount_percentage', 'MS_Home_quantity_percentage', 'MS_Home_amount_percentage',
                   'Подарки_quantity_percentage', 'Подарки_amount_percentage', 'ТВ_quantity_percentage', 'ТВ_amount_percentage',
                   'Настройки_amount_percentage', 'Страховки_amount_percentage', 'Сим_карты_quantity_percentage']
        values = [store, total_net_percentage, На_связи_quantity_percentage, На_связи_amount_percentage,
                  Аксессуары_quantity_percentage, Аксессуары_amount_percentage, LifeStyle_quantity_percentage,
                  LifeStyle_amount_percentage, MS_Home_quantity_percentage, MS_Home_amount_percentage,
                  Подарки_quantity_percentage, Подарки_amount_percentage, ТВ_quantity_percentage, ТВ_amount_percentage,
                  Настройки_amount_percentage, Страховки_amount_percentage, Сим_карты_quantity_percentage]

        # Экранируем имена колонок через обратные кавычки
        columns_safe = [f"`{col}`" for col in columns]
        placeholders = ', '.join(['%s'] * len(values))

        cursor.execute(f"""
            INSERT INTO ms_phone_performance ({', '.join(columns_safe)})
            VALUES ({placeholders})
        """, values)

        total_records += 1

    conn.commit()
    print(f"✅ Таблица ms_phone_performance успешно создана и заполнена. Загружено {total_records} строк.")

# Основной код
def main():
    conn = create_connection()
    cursor = conn.cursor()

    create_ms_phone_performance(cursor, conn)
    calculate_performance(cursor, conn)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
