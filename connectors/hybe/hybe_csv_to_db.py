import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
import glob
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HOST = 'your_host_here'
PORT = 3306  # Порт MariaDB по умолчанию
USER = 'your_username_here'
PASSWORD = 'your_password_here'
DATABASE = 'your_database_name_here'
TABLE = 'hybe_api_data'


class DatabaseManager:
    def __init__(self):
        self.connection_string = f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
        self.engine = create_engine(self.connection_string)

    def test_connection(self):
        """Проверка соединения с базой данных"""
        try:
            with self.engine.begin() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("Соединение с базой данных установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            return False

    def create_database_if_not_exists(self):
        """Создание базы данных если не существует"""
        try:
            # Подключаемся без указания базы данных
            engine_no_db = create_engine(f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/')

            with engine_no_db.begin() as connection:
                # Проверяем существование базы данных
                result = connection.execute(text(f"SHOW DATABASES LIKE '{DATABASE}'"))
                if not result.fetchone():
                    # Создаем базу данных
                    connection.execute(
                        text(f"CREATE DATABASE {DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
                    logger.info(f"База данных {DATABASE} создана")
                else:
                    logger.info(f"База данных {DATABASE} уже существует")

            engine_no_db.dispose()
            return True

        except Exception as e:
            logger.error(f"Ошибка создания базы данных: {e}")
            return False

    def create_table_if_not_exists(self):
        """Создание таблицы если не существует"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            cabinet_id INT NOT NULL,
            cabinet_name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            advertiser_name VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            campaign_name VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            campaign_id VARCHAR(255),
            date DATE,
            impressions INT DEFAULT 0,
            clicks INT DEFAULT 0,
            spend_in_rub DECIMAL(15,2) DEFAULT 0.00,
            INDEX idx_cabinet_date (cabinet_id, date),
            INDEX idx_campaign_id (campaign_id),
            INDEX idx_date (date),
            INDEX idx_cabinet_id (cabinet_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """

        try:
            with self.engine.begin() as connection:
                connection.execute(text(create_table_sql))
            logger.info(f"Таблица {TABLE} готова")
            return True
        except Exception as e:
            logger.error(f"Ошибка создания таблицы: {e}")
            return False

    def add_impressions_column_if_not_exists(self):
        """Добавить поле impressions в существующую таблицу если его нет"""
        try:
            # Проверяем существует ли поле impressions
            check_column_sql = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{DATABASE}' 
            AND TABLE_NAME = '{TABLE}' 
            AND COLUMN_NAME = 'impressions'
            """

            with self.engine.begin() as connection:
                result = connection.execute(text(check_column_sql))
                column_exists = result.fetchone()

                if not column_exists:
                    # Добавляем поле impressions
                    add_column_sql = f"""
                    ALTER TABLE {TABLE} 
                    ADD COLUMN impressions INT DEFAULT 0 
                    AFTER date
                    """
                    connection.execute(text(add_column_sql))
                    logger.info("Поле 'impressions' добавлено в таблицу")
                else:
                    logger.info("Поле 'impressions' уже существует в таблице")

            return True

        except Exception as e:
            logger.error(f"Ошибка добавления поля impressions: {e}")
            return False

    def get_existing_records_count(self):
        """Получить количество существующих записей"""
        try:
            with self.engine.begin() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM {TABLE}"))
                count = result.fetchone()[0]
                return count
        except Exception as e:
            logger.error(f"Ошибка получения количества записей: {e}")
            return 0

    def remove_duplicates_before_insert(self, df):
        """Удалить дубликаты перед вставкой в БД"""
        if df.empty:
            return df

        try:
            # Получаем существующие комбинации cabinet_id + campaign_id + date из БД
            existing_query = f"""
                SELECT DISTINCT cabinet_id, campaign_id, date 
                FROM {TABLE}
            """

            with self.engine.begin() as connection:
                existing_df = pd.read_sql(existing_query, connection)

            if existing_df.empty:
                logger.info("БД пуста, все записи будут новыми")
                return df

            # Подсчитываем дубликаты ДО фильтрации
            before_count = len(df)

            # Объединяем с существующими данными для поиска дубликатов
            merged = df.merge(
                existing_df,
                on=['cabinet_id', 'campaign_id', 'date'],
                how='left',
                indicator=True
            )

            # Оставляем только новые записи (которых нет в БД)
            df_clean = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)

            after_count = len(df_clean)
            duplicates_found = before_count - after_count

            if duplicates_found > 0:
                logger.info(f"🔍 Найдено и удалено дубликатов: {duplicates_found}")
                logger.info(f"📊 Новых записей для загрузки: {after_count}")
            else:
                logger.info(f"✅ Дубликатов не найдено, все {after_count} записей новые")

            return df_clean

        except Exception as e:
            logger.error(f"Ошибка при фильтрации дубликатов: {e}")
            logger.warning("Загружаем данные без фильтрации дубликатов")
            return df

    def save_dataframe(self, df):
        """Сохранить DataFrame в базу данных"""
        if df.empty:
            logger.warning("DataFrame пуст, нечего сохранять")
            return False

        try:
            # Удаляем дубликаты перед вставкой
            df_clean = self.remove_duplicates_before_insert(df)

            if df_clean.empty:
                logger.info("После удаления дубликатов не осталось новых записей для загрузки")
                return True

            # Сохраняем только новые данные
            rows_affected = df_clean.to_sql(
                name=TABLE,
                con=self.engine,
                if_exists='append',
                index=False,
                chunksize=5000,
                method='multi'
            )

            logger.info(f"✅ Сохранено новых записей в БД: {len(df_clean)}")
            return True

        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
            return False

    def get_data_summary(self):
        """Получить сводку по данным в БД"""
        try:
            query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT campaign_id) as unique_campaigns,
                    COUNT(DISTINCT cabinet_id) as unique_cabinets,
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    SUM(impressions) as total_impressions,
                    SUM(clicks) as total_clicks,
                    SUM(spend_in_rub) as total_spend
                FROM {TABLE}
            """

            with self.engine.begin() as connection:
                result = connection.execute(text(query))
                row = result.fetchone()

                if row:
                    return {
                        'total_records': row[0],
                        'unique_campaigns': row[1],
                        'unique_cabinets': row[2],
                        'min_date': row[3].strftime('%Y-%m-%d') if row[3] else None,
                        'max_date': row[4].strftime('%Y-%m-%d') if row[4] else None,
                        'total_impressions': row[5] or 0,
                        'total_clicks': row[6] or 0,
                        'total_spend': float(row[7]) if row[7] else 0
                    }
                return None

        except Exception as e:
            logger.error(f"Ошибка получения сводки: {e}")
            return None


def parse_date_column(date_str):
    """Парсинг даты из различных форматов"""
    if pd.isna(date_str) or date_str == '':
        return None

    # Список возможных форматов
    date_formats = [
        '%Y-%m-%d',  # 2025-01-01 (основной формат из обновленного скрипта)
        '%d.%m.%Y',  # 01.01.2025
        '%d/%m/%Y',  # 01/01/2025
        '%Y/%m/%d',  # 2025/01/01
        '%d-%m-%Y',  # 01-01-2025
        '%Y-%m-%dT%H:%M:%S',  # 2025-01-01T00:00:00 (на случай если старый формат)
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str), fmt).date()
        except ValueError:
            continue

    logger.warning(f"Не удалось распарсить дату: {date_str}")
    return None


def prepare_dataframe_for_db(df):
    """Подготовить DataFrame для загрузки в БД"""
    if df.empty:
        return df

    logger.info("Подготавливаем данные для загрузки в БД")

    # Создаем копию DataFrame
    df_clean = df.copy()

    # Обрабатываем даты
    df_clean['date'] = df_clean['date'].apply(parse_date_column)

    # Удаляем записи с невалидными датами
    before_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['date'])
    after_count = len(df_clean)

    if before_count != after_count:
        logger.warning(f"Удалено {before_count - after_count} записей с невалидными датами")

    # Приводим типы данных
    df_clean['cabinet_id'] = pd.to_numeric(df_clean['cabinet_id'], errors='coerce').fillna(0).astype(int)
    df_clean['impressions'] = pd.to_numeric(df_clean['impressions'], errors='coerce').fillna(0).astype(int)
    df_clean['clicks'] = pd.to_numeric(df_clean['clicks'], errors='coerce').fillna(0).astype(int)
    df_clean['spend_in_rub'] = pd.to_numeric(df_clean['spend_in_rub'], errors='coerce').fillna(0).round(2)

    # Обрезаем строки до максимальной длины
    df_clean['cabinet_name'] = df_clean['cabinet_name'].astype(str).str[:255]
    df_clean['advertiser_name'] = df_clean['advertiser_name'].astype(str).str[:500]
    df_clean['campaign_name'] = df_clean['campaign_name'].astype(str).str[:500]
    df_clean['campaign_id'] = df_clean['campaign_id'].astype(str).str[:255]

    # Удаляем полностью пустые записи
    df_clean = df_clean.dropna(subset=['campaign_id', 'campaign_name'])

    logger.info(f"Подготовлено {len(df_clean)} записей для загрузки")
    return df_clean


def find_csv_files():
    """Найти самый свежий CSV файл от нашего скрипта"""
    # Ищем файлы нашего скрипта по шаблону hybe_data_YYYYMMDD_HHMMSS.csv
    hybe_files = glob.glob('hybe_data_*.csv')

    if not hybe_files:
        logger.warning("Не найдены файлы hybe_data_*.csv, ищем любые CSV файлы")
        # Если нет наших файлов, берем любые CSV
        csv_files = glob.glob('*.csv')
        if csv_files:
            return [max(csv_files, key=os.path.getctime)]  # Самый новый по времени создания
        else:
            return []

    # Сортируем наши файлы по timestamp в имени (самый новый последним)
    def extract_timestamp(filename):
        """Извлекает timestamp из имени файла hybe_data_YYYYMMDD_HHMMSS.csv"""
        try:
            # Извлекаем часть YYYYMMDD_HHMMSS из имени файла
            basename = os.path.basename(filename)
            timestamp_part = basename.replace('hybe_data_', '').replace('.csv', '')
            # Преобразуем в datetime для сортировки
            return datetime.strptime(timestamp_part, '%Y%m%d_%H%M%S')
        except:
            # Если не удалось распарсить, возвращаем очень старую дату
            return datetime(1900, 1, 1)

    # Сортируем файлы по timestamp и берем самый новый
    sorted_files = sorted(hybe_files, key=extract_timestamp, reverse=True)
    latest_file = sorted_files[0]

    logger.info(f"Найдено файлов hybe_data: {len(hybe_files)}")
    logger.info(f"Выбран самый свежий файл: {latest_file}")

    # Возвращаем только самый свежий файл
    return [latest_file]


def load_csv_file(filename):
    """Загрузить CSV файл"""
    try:
        logger.info(f"Загружаем файл: {filename}")

        # Пробуем разные кодировки
        encodings = ['utf-8', 'cp1251', 'windows-1251']

        for encoding in encodings:
            try:
                df = pd.read_csv(filename, encoding=encoding)
                logger.info(f"Файл {filename} загружен с кодировкой {encoding}")
                logger.info(f"Количество записей: {len(df)}")
                return df
            except UnicodeDecodeError:
                continue

        # Если все кодировки не подошли
        logger.error(f"Не удалось загрузить файл {filename} ни с одной кодировкой")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Ошибка загрузки файла {filename}: {e}")
        return pd.DataFrame()


def validate_csv_structure(df):
    """Проверить структуру CSV файла"""
    required_columns = [
        'cabinet_id', 'cabinet_name', 'advertiser_name',
        'campaign_name', 'campaign_id', 'date', 'impressions', 'clicks', 'spend_in_rub'
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.error(f"В CSV файле отсутствуют обязательные колонки: {missing_columns}")
        logger.info(f"Найденные колонки: {list(df.columns)}")
        return False

    logger.info("Структура CSV файла корректна")
    return True


def main():
    print("CSV TO DATABASE LOADER")
    print("=" * 50)

    # Проверка параметров подключения к БД
    if not all([HOST, USER, PASSWORD, DATABASE]):
        logger.error("Не указаны параметры подключения к БД!")
        return

    # Инициализация менеджера БД
    db_manager = DatabaseManager()

    # Создание базы данных если не существует
    if not db_manager.create_database_if_not_exists():
        logger.error("Не удалось создать базу данных")
        return

    # Проверка соединения
    if not db_manager.test_connection():
        logger.error("Не удалось подключиться к базе данных")
        return

    # Создание таблицы
    if not db_manager.create_table_if_not_exists():
        logger.error("Не удалось создать таблицу")
        return

    # Добавляем поле impressions если его нет (для обратной совместимости)
    if not db_manager.add_impressions_column_if_not_exists():
        logger.warning("Не удалось добавить поле impressions")

    # Поиск CSV файлов (только самый свежий)
    csv_files = find_csv_files()

    if not csv_files:
        logger.error("CSV файлы не найдены в текущей директории")
        logger.info("Ожидаемые файлы: hybe_data_YYYYMMDD_HHMMSS.csv")
        return

    logger.info(f"Найден файл для обработки: {csv_files[0]}")

    # Получаем сводку до загрузки
    summary_before = db_manager.get_data_summary()
    if summary_before:
        logger.info(f"Записей в БД до загрузки: {summary_before['total_records']}")
    else:
        logger.info("БД пуста")

    # Обработка CSV файла (только один - самый свежий)
    csv_file = csv_files[0]

    # Загружаем CSV
    df = load_csv_file(csv_file)

    if df.empty:
        logger.error(f"Файл {csv_file} пуст или не удалось загрузить")
        return

    # Проверяем структуру
    if not validate_csv_structure(df):
        logger.error(f"Файл {csv_file} имеет неправильную структуру")
        return

    # Подготавливаем данные
    df_prepared = prepare_dataframe_for_db(df)

    if df_prepared.empty:
        logger.warning(f"После обработки файл {csv_file} оказался пуст")
        return

    logger.info(f"Файл {csv_file} подготовлен: {len(df_prepared)} записей")

    # Удаляем внутренние дубликаты в самом файле
    before_dedup = len(df_prepared)
    df_prepared = df_prepared.drop_duplicates(subset=['cabinet_id', 'campaign_id', 'date'])
    after_dedup = len(df_prepared)

    if before_dedup != after_dedup:
        logger.info(f"Удалено внутренних дубликатов в файле: {before_dedup - after_dedup}")

    logger.info(f"Итого записей для загрузки: {len(df_prepared)}")

    # Сохраняем в БД
    if db_manager.save_dataframe(df_prepared):
        logger.info("✅ Данные успешно загружены в базу данных")

        # Финальная сводка
        summary_after = db_manager.get_data_summary()
        if summary_after:
            logger.info("📊 ИТОГОВАЯ СВОДКА:")
            logger.info(f"  Всего записей в БД: {summary_after['total_records']:,}")
            logger.info(f"  Уникальных кампаний: {summary_after['unique_campaigns']:,}")
            logger.info(f"  Уникальных кабинетов: {summary_after['unique_cabinets']:,}")
            if summary_after['min_date'] and summary_after['max_date']:
                logger.info(f"  Период данных: {summary_after['min_date']} - {summary_after['max_date']}")
            logger.info(f"  Всего показов: {summary_after['total_impressions']:,}")
            logger.info(f"  Всего кликов: {summary_after['total_clicks']:,}")
            logger.info(f"  Общие расходы: {summary_after['total_spend']:,.2f} руб.")

            if summary_before:
                new_records = summary_after['total_records'] - summary_before['total_records']
                logger.info(f"  📈 Добавлено новых записей: {new_records:,}")

    else:
        logger.error("❌ Ошибка загрузки данных в базу данных")
    print("Загрузка завершена!")


if __name__ == '__main__':
    main()