import time
import hashlib
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

ACCOUNTS = [
    {
        'account_id': 1,
        'account_name': 'Account_1',
        'api_key': 'your_api_key_here',
        'access_key': 'your_access_key_here',
        'active': True
    },
    {
        'account_id': 1,
        'account_name': 'Account_2',
        'api_key': 'your_api_key_here',
        'access_key': 'your_access_key_here',
        'active': True
    }
    # Добавьте другие аккаунты по аналогии
]

GLOBAL_DATE_FROM = 'your_start_date_here'  # Дата начала выгрузки для всех кабинетов
GLOBAL_DATE_TO = 'your_end_date_here'  # Дата окончания (или '' для автоматического расчета до вчера)

DEFAULT_TIMEZONE = '+3'
MAX_RETRIES = 15
RETRY_DELAY = 45
REQUEST_TIMEOUT = 120
MAX_CONSECUTIVE_ERRORS = 3


class MintegralAPIClient:
    def __init__(self, account_config: dict):
        self.account_id = account_config['account_id']
        self.account_name = account_config['account_name']
        self.api_key = account_config['api_key']
        self.access_key = account_config['access_key']
        self.active = account_config['active']

    def get_token(self):
        """Генерация токена для аутентификации"""
        timestamp = str(int(time.time()))
        timestamp_md5 = hashlib.md5(timestamp.encode()).hexdigest()
        token = hashlib.md5((self.api_key + timestamp_md5).encode()).hexdigest()
        return token, timestamp

    def make_api_request(self, type_value, start_date, end_date, dimension_option='Offer',
                         time_granularity='daily', timezone=DEFAULT_TIMEZONE):
        """Выполнение запроса к API"""
        params = {
            'start_time': start_date,
            'end_time': end_date,
            'type': type_value,
            'dimension_option': dimension_option,
            'time_granularity': time_granularity,
            'timezone': timezone
        }

        token, timestamp = self.get_token()
        headers = {
            'access-key': self.access_key,
            'token': token,
            'timestamp': timestamp,
            'Content-Type': 'application/json'
        }

        url = 'https://ss-api.mintegral.com/api/v2/reports/data'

        try:
            response = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса для {self.account_name}: {e}")
            return None

    def test_api_connection(self):
        """Тест подключения к API"""
        try:
            test_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
            response = self.make_api_request(1, test_date, test_date, 'Offer', 'daily')
            return response and (response.ok or response.status_code in [400, 401])
        except Exception as e:
            logger.error(f"Ошибка тестирования API для {self.account_name}: {e}")
            return False

    def wait_for_data_generation(self, start_date, end_date, dimension_option='Offer',
                                 time_granularity='daily', max_retries=MAX_RETRIES):
        """Ожидание генерации данных на сервере"""
        consecutive_errors = 0

        for attempt in range(max_retries):
            try:
                response = self.make_api_request(1, start_date, end_date, dimension_option, time_granularity)

                if not response:
                    consecutive_errors += 1
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        return False
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (1 + consecutive_errors * 0.5)
                        logger.info(f"Ожидание {wait_time}с перед повтором...")
                        time.sleep(wait_time)
                    continue

                consecutive_errors = 0

                if not response.ok:
                    if attempt < max_retries - 1:
                        logger.info(f"Ожидание генерации данных... попытка {attempt + 1}/{max_retries}")
                        time.sleep(RETRY_DELAY)
                    continue

                try:
                    data = response.json()
                except:
                    if attempt < max_retries - 1:
                        time.sleep(RETRY_DELAY)
                    continue

                code = data.get('code')

                if code == 200:
                    logger.info(f"✅ Данные готовы для {self.account_name}")
                    return True
                elif code in [201, 202]:
                    if attempt < max_retries - 1:
                        logger.info(f"Генерация данных... попытка {attempt + 1}/{max_retries}")
                        time.sleep(RETRY_DELAY)
                    continue
                else:
                    logger.warning(f"❌ API вернул код {code} для {self.account_name}")
                    return False

            except Exception as e:
                consecutive_errors += 1
                logger.warning(f"Ошибка при ожидании данных: {e}")
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    return False
                if attempt < max_retries - 1:
                    wait_time = RETRY_DELAY * (1 + consecutive_errors * 0.5)
                    time.sleep(wait_time)

        return False

    def download_data(self, start_date, end_date, dimension_option='Offer', time_granularity='daily'):
        """Скачивание готовых данных"""
        response = self.make_api_request(2, start_date, end_date, dimension_option, time_granularity)

        if not response or not response.ok:
            return None

        content_type = response.headers.get('Content-Type', '')
        if 'application/octet-stream' in content_type or 'text/plain' in content_type:
            return response.text
        return None

    def get_data_for_period(self, start_date, end_date, dimension_option='Offer', time_granularity='daily'):
        """Получение данных за период"""
        logger.info(f"Получаем данные для {self.account_name} за {start_date} - {end_date}")

        if not self.wait_for_data_generation(start_date, end_date, dimension_option, time_granularity):
            logger.warning(f"❌ Не удалось получить данные для {self.account_name}")
            return None

        return self.download_data(start_date, end_date, dimension_option, time_granularity)

    def parse_data_to_dataframe(self, data_text):
        """Парсинг данных в DataFrame"""
        if not data_text or not data_text.strip():
            return None

        try:
            df = pd.read_csv(StringIO(data_text), sep='\t')
            return df if not df.empty else None
        except Exception as e:
            logger.error(f"Ошибка парсинга данных для {self.account_name}: {e}")
            return None


def convert_date_format(date_str: str, from_format: str, to_format: str) -> str:
    """Конвертация формата даты"""
    try:
        date_obj = datetime.strptime(date_str, from_format)
        return date_obj.strftime(to_format)
    except Exception as e:
        logger.error(f"Ошибка конвертации даты {date_str}: {e}")
        return date_str


def split_date_range(start_date, end_date, days=7):
    """Разбивка периода на части (API позволяет максимум 7 дней)"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    date_ranges = []
    current = start

    while current <= end:
        next_date = min(current + timedelta(days=days - 1), end)
        date_ranges.append((
            current.strftime('%Y-%m-%d'),
            next_date.strftime('%Y-%m-%d')
        ))
        current = next_date + timedelta(days=1)

    return date_ranges


def transform_to_target_format(df, account_id, account_name):
    """Преобразование данных в целевой формат"""
    if df.empty:
        return pd.DataFrame()

    result_df = pd.DataFrame()

    # Добавляем информацию об аккаунте - ИСПРАВЛЕНО
    result_df['account_id'] = account_id
    result_df['account_name'] = account_name

    # Конвертируем дату
    if 'Date' in df.columns:
        result_df['date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    else:
        result_df['date'] = ''

    # Название кампании
    if 'Offer Name' in df.columns:
        result_df['campaign_name'] = df['Offer Name']
    else:
        result_df['campaign_name'] = 'Unknown Campaign'

    # Метрики - добавляем проверки на существование колонок
    if 'Impression' in df.columns:
        result_df['impression'] = pd.to_numeric(df['Impression'], errors='coerce').fillna(0).astype(int)
    else:
        result_df['impression'] = 0

    if 'Click' in df.columns:
        result_df['clicks'] = pd.to_numeric(df['Click'], errors='coerce').fillna(0).astype(int)
    else:
        result_df['clicks'] = 0

    if 'Spend' in df.columns:
        result_df['spend_in_dollars'] = pd.to_numeric(df['Spend'], errors='coerce').fillna(0).round(4)
    else:
        result_df['spend_in_dollars'] = 0.0

    # Убеждаемся что account_id и account_name заполнены для всех строк
    result_df['account_id'] = result_df['account_id'].fillna(account_id)
    result_df['account_name'] = result_df['account_name'].fillna(account_name)

    return result_df.sort_values(['date', 'campaign_name']).reset_index(drop=True)


def process_account(account_config: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """Обработка данных одного аккаунта"""
    account_name = account_config['account_name']

    logger.info(f"Обрабатываем аккаунт: {account_name}")

    # Проверяем активность аккаунта
    if not account_config.get('active', True):
        logger.warning(f"Аккаунт {account_name} отключен")
        return pd.DataFrame()

    # Проверяем наличие учетных данных
    if not account_config.get('api_key') or not account_config.get('access_key'):
        logger.error(f"Не указаны API_KEY/ACCESS_KEY для {account_name}")
        return pd.DataFrame()

    try:
        # Инициализация клиента API
        client = MintegralAPIClient(account_config)

        # Тест подключения
        if not client.test_api_connection():
            logger.error(f"Ошибка подключения к API для {account_name}")
            return pd.DataFrame()

        # Разбиваем период на части (максимум 7 дней на запрос)
        date_ranges = split_date_range(start_date, end_date, days=7)
        logger.info(f"Период разбит на {len(date_ranges)} частей для {account_name}")

        all_dataframes = []
        successful_periods = 0
        failed_periods = 0

        for i, (period_start, period_end) in enumerate(date_ranges, 1):
            logger.info(f"[{i}/{len(date_ranges)}] Обрабатываем {period_start} - {period_end} для {account_name}")

            try:
                data_text = client.get_data_for_period(period_start, period_end, 'Offer', 'daily')

                if data_text:
                    df = client.parse_data_to_dataframe(data_text)
                    if df is not None:
                        # Преобразуем в целевой формат
                        transformed_df = transform_to_target_format(df, account_config['account_id'], account_name)
                        all_dataframes.append(transformed_df)
                        successful_periods += 1
                        logger.info(f"✓ Получено {len(transformed_df)} записей")
                    else:
                        failed_periods += 1
                        logger.warning(f"❌ Нет данных за период {period_start} - {period_end}")
                else:
                    failed_periods += 1
                    logger.warning(f"❌ Не удалось получить данные за период {period_start} - {period_end}")

            except Exception as e:
                logger.error(f"Ошибка обработки периода {period_start} - {period_end}: {e}")
                failed_periods += 1

        logger.info(f"✅ Успешных периодов для {account_name}: {successful_periods}")
        logger.info(f"❌ Неудачных периодов для {account_name}: {failed_periods}")

        if all_dataframes:
            final_df = pd.concat(all_dataframes, ignore_index=True)
            logger.info(f"📊 Итого записей для {account_name}: {len(final_df)}")
            return final_df
        else:
            logger.warning(f"Нет данных для {account_name}")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Ошибка обработки аккаунта {account_name}: {e}")
        return pd.DataFrame()


def main():
    print("MINTEGRAL DATA EXPORT TO CSV")
    print("=" * 50)

    # Проверка конфигурации аккаунтов
    if not ACCOUNTS:
        logger.error("Не настроен ни один аккаунт!")
        return

    active_accounts = [a for a in ACCOUNTS if a.get('active', True)]
    if not active_accounts:
        logger.error("Нет активных аккаунтов!")
        return

    logger.info(f"Активных аккаунтов: {len(active_accounts)}")

    # Определение периода
    start_date = GLOBAL_DATE_FROM

    # Если GLOBAL_DATE_TO пустая - используем вчерашний день
    if GLOBAL_DATE_TO == '' or not GLOBAL_DATE_TO:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
        logger.info(f"Автоматически установлена конечная дата: {end_date} (вчера)")
    else:
        end_date = GLOBAL_DATE_TO

    # Проверка формата дат
    try:
        datetime.strptime(start_date, '%d.%m.%Y')
        datetime.strptime(end_date, '%d.%m.%Y')
        logger.info(f"Глобальный период выгрузки: {start_date} - {end_date}")
    except ValueError:
        logger.error(f"Неверный формат глобальных дат! Используйте DD.MM.YYYY")
        return

    # Конвертируем даты в формат API
    api_date_from = convert_date_format(start_date, '%d.%m.%Y', '%Y-%m-%d')
    api_date_to = convert_date_format(end_date, '%d.%m.%Y', '%Y-%m-%d')

    logger.info(f"API период: {api_date_from} - {api_date_to}")

    # Обработка каждого аккаунта
    all_dataframes = []

    for account_config in ACCOUNTS:
        if account_config.get('active', True):
            df = process_account(account_config, api_date_from, api_date_to)
            if not df.empty:
                all_dataframes.append(df)

    # Объединяем все данные
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)

        # Сохраняем в CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'mintegral_data_{timestamp}.csv'

        final_df.to_csv(filename, index=False, encoding='utf-8')

        logger.info(f"✅ Данные сохранены в файл: {filename}")
        logger.info(f"📊 Всего записей: {len(final_df)}")
        logger.info(f"🏢 Уникальных аккаунтов: {final_df['account_name'].nunique()}")
        logger.info(f"📋 Уникальных кампаний: {final_df['campaign_name'].nunique()}")
        logger.info(f"📅 Период данных: {final_df['date'].min()} - {final_df['date'].max()}")

        # Статистика по метрикам
        total_impressions = final_df['impression'].sum()
        total_clicks = final_df['clicks'].sum()
        total_spend = final_df['spend_in_dollars'].sum()
        logger.info(f"💰 Показов: {total_impressions:,}, Кликов: {total_clicks:,}, Расходы: ${total_spend:,.2f}")

    else:
        logger.error("Нет данных для сохранения")


if __name__ == '__main__':
    main()