import requests
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CABINETS = [
    {
        'cabinet_id': 1,
        'cabinet_name': 'Cabinet_1',
        'client_id': 'your_client_id_here',
        'client_secret': 'your_client_secret_here',
        'active': True
    },
    {
        'cabinet_id': 2,
        'cabinet_name': 'Cabinet_2',
        'client_id': 'your_client_id_here',
        'client_secret': 'your_client_secret_here',
        'active': True
    }
    # Добавьте другие кабинеты по аналогии
]

# API настройки
TOKEN_URL = 'https://api.hybrid.ru/token'

GLOBAL_DATE_FROM = 'your_start_date_here'  # Дата начала выгрузки для всех кабинетов
GLOBAL_DATE_TO = 'your_end_date_here'  # Дата окончания (или '' для автоматического расчета до вчера)


class HybeAPIClient:
    def __init__(self, cabinet_config: Dict):
        self.cabinet_id = cabinet_config['cabinet_id']
        self.cabinet_name = cabinet_config['cabinet_name']
        self.client_id = cabinet_config['client_id']
        self.client_secret = cabinet_config['client_secret']
        self.active = cabinet_config['active']
        self.token = None

    def get_access_token(self) -> str:
        """Получение access_token для Hybe.io API"""
        if not self.active:
            logger.warning(f"Кабинет {self.cabinet_name} отключен")
            return None

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }

        try:
            resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
            resp.raise_for_status()
            self.token = resp.json()['access_token']
            logger.info(f"Токен для кабинета {self.cabinet_name} получен")
            return self.token
        except Exception as e:
            logger.error(f"Ошибка получения токена для {self.cabinet_name}: {e}")
            return None

    def get_advertisers_list(self) -> List[Dict]:
        """Получить список рекламодателей"""
        if not self.token:
            return []

        url = 'https://api.hybrid.ru/v3.0/agency/advertisers'
        headers = {'Authorization': f'Bearer {self.token}'}

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Ошибка получения рекламодателей для {self.cabinet_name}: {e}")
            return []

    def get_campaigns_by_advertiser(self, advertiser_id: str) -> List[Dict]:
        """Получить список кампаний для рекламодателя"""
        if not self.token:
            return []

        url = f'https://api.hybrid.ru/v3.0/advertiser/campaigns?advertiserId={advertiser_id}'
        headers = {'Authorization': f'Bearer {self.token}'}

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Ошибка получения кампаний для рекламодателя {advertiser_id}: {e}")
            return []

    def build_campaign_mapping(self) -> Dict[str, Dict]:
        """Построение маппинга ID кампаний к их названиям и рекламодателям"""
        if not self.token:
            return {}

        logger.info(f"Строим маппинг кампаний для {self.cabinet_name}")
        campaign_mapping = {}

        try:
            advertisers = self.get_advertisers_list()
            logger.info(f"Найдено рекламодателей: {len(advertisers)}")

            for advertiser in advertisers:
                advertiser_id = advertiser.get('Id')
                advertiser_name = advertiser.get('Name', 'Unknown Advertiser')

                campaigns = self.get_campaigns_by_advertiser(advertiser_id)
                for campaign in campaigns:
                    campaign_id = campaign.get('Id')
                    campaign_name = campaign.get('Name', f'Campaign_{campaign_id}')

                    if campaign_id:
                        campaign_mapping[campaign_id] = {
                            'real_name': campaign_name,
                            'advertiser_name': advertiser_name
                        }

            logger.info(f"Собрано {len(campaign_mapping)} кампаний")
            return campaign_mapping

        except Exception as e:
            logger.error(f"Ошибка построения маппинга для {self.cabinet_name}: {e}")
            return {}

    def get_agency_statistics(self, date_from: str, date_to: str, split: str = 'Day',
                              page: int = 0, limit: int = 100) -> Dict:
        """Получить статистику агентства"""
        if not self.token:
            return {}

        # Проверяем корректность split параметра
        valid_splits = ['Day', 'Hour', 'BannerName', 'Campaign', 'App', 'DeviceType',
                        'OS', 'Advertiser', 'Country', 'Region', 'City', 'BannerSize',
                        'BannerType', 'Ssp', 'Week', 'Month', 'Folder']

        if split not in valid_splits:
            logger.error(f"Недопустимый split параметр: {split}. Используем 'Day'")
            split = 'Day'

        url = f'https://api.hybrid.ru/v3.0/agency/{split}?from={date_from}&to={date_to}&page={page}&limit={limit}'
        headers = {'Authorization': f'Bearer {self.token}'}

        try:
            logger.info(f"Запрос к API: {url}")
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP ошибка {e.response.status_code}: {e}")
            if e.response.status_code == 400:
                logger.error(f"Ответ сервера: {e.response.text}")
            return {}
        except Exception as e:
            logger.error(f"Ошибка получения статистики агентства: {e}")
            return {}

    def get_campaign_statistics(self, date_from: str, date_to: str, campaign_id: str,
                                split: str = 'Day', page: int = 0, limit: int = 100) -> Dict:
        """Получить статистику кампании"""
        if not self.token:
            return {}

        # Проверяем корректность split параметра
        valid_splits = ['Day', 'Hour', 'BannerName', 'Campaign', 'App', 'DeviceType',
                        'OS', 'Advertiser', 'Country', 'Region', 'City', 'BannerSize',
                        'BannerType', 'Ssp', 'Week', 'Month', 'Folder']

        if split not in valid_splits:
            logger.error(f"Недопустимый split параметр: {split}. Используем 'Day'")
            split = 'Day'

        url = f'https://api.hybrid.ru/v3.0/campaign/{split}?from={date_from}&to={date_to}&campaignId={campaign_id}&page={page}&limit={limit}'
        headers = {'Authorization': f'Bearer {self.token}'}

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP ошибка {e.response.status_code} для кампании {campaign_id}: {e}")
            if e.response.status_code == 400:
                logger.warning(f"Ответ сервера: {e.response.text}")
            return {}
        except Exception as e:
            logger.warning(f"Ошибка получения статистики кампании {campaign_id}: {e}")
            return {}

    def get_detailed_statistics(self, date_from: str, date_to: str, campaign_mapping: Dict[str, Dict]) -> List[Dict]:
        """Получить детализированную статистику с реальными названиями кампаний"""
        if not self.token:
            return []

        logger.info(f"Получаем статистику для {self.cabinet_name} за {date_from} - {date_to}")

        # Проверяем период - если больше 90 дней, разбиваем на части
        start_date_obj = datetime.strptime(date_from, '%Y-%m-%d')
        end_date_obj = datetime.strptime(date_to, '%Y-%m-%d')
        period_days = (end_date_obj - start_date_obj).days + 1

        if period_days > 90:
            logger.info(f"Период {period_days} дней превышает лимит API (90 дней), разбиваем на части")
            return self.get_statistics_by_chunks(date_from, date_to, campaign_mapping, chunk_days=89)
        else:
            return self.get_statistics_single_period(date_from, date_to, campaign_mapping)

    def get_statistics_single_period(self, date_from: str, date_to: str, campaign_mapping: Dict[str, Dict]) -> List[
        Dict]:
        """Получить статистику за один период (до 90 дней)"""
        try:
            # Используем Campaign split для получения данных с CampaignId
            logger.info(f"Получаем список кампаний через Campaign split")
            campaigns_stats = self.get_agency_statistics(date_from, date_to, split='Campaign', limit=10000)

            if not campaigns_stats or not campaigns_stats.get('Statistic') or len(
                    campaigns_stats.get('Statistic', [])) == 0:
                logger.warning(f"Нет данных по кампаниям за период для {self.cabinet_name}")
                return []

            logger.info(f"✓ Найдено кампаний в Campaign split: {len(campaigns_stats['Statistic'])}")

            # Собираем уникальные ID кампаний из Campaign split
            campaign_ids = []
            for stat in campaigns_stats['Statistic']:
                campaign_id = stat.get('CampaignId')
                if campaign_id:
                    campaign_ids.append(campaign_id)

            campaign_ids = list(set(campaign_ids))  # убираем дубликаты
            logger.info(f"📋 Уникальных кампаний: {len(campaign_ids)}")

            if not campaign_ids:
                logger.warning(f"❌ Не найдено ID кампаний в ответе API для {self.cabinet_name}")
                return []

            # Получаем детальную статистику по дням для каждой кампании
            all_detailed_stats = []

            for i, campaign_id in enumerate(campaign_ids):
                # Определяем название кампании и рекламодателя
                if campaign_id in campaign_mapping:
                    campaign_name = campaign_mapping[campaign_id]['real_name']
                    advertiser_name = campaign_mapping[campaign_id]['advertiser_name']
                else:
                    campaign_name = f"Campaign_{campaign_id[-8:]}"
                    advertiser_name = "Unknown Advertiser"

                if (i + 1) % 5 == 0:
                    logger.info(f"🔄 Обработано: {i + 1}/{len(campaign_ids)} кампаний")

                # Получаем статистику по дням для этой кампании
                campaign_daily_stats = self.get_campaign_statistics(
                    date_from, date_to, campaign_id, split='Day', limit=1000
                )

                if campaign_daily_stats and campaign_daily_stats.get('Statistic'):
                    logger.info(f"✓ Кампания {campaign_name}: {len(campaign_daily_stats['Statistic'])} записей")

                    # Добавляем информацию о кампании и кабинете к каждой записи
                    for stat in campaign_daily_stats['Statistic']:
                        stat['CampaignId'] = campaign_id
                        stat['CampaignName'] = campaign_name
                        stat['AdvertiserName'] = advertiser_name
                        stat['CabinetId'] = self.cabinet_id
                        stat['CabinetName'] = self.cabinet_name

                    all_detailed_stats.extend(campaign_daily_stats['Statistic'])
                else:
                    logger.warning(f"⚠️ Нет данных для кампании {campaign_name}")

            logger.info(f"✅ Собрано {len(all_detailed_stats)} записей для {self.cabinet_name}")
            return all_detailed_stats

        except Exception as e:
            logger.error(f"❌ Ошибка получения данных для {self.cabinet_name}: {e}")
            return []

    def get_statistics_by_chunks(self, date_from: str, date_to: str, campaign_mapping: Dict[str, Dict],
                                 chunk_days: int = 89) -> List[Dict]:
        """Получение статистики по частям (для периодов больше 90 дней)"""
        if not self.token:
            return []

        all_data = []
        current_date = datetime.strptime(date_from, '%Y-%m-%d')
        end_date = datetime.strptime(date_to, '%Y-%m-%d')

        chunk_number = 1
        while current_date <= end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days - 1), end_date)
            chunk_from = current_date.strftime('%Y-%m-%d')
            chunk_to = chunk_end.strftime('%Y-%m-%d')

            logger.info(f"📅 Часть {chunk_number}: {chunk_from} - {chunk_to} для {self.cabinet_name}")

            try:
                stats = self.get_statistics_single_period(chunk_from, chunk_to, campaign_mapping)
                if stats:
                    all_data.extend(stats)
                    logger.info(f"✓ Часть {chunk_number}: получено {len(stats)} записей")
                else:
                    logger.warning(f"⚠️ Часть {chunk_number}: нет данных")
            except Exception as e:
                logger.error(f"❌ Ошибка для части {chunk_number} ({chunk_from} - {chunk_to}): {e}")

            current_date = chunk_end + timedelta(days=1)
            chunk_number += 1

        logger.info(f"🎯 Всего собрано {len(all_data)} записей по частям для {self.cabinet_name}")
        return all_data


def convert_date_format(date_str: str, from_format: str, to_format: str) -> str:
    """Конвертация формата даты"""
    try:
        date_obj = datetime.strptime(date_str, from_format)
        return date_obj.strftime(to_format)
    except Exception as e:
        logger.error(f"Ошибка конвертации даты {date_str}: {e}")
        return date_str


def prepare_dataframe(raw_data: List[Dict]) -> pd.DataFrame:
    """Подготовить DataFrame из сырых данных"""
    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)

    # Создаем финальный DataFrame с правильными полями
    df_final = pd.DataFrame()
    df_final['cabinet_id'] = df.get('CabinetId', 0)
    df_final['cabinet_name'] = df.get('CabinetName', 'Unknown Cabinet')
    df_final['advertiser_name'] = df.get('AdvertiserName', 'Unknown Advertiser')
    df_final['campaign_name'] = df.get('CampaignName', 'Unknown Campaign')
    df_final['campaign_id'] = df.get('CampaignId', 'Unknown ID')

    # Конвертируем дату из формата "2025-05-15T00:00:00" в "2025-05-15"
    if 'Day' in df.columns:
        df_final['date'] = pd.to_datetime(df['Day']).dt.strftime('%Y-%m-%d')
    else:
        df_final['date'] = ''

    # Добавляем показы из поля ImpressionCount
    df_final['impressions'] = pd.to_numeric(df.get('ImpressionCount', 0), errors='coerce').fillna(0).astype(int)

    # Используем правильные поля из анализа API
    df_final['clicks'] = pd.to_numeric(df.get('ClickCount', 0), errors='coerce').fillna(0).astype(int)
    df_final['spend_in_rub'] = pd.to_numeric(df.get('SumWinningPrice', 0), errors='coerce').fillna(0).round(2)

    # Фильтруем некорректные записи
    df_final = df_final.dropna(subset=['campaign_name', 'campaign_id', 'date'])
    df_final = df_final[df_final['campaign_name'] != 'Unknown Campaign']
    df_final = df_final[df_final['campaign_id'] != 'Unknown ID']

    return df_final


def process_cabinet(cabinet_config: Dict) -> pd.DataFrame:
    """Обработка данных одного кабинета"""
    cabinet_name = cabinet_config['cabinet_name']

    logger.info(f"Обрабатываем кабинет: {cabinet_name}")

    # Проверяем активность кабинета
    if not cabinet_config.get('active', True):
        logger.warning(f"Кабинет {cabinet_name} отключен")
        return pd.DataFrame()

    # Проверяем наличие учетных данных
    if not cabinet_config.get('client_id') or not cabinet_config.get('client_secret'):
        logger.error(f"Не указаны CLIENT_ID/CLIENT_SECRET для {cabinet_name}")
        return pd.DataFrame()

    try:
        # Инициализация клиента API
        client = HybeAPIClient(cabinet_config)

        # Получаем токен
        token = client.get_access_token()
        if not token:
            logger.error(f"Не удалось получить токен для {cabinet_name}")
            return pd.DataFrame()

        # Используем глобальные настройки периода для всех кабинетов
        start_date = GLOBAL_DATE_FROM

        # Если GLOBAL_DATE_TO пустая - используем вчерашний день
        if GLOBAL_DATE_TO == '' or not GLOBAL_DATE_TO:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
            logger.info(f"Автоматически установлена конечная дата: {end_date} (вчера)")
        else:
            end_date = GLOBAL_DATE_TO

        logger.info(f"Период для {cabinet_name}: {start_date} - {end_date}")

        # Конвертируем даты из формата dd.mm.yyyy в yyyy-mm-dd для API
        api_date_from = convert_date_format(start_date, '%d.%m.%Y', '%Y-%m-%d')
        api_date_to = convert_date_format(end_date, '%d.%m.%Y', '%Y-%m-%d')

        logger.info(f"API период для {cabinet_name}: {api_date_from} - {api_date_to}")

        # Строим маппинг кампаний
        campaign_mapping = client.build_campaign_mapping()

        # Получаем данные
        raw_data = client.get_detailed_statistics(api_date_from, api_date_to, campaign_mapping)

        if raw_data:
            # Подготавливаем DataFrame
            df = prepare_dataframe(raw_data)

            if not df.empty:
                logger.info(f"Получено данных для {cabinet_name}: {len(df)} записей")
                return df
            else:
                logger.warning(f"Нет данных для {cabinet_name}")
                return pd.DataFrame()
        else:
            logger.warning(f"Не удалось получить данные для {cabinet_name}")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Ошибка обработки {cabinet_name}: {e}")
        return pd.DataFrame()


def main():
    print("HYBE.IO DATA EXPORT TO CSV")
    print("=" * 50)

    # Проверка конфигурации кабинетов
    if not CABINETS:
        logger.error("Не настроен ни один кабинет!")
        return

    active_cabinets = [c for c in CABINETS if c.get('active', True)]
    if not active_cabinets:
        logger.error("Нет активных кабинетов!")
        return

    logger.info(f"Активных кабинетов: {len(active_cabinets)}")

    # Проверка формата дат
    try:
        datetime.strptime(GLOBAL_DATE_FROM, '%d.%m.%Y')

        # Проверяем конечную дату
        if GLOBAL_DATE_TO == '' or not GLOBAL_DATE_TO:
            calculated_end_date = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
            logger.info(f"Глобальный период выгрузки: {GLOBAL_DATE_FROM} - {calculated_end_date} (до вчера)")
        else:
            datetime.strptime(GLOBAL_DATE_TO, '%d.%m.%Y')
            logger.info(f"Глобальный период выгрузки: {GLOBAL_DATE_FROM} - {GLOBAL_DATE_TO}")

    except ValueError:
        logger.error(f"Неверный формат глобальных дат! Используйте DD.MM.YYYY")
        return

    # Обработка каждого кабинета
    all_dataframes = []

    for cabinet_config in CABINETS:
        if cabinet_config.get('active', True):
            df = process_cabinet(cabinet_config)
            if not df.empty:
                all_dataframes.append(df)

    # Объединяем все данные
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)

        # Сохраняем в CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'hybe_data_{timestamp}.csv'

        final_df.to_csv(filename, index=False, encoding='utf-8')

        logger.info(f"Данные сохранены в файл: {filename}")
        logger.info(f"Всего записей: {len(final_df)}")
        logger.info(f"Уникальных кампаний: {final_df['campaign_name'].nunique()}")
        logger.info(f"Период данных: {final_df['date'].min()} - {final_df['date'].max()}")
        logger.info(f"Всего показов: {final_df['impressions'].sum():,}")
        logger.info(f"Всего кликов: {final_df['clicks'].sum():,}")
        logger.info(f"Общие расходы: {final_df['spend_in_rub'].sum():,.2f} руб.")

    else:
        logger.error("Нет данных для сохранения")


if __name__ == '__main__':
    main()