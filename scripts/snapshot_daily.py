"""
Ежедневный снимок активного рынка вакансий HH.

Назначение:
    - Получает все активные вакансии аналитиков с зарплатой
    - Сохраняет состояние рынка в vacancy_snapshots
    - Позволяет отслеживать время жизни вакансий

Запуск:
    GitHub Actions ежедневно

Переменные окружения:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY
    HH_ACCESS_TOKEN
"""

import os
import sys
import time
from datetime import datetime
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==========================================================
# ENV
# ==========================================================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HH_ACCESS_TOKEN = os.getenv("HH_ACCESS_TOKEN")

if not all([SUPABASE_URL, SUPABASE_KEY, HH_ACCESS_TOKEN]):
    print("❌ Отсутствуют обязательные переменные окружения")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BASE_URL = "https://api.hh.ru"
HH_USER_AGENT = "CustomMonitor/1.0 (olegjerryborisov@yandex.ru)"


# ==========================================================
# HTTP SESSION
# ==========================================================

def create_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": HH_USER_AGENT,
        "Authorization": f"Bearer {HH_ACCESS_TOKEN}",
        "Accept": "application/json"
    })

    retry = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def hh_request(session, url, params=None):
    try:
        r = session.get(url, params=params, timeout=20)
        if r.status_code != 200:
            print(f"⚠️ HH ошибка {r.status_code}")
            return None
        return r.json()
    except Exception as e:
        print(f"❌ Ошибка запроса HH: {e}")
        return None


# ==========================================================
# SUPABASE
# ==========================================================

def get_salary_data(vacancy_ids):
    """Забираем рассчитанную зарплату из таблицы vacancies"""
    result = {}
    if not vacancy_ids:
        return result

    try:
        response = (
            supabase
            .table("vacancies")
            .select("id, salary_avg_rub, salary_avg_net_rub")
            .in_("id", vacancy_ids)
            .execute()
        )
        for row in response.data:
            result[row["id"]] = row
        return result
    except Exception as e:
        print(f"❌ Ошибка получения зарплат из Supabase: {e}")
        return {}


def insert_snapshots(rows):
    if not rows:
        return 0
    try:
        response = (
            supabase
            .table("vacancy_snapshots")
            .upsert(rows, on_conflict="vacancy_id,snapshot_date")
            .execute()
        )
        return len(response.data)
    except Exception as e:
        print(f"❌ Ошибка вставки snapshot: {e}")
        return 0


# ==========================================================
# MAIN
# ==========================================================

print("=" * 70)
print("📸 ЕЖЕДНЕВНЫЙ СНИМОК РЫНКА HH.RU")
print("=" * 70)
print(f"🕐 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"🌍 Запрос: все активные вакансии аналитиков с зарплатой")

session = create_session()

today = datetime.now().date()
analytics_roles = [10, 148, 150, 156, 164, 165]

params = {
    "professional_role": analytics_roles,
    "only_with_salary": True,
    "area": 113,
    "per_page": 100,
    "page": 0
}

# Первый запрос — получаем метаданные
first = hh_request(session, f"{BASE_URL}/vacancies", params)
if not first:
    print("❌ Не удалось получить данные от hh.ru")
    sys.exit(0)

total_found = first.get("found", 0)
pages = min(first.get("pages", 0), 20)  # лимит API без разбивки по регионам
print(f"✅ Найдено активных вакансий: {total_found} (обработаем до {pages * 100})")

# Сбор всех ID
vacancy_ids = []
for page in range(pages):
    if page > 0:
        time.sleep(0.5)
    params["page"] = page
    data = hh_request(session, f"{BASE_URL}/vacancies", params)
    if data:
        ids = [x["id"] for x in data.get("items", [])]
        vacancy_ids.extend(ids)
        print(f"  📄 Страница {page+1}/{pages}: +{len(ids)} ID")
    else:
        print(f"  ⚠️ Страница {page+1}: ошибка")

print(f"\n📊 ИТОГ:")
print(f"  Собрано ID: {len(vacancy_ids)}")

if not vacancy_ids:
    print("✅ Нет активных вакансий")
    sys.exit(0)

# Получаем зарплаты из основной таблицы
salary_map = get_salary_data(vacancy_ids)

# Формируем snapshot
snapshot_rows = []
for vid in vacancy_ids:
    salary = salary_map.get(vid, {})
    snapshot_rows.append({
        "vacancy_id": vid,
        "snapshot_date": today.isoformat(),
        "salary_avg_rub": salary.get("salary_avg_rub"),
        "salary_avg_net_rub": salary.get("salary_avg_net_rub")
    })

print(f"  Подготовлено snapshot'ов: {len(snapshot_rows)}")

# Вставка
inserted = insert_snapshots(snapshot_rows)
print(f"\n✅ ЗАВЕРШЕНО")
print(f"  Вставлено записей: {inserted}")
print(f"  Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
