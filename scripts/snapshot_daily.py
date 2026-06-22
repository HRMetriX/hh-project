"""
Ежедневный снимок активного рынка вакансий HH.

Назначение:
    - Берёт текущий пул вакансий с HH API
    - Сохраняет состояние рынка в vacancy_snapshots
    - Не изменяет таблицу vacancies

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
from datetime import datetime, timedelta

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


if not all([
    SUPABASE_URL,
    SUPABASE_KEY,
    HH_ACCESS_TOKEN
]):
    print("❌ Нет переменных окружения")
    sys.exit(1)


supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)


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
        status_forcelist=[
            429,
            500,
            502,
            503,
            504
        ],
        allowed_methods=["GET"]
    )


    adapter = HTTPAdapter(
        max_retries=retry
    )

    session.mount(
        "https://",
        adapter
    )


    return session



def hh_request(session, url, params=None):

    try:

        r = session.get(
            url,
            params=params,
            timeout=20
        )


        if r.status_code != 200:
            print(
                f"⚠️ HH ошибка {r.status_code}"
            )
            return None


        return r.json()


    except Exception as e:

        print(
            f"❌ Ошибка запроса HH: {e}"
        )

        return None



# ==========================================================
# SUPABASE
# ==========================================================


def get_salary_data(vacancy_ids):

    """
    Забираем рассчитанную зарплату
    из уже существующей таблицы vacancies
    """

    result = {}


    if not vacancy_ids:
        return result


    response = (
        supabase
        .table("vacancies")
        .select(
            """
            id,
            salary_avg_rub,
            salary_avg_net_rub
            """
        )
        .in_("id", vacancy_ids)
        .execute()
    )


    for row in response.data:

        result[row["id"]] = row


    return result



def insert_snapshots(rows):

    if not rows:
        return 0


    try:

        response = (
            supabase
            .table("vacancy_snapshots")
            .insert(rows)
            .execute()
        )


        return len(response.data)


    except Exception as e:

        print(
            f"❌ Ошибка вставки snapshot: {e}"
        )

        return 0



# ==========================================================
# MAIN
# ==========================================================


print("=" * 70)
print("📸 SNAPSHOT DAILY")
print("=" * 70)


session = create_session()



today = datetime.now().date()

yesterday = today - timedelta(days=1)


date_from = yesterday.strftime("%Y-%m-%d")
date_to = (
    today + timedelta(days=1)
).strftime("%Y-%m-%d")



analytics_roles = [
    10,
    148,
    150,
    156,
    164,
    165
]



print(
    f"Дата снимка: {today}"
)

print(
    f"Период HH: {date_from} - {date_to}"
)



# ==========================================================
# Получаем список вакансий
# ==========================================================


params = {

    "professional_role": analytics_roles,

    "only_with_salary": True,

    "area": 113,

    "date_from": date_from,

    "date_to": date_to,

    "per_page": 100,

    "page": 0

}



first = hh_request(
    session,
    f"{BASE_URL}/vacancies",
    params
)


if not first:

    print(
        "❌ HH недоступен"
    )

    sys.exit(0)



pages = min(
    first.get("pages", 0),
    20
)



vacancy_ids = []


for page in range(pages):

    params["page"] = page


    data = hh_request(
        session,
        f"{BASE_URL}/vacancies",
        params
    )


    if data:

        ids = [
            x["id"]
            for x in data.get(
                "items",
                []
            )
        ]

        vacancy_ids.extend(ids)


    time.sleep(0.5)



print(
    f"Найдено вакансий: {len(vacancy_ids)}"
)



# ==========================================================
# Берём данные из vacancies
# ==========================================================


salary_map = get_salary_data(
    vacancy_ids
)



# ==========================================================
# Формируем snapshot
# ==========================================================


snapshot_rows = []



for vid in vacancy_ids:


    salary = salary_map.get(
        vid,
        {}
    )


    snapshot_rows.append({

        "vacancy_id": vid,

        "snapshot_date": today.isoformat(),


        "salary_avg_rub":
            salary.get(
                "salary_avg_rub"
            ),


        "salary_avg_net_rub":
            salary.get(
                "salary_avg_net_rub"
            )

    })



print(
    f"Подготовлено snapshots: {len(snapshot_rows)}"
)



inserted = insert_snapshots(
    snapshot_rows
)



print(
    f"✅ Вставлено: {inserted}"
)


print(
    "Готово"
)
