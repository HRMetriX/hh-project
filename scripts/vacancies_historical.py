import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Количество дней истории (из GitHub Actions или по умолчанию 30)
days = int(os.getenv('DAYS', '31'))

end_date = datetime.now().date()
start_date = end_date - timedelta(days=days)

date_from = start_date.strftime("%Y-%m-%d")
date_to = end_date.strftime("%Y-%m-%d")

print(f"Сбор за последние {days} дней: {date_from} - {date_to}")

load_dotenv()

# Подключение к Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://api.hh.ru"

# Настройка сессии
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)
session.mount("http://", adapter)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Кэши
city_coords_cache = {}
employer_industries_cache = {}

def get_city_coords(area_id):
    if area_id in city_coords_cache:
        return city_coords_cache[area_id]
    try:
        response = session.get(f"{BASE_URL}/areas/{area_id}", headers=headers, timeout=10)
        if response.status_code == 200:
            area_data = response.json()
            coords = {'lat': area_data.get('lat'), 'lng': area_data.get('lng')}
            city_coords_cache[area_id] = coords
            return coords
        return {'lat': None, 'lng': None}
    except Exception:
        return {'lat': None, 'lng': None}

def get_employer_industries(employer_id):
    if employer_id in employer_industries_cache:
        return employer_industries_cache[employer_id]
    try:
        response = session.get(f"{BASE_URL}/employers/{employer_id}", headers=headers, timeout=10)
        if response.status_code == 200:
            employer_data = response.json()
            industries = employer_data.get('industries', [])
            employer_industries_cache[employer_id] = industries
            return industries
        return []
    except Exception:
        return []

def enrich_with_coordinates(vacancy):
    address = vacancy.get('address', {})
    area = vacancy.get('area', {})
    if address and address.get('lat') and address.get('lng'):
        return address.get('lat'), address.get('lng'), 'address'
    area_id = area.get('id')
    if area_id:
        coords = get_city_coords(area_id)
        return coords.get('lat'), coords.get('lng'), f'area_{area_id}'
    return None, None, 'none'

def enrich_with_industries(vacancy):
    employer = vacancy.get('employer', {})
    employer_id = employer.get('id')
    if employer_id:
        industries = get_employer_industries(employer_id)
        if industries:
            return industries[0].get('name'), industries[0].get('id')
    return None, None

def enrich_with_professional_roles(vacancy):
    roles = vacancy.get('professional_roles', [])
    if roles:
        return roles[0].get('name'), roles[0].get('id')
    return None, None

def get_existing_ids():
    """Получает список уже существующих ID вакансий из базы"""
    try:
        response = supabase.table("vacancies").select("id").execute()
        return set(row['id'] for row in response.data)
    except Exception as e:
        print(f"Ошибка при получении существующих ID: {e}")
        return set()

def insert_vacancies_batch(rows):
    """Вставляет только новые вакансии (без дубликатов)"""
    if not rows:
        return 0
    
    try:
        # Получаем существующие ID
        existing_ids = get_existing_ids()
        
        # Фильтруем только новые
        new_rows = [row for row in rows if row['id'] not in existing_ids]
        
        if not new_rows:
            return 0
        
        # Вставляем пакетно
        supabase.table("vacancies").insert(new_rows).execute()
        return len(new_rows)
    except Exception as e:
        print(f"Ошибка при вставке: {e}")
        return 0

# Параметры сбора
analytics_roles = [10, 148, 150, 156, 164, 165]

# Дата начала и конца для исторического сбора
# Например, последние 30 дней
end_date = datetime.now().date()
start_date = end_date - timedelta(days=31)

date_from = start_date.strftime("%Y-%m-%d")
date_to = end_date.strftime("%Y-%m-%d")

print("=" * 80)
print("ИСТОРИЧЕСКИЙ СБОР ВАКАНСИЙ")
print("=" * 80)
print(f"Период: {date_from} - {date_to}")
print(f"Роли: {analytics_roles}")

# Получаем список уже существующих ID
existing_ids = get_existing_ids()
print(f"Уже в базе: {len(existing_ids)} вакансий")

params = {
    "professional_role": analytics_roles,
    "only_with_salary": True,
    "area": 113,
    "date_from": date_from,
    "date_to": date_to,
    "per_page": 100,
    "page": 0
}

# Получаем первую страницу
print("Запрос первой страницы...")
try:
    response = session.get(f"{BASE_URL}/vacancies", params=params, headers=headers, timeout=30)
    data = response.json()
except Exception as e:
    print(f"Ошибка: {e}")
    raise

total_found = data.get('found', 0)
pages = min(data.get('pages', 0), 20)

print(f"Всего вакансий в HH: {total_found}")
print(f"Страниц: {pages}")

if total_found == 0:
    print("Нет вакансий")
else:
    # Собираем ID всех вакансий
    all_vacancy_ids = []
    for page in range(pages):
        params["page"] = page
        try:
            response = session.get(f"{BASE_URL}/vacancies", params=params, headers=headers, timeout=30)
            page_data = response.json()
            for item in page_data.get('items', []):
                all_vacancy_ids.append(item['id'])
            print(f"Страница {page+1}: собрано {len(all_vacancy_ids)} ID")
            time.sleep(0.5)
        except Exception as e:
            print(f"Ошибка страницы {page+1}: {e}")
            continue
    
    # Фильтруем только новые ID
    new_ids = [vid for vid in all_vacancy_ids if vid not in existing_ids]
    print(f"\nВсего ID: {len(all_vacancy_ids)}")
    print(f"Новых ID (ещё нет в базе): {len(new_ids)}")
    
    if not new_ids:
        print("Нет новых вакансий для загрузки")
    else:
        print("\nСбор полных данных...")
        vacancies_batch = []
        errors = []
        start_total = time.time()
        
        for i, vac_id in enumerate(new_ids):
            print(f"[{i+1}/{len(new_ids)}] Загрузка {vac_id}")
            
            try:
                response = session.get(f"{BASE_URL}/vacancies/{vac_id}", headers=headers, timeout=30)
                
                if response.status_code == 200:
                    vacancy = response.json()
                    
                    lat, lng, coords_source = enrich_with_coordinates(vacancy)
                    main_industry, main_industry_id = enrich_with_industries(vacancy)
                    main_role, main_role_id = enrich_with_professional_roles(vacancy)
                    
                    # Маппинг BYR -> BYN
                    salary_currency = vacancy.get('salary', {}).get('currency') if vacancy.get('salary') else None
                    if salary_currency == 'BYR':
                        salary_currency = 'BYN'
                    
                    row = {
                        'id': vacancy.get('id'),
                        'name': vacancy.get('name'),
                        'published_at': vacancy.get('published_at'),
                        'created_at': vacancy.get('created_at'),
                        'initial_created_at': vacancy.get('initial_created_at'),
                        'alternate_url': vacancy.get('alternate_url'),
                        'salary_from': vacancy.get('salary', {}).get('from') if vacancy.get('salary') else None,
                        'salary_to': vacancy.get('salary', {}).get('to') if vacancy.get('salary') else None,
                        'salary_currency': salary_currency,
                        'salary_gross': vacancy.get('salary', {}).get('gross') if vacancy.get('salary') else None,
                        'area_id': vacancy.get('area', {}).get('id'),
                        'area_name': vacancy.get('area', {}).get('name'),
                        'lat': lat,
                        'lng': lng,
                        'coords_source': coords_source,
                        'address_raw': vacancy.get('address', {}).get('raw') if vacancy.get('address') else None,
                        'employer_id': vacancy.get('employer', {}).get('id'),
                        'employer_name': vacancy.get('employer', {}).get('name'),
                        'employer_accredited_it': vacancy.get('employer', {}).get('accredited_it_employer'),
                        'employer_trusted': vacancy.get('employer', {}).get('trusted'),
                        'employer_main_industry': main_industry,
                        'employer_main_industry_id': main_industry_id,
                        'professional_role': main_role,
                        'professional_role_id': main_role_id,
                        'experience_id': vacancy.get('experience', {}).get('id'),
                        'experience_name': vacancy.get('experience', {}).get('name'),
                        'employment_name': vacancy.get('employment', {}).get('name'),
                        'schedule_name': vacancy.get('schedule', {}).get('name'),
                        'accept_temporary': vacancy.get('accept_temporary'),
                        'accept_labor_contract': vacancy.get('accept_labor_contract'),
                        'internship': vacancy.get('internship'),
                        'night_shifts': vacancy.get('night_shifts'),
                        'work_format': ', '.join([f.get('name', '') for f in vacancy.get('work_format', [])]),
                        'working_hours': ', '.join([h.get('name', '') for h in vacancy.get('working_hours', [])]),
                        'work_schedule_by_days': ', '.join([s.get('name', '') for s in vacancy.get('work_schedule_by_days', [])]),
                        'key_skills': ', '.join([s.get('name', '') for s in vacancy.get('key_skills', [])]),
                        'has_test': vacancy.get('has_test'),
                        'test_required': vacancy.get('test', {}).get('required') if vacancy.get('test') else None,
                        'archived': vacancy.get('archived'),
                        'response_letter_required': vacancy.get('response_letter_required'),
                        'premium': vacancy.get('premium'),
                        'billing_type': vacancy.get('billing_type', {}).get('id') if vacancy.get('billing_type') else None,
                    }
                    
                    vacancies_batch.append(row)
                    print(f"  ✅ Собрано. Всего в батче: {len(vacancies_batch)}")
                    
                    # Вставляем каждые 50 строк
                    if len(vacancies_batch) >= 50:
                        inserted = insert_vacancies_batch(vacancies_batch)
                        print(f"  📦 Вставлено в БД: {inserted}")
                        vacancies_batch = []
                    
                else:
                    errors.append(vac_id)
                    print(f"  ❌ Ошибка {response.status_code}")
                    
            except Exception as e:
                errors.append(vac_id)
                print(f"  ❌ Исключение: {e}")
            
            time.sleep(0.3)
            
            if (i + 1) % 50 == 0:
                elapsed = time.time() - start_total
                avg = elapsed / (i + 1)
                remaining = (len(new_ids) - (i + 1)) * avg
                print(f"--- ПРОГРЕСС: {i+1}/{len(new_ids)} | Вставлено: {len(vacancies_batch)} в буфере | Осталось: {remaining/60:.1f} мин ---")
        
        # Вставляем остаток
        if vacancies_batch:
            inserted = insert_vacancies_batch(vacancies_batch)
            print(f"\n📦 Последняя вставка: {inserted} новых вакансий")
        
        total_time = time.time() - start_total
        print("\n" + "=" * 80)
        print("ЗАВЕРШЕНО")
        print("=" * 80)
        print(f"Всего новых вакансий обработано: {len(new_ids)}")
        print(f"Успешно вставлено: {len(new_ids) - len(errors)}")
        print(f"Ошибок: {len(errors)}")
        print(f"⏱️ Время: {total_time/60:.1f} мин")
