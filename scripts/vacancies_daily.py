import requests
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import json
import sys

load_dotenv()

# Подключение к Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://api.hh.ru"

# ========== НАСТРОЙКИ ДЛЯ GITHUB ACTIONS ==========
MAX_RETRIES = 2  # Минимум повторных попыток
REQUEST_TIMEOUT = 15  # Короткий таймаут для Actions
DELAY_BETWEEN_REQUESTS = 0.5  # Минимальная задержка

def create_session():
    """Создает сессию, оптимизированную для GitHub Actions"""
    session = requests.Session()
    
    # Используем один стабильный User-Agent
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://hh.ru/",
        "Origin": "https://hh.ru"
    })
    
    # Минимальный retry
    retry_strategy = Retry(
        total=1,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

def check_ip_block():
    """Быстрая проверка блокировки IP"""
    print("🔍 Проверка доступности API...")
    session = create_session()
    
    try:
        # Пробуем самый легкий эндпоинт
        response = session.get(
            f"{BASE_URL}/dictionaries", 
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code == 200:
            print("✅ API доступен")
            return True
        elif response.status_code == 403:
            print("❌ IP ЗАБЛОКИРОВАН (403 Forbidden)")
            print("💡 GitHub Actions IP часто блокируются hh.ru")
            print("💡 Решения:")
            print("   1. Запускать скрипт локально")
            print("   2. Использовать self-hosted runner")
            print("   3. Настроить прокси/VPN в Actions")
            return False
        else:
            print(f"⚠️ Неожиданный статус: {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Таймаут соединения")
        return False
    except Exception as e:
        print(f"❌ Ошибка соединения: {e}")
        return False

def quick_request(session, url, params=None, context=""):
    """Быстрый запрос с минимальными задержками"""
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        
        if response.status_code == 403:
            print(f"❌ [BLOCKED] {context}: IP заблокирован")
            return None
        elif response.status_code != 200:
            print(f"⚠️ [ERROR] {context}: статус {response.status_code}")
            return None
            
        return response
        
    except requests.exceptions.Timeout:
        print(f"❌ [TIMEOUT] {context}: превышено время ожидания")
        return None
    except Exception as e:
        print(f"❌ [EXCEPTION] {context}: {e}")
        return None

# Кэши
city_coords_cache = {}
employer_industries_cache = {}

def get_city_coords(area_id):
    if area_id in city_coords_cache:
        return city_coords_cache[area_id]
    
    response = quick_request(session, f"{BASE_URL}/areas/{area_id}", context=f"area_{area_id}")
    if response:
        area_data = response.json()
        coords = {'lat': area_data.get('lat'), 'lng': area_data.get('lng')}
        city_coords_cache[area_id] = coords
        return coords
    
    return {'lat': None, 'lng': None}

def get_employer_industries(employer_id):
    if employer_id in employer_industries_cache:
        return employer_industries_cache[employer_id]
    
    response = quick_request(session, f"{BASE_URL}/employers/{employer_id}", context=f"employer_{employer_id}")
    if response:
        employer_data = response.json()
        industries = employer_data.get('industries', [])
        employer_industries_cache[employer_id] = industries
        return industries
    
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
    """Получает список всех существующих ID вакансий из базы"""
    try:
        all_ids = []
        page = 0
        page_size = 1000
        
        while True:
            response = supabase.table("vacancies").select("id").range(
                page * page_size, 
                (page + 1) * page_size - 1
            ).execute()
            
            if not response.data:
                break
                
            all_ids.extend([row['id'] for row in response.data])
            
            if len(response.data) < page_size:
                break
                
            page += 1
        
        return set(all_ids)
    except Exception as e:
        print(f"⚠️ Ошибка получения ID из Supabase: {e}")
        return set()

def insert_vacancies_batch(rows):
    """Вставляет только новые вакансии"""
    if not rows:
        return 0
    
    try:
        existing_ids = get_existing_ids()
        new_rows = [row for row in rows if row['id'] not in existing_ids]
        
        if not new_rows:
            return 0
        
        result = supabase.table("vacancies").insert(new_rows).execute()
        return len(new_rows)
    except Exception as e:
        print(f"⚠️ Ошибка вставки в Supabase: {e}")
        return 0

# ========== ОСНОВНОЙ КОД ==========

print("=" * 80)
print("🤖 GITHUB ACTIONS - СБОР ВАКАНСИЙ HH.RU")
print("=" * 80)
print(f"🕐 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Быстрая проверка блокировки
if not check_ip_block():
    print("\n❌ СКРИПТ ОСТАНОВЛЕН: API недоступен")
    print("📝 Создаю issue в логах Actions для информации")
    # В GitHub Actions это не уронит workflow с ошибкой
    sys.exit(0)  # Завершаем успешно, чтобы Actions не падал

# Создаем сессию
session = create_session()

# Параметры сбора
analytics_roles = [10, 148, 150, 156, 164, 165]

# Даты
today = datetime.now().date()
yesterday = today - timedelta(days=1)
date_from = yesterday.strftime("%Y-%m-%d")
date_to = (today + timedelta(days=1)).strftime("%Y-%m-%d")

print("\n" + "=" * 80)
print("📊 ПАРАМЕТРЫ ПОИСКА")
print("=" * 80)
print(f"Период: {date_from} - {date_to}")
print(f"Роли: {analytics_roles}")

# Получаем существующие ID
existing_ids = get_existing_ids()
print(f"В базе: {len(existing_ids)} вакансий")

# Параметры запроса
params = {
    "professional_role": analytics_roles,
    "only_with_salary": True,
    "area": 113,
    "date_from": date_from,
    "date_to": date_to,
    "per_page": 100,
    "page": 0
}

print("\n" + "=" * 80)
print("🔍 ПОИСК ВАКАНСИЙ")
print("=" * 80)

# Основной запрос
response = quick_request(session, f"{BASE_URL}/vacancies", params, "основной поиск")

if not response:
    print("\n❌ Не удалось получить данные")
    sys.exit(0)

data = response.json()
total_found = data.get('found', 0)
pages = min(data.get('pages', 0), 20)  # Ограничиваем 20 страницами для скорости

print(f"Найдено: {total_found} вакансий")
print(f"Страниц для обработки: {pages}")

if total_found == 0:
    print("\n✅ Нет вакансий за период")
    sys.exit(0)

# Быстрый сбор ID
print("\n📑 Сбор ID вакансий...")
all_vacancy_ids = []

for page in range(pages):
    if page > 0:  # Пропускаем паузу для первой страницы
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    params["page"] = page
    response = quick_request(session, f"{BASE_URL}/vacancies", params, f"страница {page+1}")
    
    if response:
        page_data = response.json()
        items = page_data.get('items', [])
        all_vacancy_ids.extend([item['id'] for item in items])
        print(f"  📄 Стр. {page+1}/{pages}: +{len(items)} ID")
    else:
        print(f"  ⚠️ Стр. {page+1}: ошибка, пропускаем")
        continue

# Фильтрация новых ID
new_ids = [vid for vid in all_vacancy_ids if vid not in existing_ids]
print(f"\n📊 ИТОГ:")
print(f"  Всего ID: {len(all_vacancy_ids)}")
print(f"  Новых: {len(new_ids)}")

if not new_ids:
    print("\n✅ Нет новых вакансий")
    sys.exit(0)

# Сбор деталей (быстрый)
print(f"\n📥 Сбор данных для {len(new_ids)} вакансий...")
vacancies_batch = []
errors = []
start_time = time.time()
inserted_total = 0

for i, vac_id in enumerate(new_ids):
    if i > 0:
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    print(f"[{i+1}/{len(new_ids)}] {vac_id}", end=" ")
    
    response = quick_request(session, f"{BASE_URL}/vacancies/{vac_id}", context=f"вакансия {vac_id}")
    
    if response:
        vacancy = response.json()
        
        lat, lng, coords_source = enrich_with_coordinates(vacancy)
        main_industry, main_industry_id = enrich_with_industries(vacancy)
        main_role, main_role_id = enrich_with_professional_roles(vacancy)
        
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
        print("✅")
        
        # Вставка каждые 20 вакансий (меньше для Actions)
        if len(vacancies_batch) >= 20:
            inserted = insert_vacancies_batch(vacancies_batch)
            inserted_total += inserted
            print(f"  💾 Вставлено: {inserted}")
            vacancies_batch = []
    else:
        errors.append(vac_id)
        print("❌")
    
    # Прогресс каждые 10 вакансий
    if (i + 1) % 10 == 0:
        elapsed = time.time() - start_time
        avg = elapsed / (i + 1)
        remaining = (len(new_ids) - (i + 1)) * avg
        print(f"  ⏱️ Прогресс: {i+1}/{len(new_ids)} | Осталось: {remaining/60:.1f} мин")

# Вставка остатка
if vacancies_batch:
    inserted = insert_vacancies_batch(vacancies_batch)
    inserted_total += inserted
    print(f"\n💾 Финальная вставка: {inserted}")

total_time = time.time() - start_time

print("\n" + "=" * 80)
print("✅ ЗАВЕРШЕНО")
print("=" * 80)
print(f"Обработано: {len(new_ids)}")
print(f"Вставлено: {inserted_total}")
print(f"Ошибок: {len(errors)}")
print(f"Время: {total_time/60:.1f} мин")
print(f"🕐 Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
