import requests
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import sys

load_dotenv()

# Секреты (из .env или GitHub Secrets)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HH_ACCESS_TOKEN = os.getenv("HH_ACCESS_TOKEN")

# Проверка наличия всех секретов
if not all([SUPABASE_URL, SUPABASE_KEY, HH_ACCESS_TOKEN]):
    print("❌ Отсутствуют обязательные переменные окружения")
    print("   Проверь: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, HH_ACCESS_TOKEN")
    sys.exit(1)

# Подключение к Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://api.hh.ru"

# Открытые данные (можно прямо в коде)
HH_USER_AGENT = "CustomMonitor/1.0 (olegjerryborisov@yandex.ru)"

# ========== МАППЕРЫ ДЛЯ СОВМЕСТИМОСТИ С БД ==========
EMPLOYMENT_MAPPING = {
    "Полная": "Полная занятость",
    "Частичная": "Частичная занятость",
    "Проект": "Проектная работа",
    "Вахта": "Вахта",
    "Подработка": "Подработка",
    "Стажировка": "Стажировка",
}

SCHEDULE_MAPPING = {
    "5/2": "Полный день",
    "6/1": "Полный день",
    "2/2": "Сменный график",
    "Сменный": "Сменный график",
    "Свободный": "Гибкий график",
    "Гибкий": "Гибкий график",
}

def map_employment_name(api_value):
    """Приводит значение employment_form.name к формату БД"""
    if api_value is None:
        return None
    mapped = EMPLOYMENT_MAPPING.get(api_value)
    if mapped is None:
        print(f"  ⚠️ Новое значение employment_name: '{api_value}' (будет сохранено как есть)")
        return api_value
    return mapped

def map_schedule_name(vacancy):
    """
    Формирует schedule_name из новых полей API.
    Приоритет: удалёнка → work_schedule_by_days → None
    """
    # 1. Проверяем формат работы (приоритет)
    work_format = vacancy.get('work_format', [])
    if work_format and len(work_format) > 0:
        format_name = work_format[0].get('name')
        if format_name == 'Удалённо':
            return "Удаленная работа"
    
    # 2. Берём из work_schedule_by_days
    work_schedule = vacancy.get('work_schedule_by_days', [])
    if work_schedule and len(work_schedule) > 0:
        api_value = work_schedule[0].get('name')
        mapped = SCHEDULE_MAPPING.get(api_value)
        if mapped is None:
            print(f"  ⚠️ Новое значение schedule_name: '{api_value}' (будет сохранено как есть)")
            return api_value
        return mapped
    
    return None

# ========== НАСТРОЙКИ ДЛЯ GITHUB ACTIONS ==========
MAX_RETRIES = 2  # Минимум повторных попыток
REQUEST_TIMEOUT = 15  # Короткий таймаут для Actions
DELAY_BETWEEN_REQUESTS = 0.5  # Минимальная задержка

def create_session():
    """Создает сессию с авторизацией через токен приложения"""
    session = requests.Session()
    
    session.headers.update({
        "User-Agent": HH_USER_AGENT,
        "Authorization": f"Bearer {HH_ACCESS_TOKEN}",
        "Accept": "application/json",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://hh.ru/",
        "Origin": "https://hh.ru"
    })
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
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
            if 'X-Captcha-Required' in response.headers:
                print(f"   🔐 Требуется капча: {response.headers['X-Captcha-Required']}")
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
    """Быстрый запрос с расширенным логированием"""
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        
        if response.status_code == 403:
            print(f"❌ [BLOCKED] {context}: IP заблокирован (403)")
            if 'X-Captcha-Required' in response.headers:
                print(f"   🔐 Требуется капча: {response.headers['X-Captcha-Required']}")
            return None
        elif response.status_code == 400:
            print(f"⚠️ [BAD REQUEST] {context}: неверные параметры")
            try:
                error_data = response.json()
                print(f"   📝 {error_data.get('description', 'Нет описания')}")
            except:
                pass
            return None
        elif response.status_code == 429:
            print(f"⚠️ [RATE LIMIT] {context}: превышен лимит запросов")
            return None
        elif response.status_code != 200:
            print(f"⚠️ [ERROR] {context}: статус {response.status_code}")
            return None
            
        return response
        
    except requests.exceptions.Timeout:
        print(f"❌ [TIMEOUT] {context}: превышено время ожидания ({REQUEST_TIMEOUT}с)")
        return None
    except requests.exceptions.ConnectionError:
        print(f"❌ [CONNECTION] {context}: ошибка соединения")
        return None
    except Exception as e:
        print(f"❌ [EXCEPTION] {context}: {type(e).__name__}: {str(e)[:100]}")
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
        
        print(f"📋 Загружено {len(all_ids)} существующих ID из БД")
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
    sys.exit(0)

# Создаем сессию
session = create_session()
print("🔐 Сессия создана с авторизацией через токен приложения")

# Параметры сбора
analytics_roles = [10, 148, 150, 156, 164, 165]

# Даты
today = datetime.now().date()
yesterday = today - timedelta(days=10)
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
print(f"📤 Параметры: area={params['area']}, per_page={params['per_page']}, даты: {date_from} - {date_to}")

# Основной запрос
response = quick_request(session, f"{BASE_URL}/vacancies", params, "основной поиск")

if not response:
    print("\n❌ Не удалось получить данные")
    sys.exit(0)

data = response.json()
total_found = data.get('found', 0)
pages = min(data.get('pages', 0), 20)  # Ограничиваем 20 страницами для скорости

print(f"✅ Основной запрос успешен")
print(f"   Найдено: {total_found} вакансий")
print(f"   Доступно страниц: {data.get('pages', 0)} (обработаем: {pages})")

if total_found == 0:
    print("\n✅ Нет вакансий за период")
    sys.exit(0)

# Быстрый сбор ID
print(f"\n📑 Сбор ID вакансий (ограничение: {pages} страниц)...")
all_vacancy_ids = []

for page in range(pages):
    if page > 0:
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    params["page"] = page
    response = quick_request(session, f"{BASE_URL}/vacancies", params, f"страница {page+1}")
    
    if response:
        page_data = response.json()
        items = page_data.get('items', [])
        all_vacancy_ids.extend([item['id'] for item in items])
        print(f"  📄 Стр. {page+1}/{pages}: +{len(items)} ID (всего: {len(all_vacancy_ids)})")
    else:
        print(f"  ⚠️ Стр. {page+1}: ошибка, пропускаем")
        continue

# Фильтрация новых ID
new_ids = [vid for vid in all_vacancy_ids if vid not in existing_ids]
print(f"\n📊 ИТОГ СБОРА ID:")
print(f"  Всего собрано: {len(all_vacancy_ids)}")
print(f"  Уже в базе: {len(existing_ids)}")
print(f"  Новых: {len(new_ids)}")

if not new_ids:
    print("\n✅ Нет новых вакансий")
    sys.exit(0)

# Сбор деталей
print(f"\n📥 Сбор детальных данных для {len(new_ids)} вакансий...")
print(f"⏱️ Ожидаемое время: ~{len(new_ids) * DELAY_BETWEEN_REQUESTS / 60:.1f} мин")
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
            'employment_name': map_employment_name(vacancy.get('employment_form', {}).get('name')),
            'schedule_name': map_schedule_name(vacancy),
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
        
        # Вставка каждые 20 вакансий
        if len(vacancies_batch) >= 20:
            inserted = insert_vacancies_batch(vacancies_batch)
            inserted_total += inserted
            print(f"  💾 Вставлено в БД: {inserted} (всего: {inserted_total})")
            vacancies_batch = []
    else:
        errors.append(vac_id)
        print("❌")
    
    # Прогресс каждые 10 вакансий
    if (i + 1) % 10 == 0:
        elapsed = time.time() - start_time
        avg = elapsed / (i + 1)
        remaining = (len(new_ids) - (i + 1)) * avg
        success_rate = ((i + 1 - len(errors)) / (i + 1)) * 100
        print(f"  ⏱️ Прогресс: {i+1}/{len(new_ids)} | Успех: {success_rate:.0f}% | Вставлено: {inserted_total} | Осталось: {remaining/60:.1f} мин")

# Вставка остатка
if vacancies_batch:
    inserted = insert_vacancies_batch(vacancies_batch)
    inserted_total += inserted
    print(f"\n💾 Финальная вставка: {inserted} (всего: {inserted_total})")

total_time = time.time() - start_time

print("\n" + "=" * 80)
print("✅ ЗАВЕРШЕНО")
print("=" * 80)
print(f"📊 СТАТИСТИКА:")
print(f"  Обработано вакансий: {len(new_ids)}")
print(f"  Успешно вставлено: {inserted_total}")
print(f"  Ошибок: {len(errors)}")
if len(new_ids) > 0:
    print(f"  Процент успеха: {(inserted_total/len(new_ids))*100:.1f}%")
print(f"  Общее время: {total_time/60:.1f} мин")
if len(new_ids) > 0:
    print(f"  Среднее время на вакансию: {total_time/len(new_ids):.2f} сек")
print(f"🕐 Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if errors:
    print(f"\n⚠️ Ошибки при обработке ID:")
    for err_id in errors[:10]:
        print(f"  - {err_id}")
    if len(errors) > 10:
        print(f"  ... и ещё {len(errors) - 10}")
