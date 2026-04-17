import requests
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import json
import random

load_dotenv()

# Подключение к Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://api.hh.ru"

# ========== НОВЫЕ НАСТРОЙКИ ДЛЯ ОБХОДА БЛОКИРОВКИ ==========
def create_session_with_rotation():
    """Создает сессию с ротацией User-Agent и дополнительными заголовками"""
    session = requests.Session()
    
    # Расширенный список User-Agent для ротации
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0"
    ]
    
    session.headers.update({
        "User-Agent": random.choice(user_agents),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://hh.ru/",
        "Origin": "https://hh.ru",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    })
    
    # Настройка retry с увеличенными задержками
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,  # Увеличиваем задержку между попытками
        status_forcelist=[429, 500, 502, 503, 504, 403],  # Добавляем 403 в retry
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

def add_delay():
    """Случайная задержка для имитации человеческого поведения"""
    delay = random.uniform(1.0, 3.0)
    time.sleep(delay)

def get_with_retry(session, url, params=None, max_attempts=3):
    """Запрос с повторными попытками и сменой User-Agent при 403"""
    for attempt in range(max_attempts):
        try:
            # Меняем User-Agent при повторных попытках
            if attempt > 0:
                user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                ]
                session.headers.update({"User-Agent": random.choice(user_agents)})
                print(f"   🔄 Попытка {attempt + 1}/{max_attempts} с новым User-Agent")
                add_delay()  # Добавляем задержку перед повторной попыткой
            
            response = session.get(url, params=params, timeout=30)
            
            if response.status_code == 403:
                print(f"   ⚠️ Получен 403 Forbidden (попытка {attempt + 1})")
                
                # Проверяем наличие куки с капчей
                if 'captcha' in response.text.lower() or 'ddos-guard' in response.headers.get('Server', '').lower():
                    print("   🛡️ Обнаружена защита DDoS-Guard. Увеличиваем задержку...")
                    time.sleep(random.uniform(5, 10))  # Длинная пауза
                
                if attempt < max_attempts - 1:
                    continue
            
            return response
            
        except Exception as e:
            print(f"   ❌ Ошибка запроса: {e}")
            if attempt < max_attempts - 1:
                time.sleep(random.uniform(2, 5))
                continue
            raise
    
    return response

# Создаем сессию с защитой от блокировки
session = create_session_with_rotation()

# Кэши
city_coords_cache = {}
employer_industries_cache = {}

def diagnose_response(response, context=""):
    """Детальная диагностика ответа API"""
    print(f"\n🔍 ДИАГНОСТИКА ОТВЕТА [{context}]")
    print(f"   Status Code: {response.status_code}")
    print(f"   URL: {response.url}")
    
    # Проверка на блокировку
    if response.status_code == 403:
        print("   🚫 ДОСТУП ЗАБЛОКИРОВАН (403 Forbidden)")
        print(f"   Server: {response.headers.get('Server', 'N/A')}")
        
        if 'ddos-guard' in response.headers.get('Server', '').lower():
            print("   ⚠️ Сработала защита DDoS-Guard от hh.ru")
            print("   💡 РЕШЕНИЕ: Нужно использовать прокси или VPN")
            print("   💡 ИЛИ: Подождать 30-60 минут и попробовать снова")
        
        try:
            error_data = response.json()
            print(f"   Ошибка API: {json.dumps(error_data, ensure_ascii=False)}")
        except:
            pass
    
    # Проверка на капчу
    if 'captcha' in response.text.lower():
        print("   🤖 ОБНАРУЖЕНА КАПЧА!")
        print("   💡 Требуется ручное решение капчи в браузере")
    
    return response

def test_connection():
    """Тестирование соединения с разными методами"""
    print("\n🧪 ТЕСТИРОВАНИЕ СОЕДИНЕНИЯ С HH.RU")
    
    # Тест 1: Прямой запрос к главной странице
    print("\n1. Проверка доступности hh.ru:")
    try:
        test_session = requests.Session()
        test_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        response = test_session.get("https://hh.ru", timeout=10)
        print(f"   Статус: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ Сайт доступен")
        else:
            print(f"   ⚠️ Сайт вернул {response.status_code}")
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
    
    # Тест 2: API справочников (обычно менее защищен)
    print("\n2. Проверка API справочников:")
    try:
        response = session.get(f"{BASE_URL}/dictionaries", timeout=10)
        print(f"   Статус: {response.status_code}")
        if response.status_code == 200:
            print("   ✅ API справочников доступен")
        else:
            print(f"   ⚠️ Статус: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
    
    # Тест 3: API вакансий с паузой
    print("\n3. Проверка API вакансий (с паузой):")
    time.sleep(2)
    try:
        response = get_with_retry(session, f"{BASE_URL}/vacancies", params={"per_page": 1})
        diagnose_response(response, "тест вакансий")
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ API вакансий доступен. Найдено: {data.get('found', 0)}")
        elif response.status_code == 403:
            print("   🚫 API вакансий заблокирован!")
            print("   💡 ВАШ IP ВРЕМЕННО ЗАБЛОКИРОВАН HH.RU")
            print("   💡 Варианты решения:")
            print("      1. Подождать 1-2 часа")
            print("      2. Использовать VPN/прокси")
            print("      3. Запустить скрипт с другого IP")
            return False
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return False
    
    return True

def get_city_coords(area_id):
    if area_id in city_coords_cache:
        return city_coords_cache[area_id]
    try:
        response = get_with_retry(session, f"{BASE_URL}/areas/{area_id}")
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
        response = get_with_retry(session, f"{BASE_URL}/employers/{employer_id}")
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
    """Получает список всех существующих ID вакансий из базы"""
    try:
        all_ids = []
        page = 0
        page_size = 1000
        
        while True:
            response = supabase.table("vacancies").select("id").range(page * page_size, (page + 1) * page_size - 1).execute()
            if not response.data:
                break
            all_ids.extend([row['id'] for row in response.data])
            if len(response.data) < page_size:
                break
            page += 1
        
        return set(all_ids)
    except Exception as e:
        print(f"Ошибка при получении существующих ID: {e}")
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
        
        supabase.table("vacancies").insert(new_rows).execute()
        return len(new_rows)
    except Exception as e:
        print(f"Ошибка при вставке: {e}")
        return 0

# ========== ОСНОВНОЙ КОД ==========

print("=" * 80)
print("ЕЖЕДНЕВНЫЙ СБОР ВАКАНСИЙ (С ЗАЩИТОЙ ОТ БЛОКИРОВКИ)")
print("=" * 80)

# Тестируем соединение
if not test_connection():
    print("\n❌ НЕТ ДОСТУПА К API. Скрипт остановлен.")
    print("📧 Рекомендуется настроить прокси или VPN для обхода блокировки.")
    exit(1)

print("\n" + "=" * 80)
print("ПАРАМЕТРЫ ПОИСКА")
print("=" * 80)

# Параметры сбора
analytics_roles = [10, 148, 150, 156, 164, 165]

# Даты
today = datetime.now().date()
yesterday = today - timedelta(days=1)
date_from = yesterday.strftime("%Y-%m-%d")
date_to = (today + timedelta(days=1)).strftime("%Y-%m-%d")

print(f"Период: {date_from} - {date_to}")
print(f"Роли: {analytics_roles}")

# Получаем существующие ID
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

print("\n" + "=" * 80)
print("ЗАПРОС К API")
print("=" * 80)

# Добавляем задержку перед основным запросом
print("\n⏳ Пауза 5 секунд перед запросом...")
time.sleep(5)

# Основной запрос с защитой
print("Выполнение основного запроса...")
response = get_with_retry(session, f"{BASE_URL}/vacancies", params=params, max_attempts=3)
diagnose_response(response, "основной запрос")

if response.status_code != 200:
    print("\n❌ Не удалось получить данные от API")
    if response.status_code == 403:
        print("💡 ВАШ IP ЗАБЛОКИРОВАН. Рекомендации:")
        print("   1. Подождите 1-2 часа и запустите скрипт снова")
        print("   2. Используйте VPN (например, ProtonVPN бесплатный)")
        print("   3. Настройте прокси в скрипте")
    exit(1)

data = response.json()
total_found = data.get('found', 0)
pages = min(data.get('pages', 0), 20)

print(f"\n📊 РЕЗУЛЬТАТЫ ПОИСКА:")
print(f"   Всего вакансий: {total_found}")
print(f"   Доступно страниц: {pages}")

if total_found == 0:
    print("\n✅ Нет новых вакансий за период")
    exit(0)

# Сбор вакансий (остальная часть без изменений)
all_vacancy_ids = []
for page in range(pages):
    params["page"] = page
    try:
        add_delay()  # Добавляем случайную задержку между запросами
        response = get_with_retry(session, f"{BASE_URL}/vacancies", params=params)
        
        if response.status_code == 200:
            page_data = response.json()
            for item in page_data.get('items', []):
                all_vacancy_ids.append(item['id'])
            print(f"📄 Страница {page+1}/{pages}: +{len(page_data.get('items', []))} ID")
        else:
            print(f"⚠️ Ошибка страницы {page+1}: статус {response.status_code}")
            continue
            
    except Exception as e:
        print(f"❌ Ошибка страницы {page+1}: {e}")
        continue

# Фильтрация новых ID
new_ids = [vid for vid in all_vacancy_ids if vid not in existing_ids]
print(f"\n📊 ИТОГИ СБОРА:")
print(f"   Всего ID: {len(all_vacancy_ids)}")
print(f"   Новых ID: {len(new_ids)}")

if not new_ids:
    print("✅ Нет новых вакансий для загрузки")
    exit(0)

print("\n🔄 Сбор полных данных...")
vacancies_batch = []
errors = []
start_total = time.time()
inserted_total = 0

for i, vac_id in enumerate(new_ids):
    print(f"[{i+1}/{len(new_ids)}] {vac_id}")
    
    try:
        add_delay()  # Задержка между запросами деталей
        response = get_with_retry(session, f"{BASE_URL}/vacancies/{vac_id}")
        
        if response.status_code == 200:
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
            print(f"  ✅ OK (в батче: {len(vacancies_batch)})")
            
            if len(vacancies_batch) >= 50:
                inserted = insert_vacancies_batch(vacancies_batch)
                inserted_total += inserted
                print(f"  💾 Вставлено: {inserted}")
                vacancies_batch = []
        else:
            errors.append(vac_id)
            print(f"  ❌ Статус: {response.status_code}")
            
    except Exception as e:
        errors.append(vac_id)
        print(f"  ❌ Ошибка: {e}")
    
    if (i + 1) % 50 == 0:
        elapsed = time.time() - start_total
        avg = elapsed / (i + 1)
        remaining = (len(new_ids) - (i + 1)) * avg
        print(f"--- Прогресс: {i+1}/{len(new_ids)} | Вставлено: {inserted_total} | Осталось: {remaining/60:.1f} мин ---")

# Вставка остатка
if vacancies_batch:
    inserted = insert_vacancies_batch(vacancies_batch)
    inserted_total += inserted
    print(f"\n💾 Финальная вставка: {inserted}")

total_time = time.time() - start_total
print("\n" + "=" * 80)
print("✅ ЗАВЕРШЕНО")
print("=" * 80)
print(f"Обработано: {len(new_ids)}")
print(f"Вставлено: {inserted_total}")
print(f"Ошибок: {len(errors)}")
print(f"Время: {total_time/60:.1f} мин")
