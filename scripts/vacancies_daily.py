"""
Ежедневный сбор вакансий аналитических ролей с HeadHunter API.

Назначение:
    - Собирает вакансии за вчера и сегодня по заданным профессиональным ролям
    - Обогащает данные координатами, индустрией работодателя и профролью
    - Сохраняет в Supabase с защитой от дубликатов

Авторизация:
    Используется токен приложения (client_credentials flow),
    зарегистрированного на dev.hh.ru (заявка #7665, CustomMonitor).

Структура БД:
    49 полей в таблице vacancies, включая пересчитываемые триггерами
    зарплатные поля (rub, net, avg).

Запуск:
    - Автоматически: GitHub Actions, ежедневно в 21:00 MSK
    - Вручную: python scripts/vacancies_daily.py

Переменные окружения (обязательные):
    SUPABASE_URL              — URL проекта Supabase
    SUPABASE_SERVICE_ROLE_KEY — сервисный ключ Supabase
    HH_ACCESS_TOKEN           — токен приложения hh.ru
"""

import requests
from datetime import datetime, timedelta
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
from supabase import create_client, Client
from dotenv import load_dotenv
import sys

# ---------------------------------------------------------------------------
# Загрузка конфигурации и проверка обязательных переменных окружения
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HH_ACCESS_TOKEN = os.getenv("HH_ACCESS_TOKEN")

# Если хотя бы одной переменной нет — завершаем работу, чтобы не тратить
# время на заведомо обречённые запросы
if not all([SUPABASE_URL, SUPABASE_KEY, HH_ACCESS_TOKEN]):
    print("❌ Отсутствуют обязательные переменные окружения")
    print("   Проверь: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, HH_ACCESS_TOKEN")
    sys.exit(1)

# Подключение к Supabase (глобальный клиент — используется во всём скрипте)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_URL = "https://api.hh.ru"

# ---------------------------------------------------------------------------
# Идентификация клиента для hh.ru
# Формат: НазваниеПриложения/Версия (email)
# Требуется документацией hh.ru, почта нужна для обратной связи
# ---------------------------------------------------------------------------
HH_USER_AGENT = "CustomMonitor/1.0 (olegjerryborisov@yandex.ru)"

# ===========================================================================
# МАППЕРЫ ЗНАЧЕНИЙ ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ С БД
# ===========================================================================
# Проблема: hh.ru обновил API — изменились названия и структура некоторых полей.
# Решение: приводим новые значения к историческому формату, который уже
# используется в дашборде. Это позволяет не перестраивать все чарты (их ~30).

EMPLOYMENT_MAPPING = {
    # Новое значение (API)   → Историческое значение (БД)
    "Полная":                  "Полная занятость",
    "Частичная":               "Частичная занятость",
    "Проект":                  "Проектная работа",
    "Вахта":                   "Вахта",                # Новое, оставлено как есть
    "Подработка":              "Подработка",           # Новое, оставлено как есть
    "Стажировка":              "Стажировка",           # Новое, оставлено как есть
}

SCHEDULE_MAPPING = {
    # Новое значение (API)   → Историческое значение (БД)
    "5/2":                     "Полный день",
    "6/1":                     "Полный день",
    "2/1":                     "Сменный график",
    "2/2":                     "Сменный график",
    "1/3":                     "Сменный график",
    "4/2":                     "Сменный график",
    "Сменный":                 "Сменный график",
    "Свободный":               "Гибкий график",
    "Гибкий":                  "Гибкий график",
}


def map_employment_name(api_value):
    """
    Приводит значение employment_form.name из нового API к формату БД.
    
    Args:
        api_value: значение из поля employment_form.name (например, "Полная")
    
    Returns:
        str | None: значение в формате БД (например, "Полная занятость")
    """
    if api_value is None:
        return None

    mapped = EMPLOYMENT_MAPPING.get(api_value)
    if mapped is None:
        # Неизвестное значение — логируем, но сохраняем как есть
        print(f"  ⚠️ Новое значение employment_name: '{api_value}' (будет сохранено как есть)")
        return api_value
    return mapped


def map_schedule_name(vacancy):
    """
    Формирует schedule_name из новых полей API.
    
    Логика:
        1. Если формат работы «Удалённо» → "Удаленная работа" (приоритет)
        2. Иначе берём первый элемент из work_schedule_by_days
        3. Если ничего не нашли → None
    
    Args:
        vacancy: полный JSON-объект вакансии из API
    
    Returns:
        str | None: значение для поля schedule_name в БД
    """
    # Шаг 1: проверяем формат работы (имеет приоритет)
    work_format = vacancy.get('work_format', [])
    if work_format and len(work_format) > 0:
        format_name = work_format[0].get('name')
        if format_name == 'Удалённо':
            return "Удаленная работа"

    # Шаг 2: извлекаем график по дням (первый из массива)
    work_schedule = vacancy.get('work_schedule_by_days', [])
    if work_schedule and len(work_schedule) > 0:
        api_value = work_schedule[0].get('name')
        mapped = SCHEDULE_MAPPING.get(api_value)
        if mapped is None:
            print(f"  ⚠️ Новое значение schedule_name: '{api_value}' (будет сохранено как есть)")
            return api_value
        return mapped

    return None


# ===========================================================================
# СЕТЕВЫЕ НАСТРОЙКИ ДЛЯ GITHUB ACTIONS
# ===========================================================================
# Значения подобраны под ограничения облачных раннеров:
#   - короткий таймаут (чтобы не висеть на проблемных запросах)
#   - минимальные задержки (чтобы уложиться в лимиты Actions)
#   - ограниченное количество повторных попыток

MAX_RETRIES = 2              # Количество повторных попыток при ошибках сервера
REQUEST_TIMEOUT = 15         # Таймаут одного HTTP-запроса (секунд)
DELAY_BETWEEN_REQUESTS = 0.5 # Пауза между запросами (секунд)


def create_session():
    """
    Создаёт HTTP-сессию с авторизацией через токен приложения hh.ru.
    
    Особенности:
        - Токен передаётся в заголовке Authorization: Bearer {token}
        - User-Agent соответствует требованиям hh.ru (название + почта)
        - Настроен автоматический retry при серверных ошибках (429, 5xx)
    
    Returns:
        requests.Session: готовая к работе сессия
    """
    session = requests.Session()

    # Базовые заголовки для всех запросов
    session.headers.update({
        "User-Agent": HH_USER_AGENT,
        "Authorization": f"Bearer {HH_ACCESS_TOKEN}",
        "Accept": "application/json",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "Referer": "https://hh.ru/",
        "Origin": "https://hh.ru"
    })

    # Стратегия повторных попыток:
    #   - срабатывает при 429 (слишком много запросов) и 5xx (ошибки сервера)
    #   - всего 2 попытки с задержкой 1 секунда между ними
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
    """
    Проверяет доступность API перед началом основного сбора.
    
    Использует /dictionaries как самый лёгкий эндпоинт.
    Если он отвечает 403 — IP заблокирован, и весь дальнейший сбор
    не имеет смысла.
    
    Returns:
        bool: True если API доступен, False в противном случае
    """
    print("🔍 Проверка доступности API...")
    session = create_session()

    try:
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
    """
    Выполняет HTTP-запрос с расширенным логированием и обработкой ошибок.
    
    Все запросы к API hh.ru проходят через эту функцию.
    Она обеспечивает:
        - Единообразную обработку ошибок (403, 400, 429, таймауты)
        - Понятный вывод в логи GitHub Actions
        - Безопасное завершение при проблемах (возвращает None, а не падает)
    
    Args:
        session:    активная requests.Session
        url:        URL для запроса
        params:     GET-параметры (опционально)
        context:    человекочитаемое описание запроса для логов
    
    Returns:
        requests.Response | None: объект ответа или None при ошибке
    """
    try:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        # Разбираем статус ответа и логируем понятным языком
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


# ===========================================================================
# КЭШИ ДЛЯ УМЕНЬШЕНИЯ ЧИСЛА ЗАПРОСОВ К API
# ===========================================================================
# Координаты городов и индустрии работодателей не меняются в рамках одного
# запуска, поэтому сохраняем их в памяти после первого получения.

city_coords_cache = {}         # {area_id: {'lat': float, 'lng': float}}
employer_industries_cache = {} # {employer_id: [{'name': str, 'id': str}, ...]}


def get_city_coords(area_id):
    """
    Возвращает координаты города по его ID из справочника hh.ru.
    
    Использует кэш — при повторном запросе того же города
    не делает дополнительный HTTP-запрос.
    
    Args:
        area_id: ID города/региона из справочника hh.ru
    
    Returns:
        dict: {'lat': float, 'lng': float} или {'lat': None, 'lng': None}
    """
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
    """
    Возвращает список индустрий работодателя.
    
    Использует кэш — при повторном запросе того же работодателя
    не делает дополнительный HTTP-запрос.
    
    Args:
        employer_id: ID работодателя из hh.ru
    
    Returns:
        list: [{'name': str, 'id': str}, ...] или []
    """
    if employer_id in employer_industries_cache:
        return employer_industries_cache[employer_id]

    response = quick_request(session, f"{BASE_URL}/employers/{employer_id}", context=f"employer_{employer_id}")
    if response:
        employer_data = response.json()
        industries = employer_data.get('industries', [])
        employer_industries_cache[employer_id] = industries
        return industries

    return []


# ===========================================================================
# ФУНКЦИИ ОБОГАЩЕНИЯ ДАННЫХ
# ===========================================================================
# Каждая функция извлекает конкретный аспект из полного JSON вакансии.
# Вынесены в отдельные функции для читаемости и переиспользования.

def enrich_with_coordinates(vacancy):
    """
    Извлекает координаты из вакансии.
    
    Приоритет:
        1. Точный адрес (address.lat / address.lng)
        2. Центр города из справочника (/areas/{area_id})
        3. None — если ничего не нашли
    
    Returns:
        tuple: (lat, lng, source) — координаты и описание источника
    """
    address = vacancy.get('address', {})
    area = vacancy.get('area', {})

    # Точные координаты из адреса (лучший вариант)
    if address and address.get('lat') and address.get('lng'):
        return address.get('lat'), address.get('lng'), 'address'

    # Координаты центра города (запасной вариант)
    area_id = area.get('id')
    if area_id:
        coords = get_city_coords(area_id)
        return coords.get('lat'), coords.get('lng'), f'area_{area_id}'

    return None, None, 'none'


def enrich_with_industries(vacancy):
    """
    Извлекает основную индустрию работодателя.
    
    Берёт только первую индустрию из массива.
    Для получения данных делает отдельный запрос к /employers/{id}.
    
    Returns:
        tuple: (industry_name, industry_id) или (None, None)
    """
    employer = vacancy.get('employer', {})
    employer_id = employer.get('id')
    if employer_id:
        industries = get_employer_industries(employer_id)
        if industries:
            return industries[0].get('name'), industries[0].get('id')
    return None, None


def enrich_with_professional_roles(vacancy):
    """
    Извлекает основную профессиональную роль из вакансии.
    
    Берёт только первую роль из массива professional_roles.
    
    Returns:
        tuple: (role_name, role_id) или (None, None)
    """
    roles = vacancy.get('professional_roles', [])
    if roles:
        return roles[0].get('name'), roles[0].get('id')
    return None, None


# ===========================================================================
# РАБОТА С БАЗОЙ ДАННЫХ SUPABASE
# ===========================================================================

def get_existing_ids():
    """
    Загружает все существующие ID вакансий из Supabase.
    
    Использует пагинацию (по 1000 записей), так как в базе уже больше
    3000 вакансий. Возвращает множество для быстрой проверки членства.
    
    Returns:
        set: множество строковых ID вакансий
    """
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
    """
    Вставляет батч вакансий в Supabase с проверкой на дубликаты.
    
    Алгоритм:
        1. Загружает все существующие ID из БД
        2. Фильтрует — оставляет только те, чьих ID ещё нет
        3. Вставляет отфильтрованные строки
    
    Это идемпотентная операция: повторная вставка того же батча
    не создаст дубликатов.
    
    Args:
        rows: список словарей с данными вакансий
    
    Returns:
        int: количество реально вставленных строк
    """
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
        print(f"⚠️ Ошибка вставки в Supabase: {e}")
        return 0


# ===========================================================================
# ОСНОВНОЙ ПАЙПЛАЙН СБОРА ДАННЫХ
# ===========================================================================
# Порядок работы:
#   1. Проверить доступность API (check_ip_block)
#   2. Получить список ID вакансий за период (поиск с пагинацией)
#   3. Отфильтровать новые ID (которых нет в БД)
#   4. Для каждого нового ID загрузить полную информацию
#   5. Обогатить данные (координаты, индустрия, профроль)
#   6. Вставить батчами в Supabase

print("=" * 80)
print("🤖 GITHUB ACTIONS - СБОР ВАКАНСИЙ HH.RU")
print("=" * 80)
print(f"🕐 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Шаг 1: проверка доступности API
if not check_ip_block():
    print("\n❌ СКРИПТ ОСТАНОВЛЕН: API недоступен")
    sys.exit(0)

# Создаём сессию для всех последующих запросов
session = create_session()
print("🔐 Сессия создана с авторизацией через токен приложения")

# -----------------------------------------------------------------------
# Параметры поиска: какие вакансии ищем
# -----------------------------------------------------------------------
analytics_roles = [10, 148, 150, 156, 164, 165]
# 10  — Аналитик
# 148 — Системный аналитик
# 150 — Бизнес-аналитик
# 156 — BI-аналитик, аналитик данных
# 164 — Продуктовый аналитик
# 165 — Дата-сайентист

# Период сбора: вчера + сегодня
# date_to = завтра, потому что API включает дату "до" невключительно
today = datetime.now().date()
yesterday = today - timedelta(days=1)
date_from = yesterday.strftime("%Y-%m-%d")
date_to = (today + timedelta(days=1)).strftime("%Y-%m-%d")

print("\n" + "=" * 80)
print("📊 ПАРАМЕТРЫ ПОИСКА")
print("=" * 80)
print(f"Период: {date_from} - {date_to}")
print(f"Роли: {analytics_roles}")

# Загружаем существующие ID (чтобы не собирать дубли)
existing_ids = get_existing_ids()
print(f"В базе: {len(existing_ids)} вакансий")

# -----------------------------------------------------------------------
# Шаг 2: поиск вакансий — получаем список ID
# -----------------------------------------------------------------------
params = {
    "professional_role": analytics_roles,
    "only_with_salary": True,   # Только вакансии с указанной зарплатой
    "area": 113,                # Россия
    "date_from": date_from,
    "date_to": date_to,
    "per_page": 100,            # Максимальный размер страницы
    "page": 0
}

print("\n" + "=" * 80)
print("🔍 ПОИСК ВАКАНСИЙ")
print("=" * 80)
print(f"📤 Параметры: area={params['area']}, per_page={params['per_page']}, даты: {date_from} - {date_to}")

# Первый запрос — получаем метаданные (сколько всего найдено, сколько страниц)
response = quick_request(session, f"{BASE_URL}/vacancies", params, "основной поиск")

if not response:
    print("\n❌ Не удалось получить данные")
    sys.exit(0)

data = response.json()
total_found = data.get('found', 0)
# Ограничиваем 20 страницами (2000 вакансий) — лимит API
pages = min(data.get('pages', 0), 20)

print(f"✅ Основной запрос успешен")
print(f"   Найдено: {total_found} вакансий")
print(f"   Доступно страниц: {data.get('pages', 0)} (обработаем: {pages})")

if total_found == 0:
    print("\n✅ Нет вакансий за период")
    sys.exit(0)

# -----------------------------------------------------------------------
# Сбор всех ID (пагинация по страницам)
# -----------------------------------------------------------------------
print(f"\n📑 Сбор ID вакансий (ограничение: {pages} страниц)...")
all_vacancy_ids = []

for page in range(pages):
    # Пауза между страницами (кроме первой — её уже получили)
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

# -----------------------------------------------------------------------
# Шаг 3: фильтрация — оставляем только новые ID
# -----------------------------------------------------------------------
new_ids = [vid for vid in all_vacancy_ids if vid not in existing_ids]
print(f"\n📊 ИТОГ СБОРА ID:")
print(f"  Всего собрано: {len(all_vacancy_ids)}")
print(f"  Уже в базе: {len(existing_ids)}")
print(f"  Новых: {len(new_ids)}")

if not new_ids:
    print("\n✅ Нет новых вакансий")
    sys.exit(0)

# -----------------------------------------------------------------------
# Шаг 4-5: загрузка полных данных и обогащение
# -----------------------------------------------------------------------
print(f"\n📥 Сбор детальных данных для {len(new_ids)} вакансий...")
print(f"⏱️ Ожидаемое время: ~{len(new_ids) * DELAY_BETWEEN_REQUESTS / 60:.1f} мин")

vacancies_batch = []   # Накопитель для батчевой вставки
errors = []            # Список ID, которые не удалось загрузить
start_time = time.time()
inserted_total = 0

for i, vac_id in enumerate(new_ids):
    # Пауза между запросами (уважаем ограничения API)
    if i > 0:
        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"[{i+1}/{len(new_ids)}] {vac_id}", end=" ")

    # Загружаем полную информацию по одной вакансии
    response = quick_request(session, f"{BASE_URL}/vacancies/{vac_id}", context=f"вакансия {vac_id}")

    if response:
        vacancy = response.json()

        # --- Обогащение данных ---
        lat, lng, coords_source = enrich_with_coordinates(vacancy)
        main_industry, main_industry_id = enrich_with_industries(vacancy)
        main_role, main_role_id = enrich_with_professional_roles(vacancy)

        # Корректировка: в API Беларуси валюта BYR, но в таблице курсов — BYN
        salary_currency = vacancy.get('salary', {}).get('currency') if vacancy.get('salary') else None
        if salary_currency == 'BYR':
            salary_currency = 'BYN'

        # --- Формирование строки для вставки в БД ---
        # Структура должна строго соответствовать таблице vacancies (49 полей).
        # Изменение порядка или названий полей сломает дашборд.
        row = {
            # Базовые поля
            'id': vacancy.get('id'),
            'name': vacancy.get('name'),
            'published_at': vacancy.get('published_at'),
            'created_at': vacancy.get('created_at'),
            'initial_created_at': vacancy.get('initial_created_at'),
            'alternate_url': vacancy.get('alternate_url'),

            # Зарплата (в исходной валюте, пересчёт в рубли — триггерами в БД)
            'salary_from': vacancy.get('salary', {}).get('from') if vacancy.get('salary') else None,
            'salary_to': vacancy.get('salary', {}).get('to') if vacancy.get('salary') else None,
            'salary_currency': salary_currency,
            'salary_gross': vacancy.get('salary', {}).get('gross') if vacancy.get('salary') else None,

            # География
            'area_id': vacancy.get('area', {}).get('id'),
            'area_name': vacancy.get('area', {}).get('name'),
            'lat': lat,
            'lng': lng,
            'coords_source': coords_source,
            'address_raw': vacancy.get('address', {}).get('raw') if vacancy.get('address') else None,

            # Работодатель
            'employer_id': vacancy.get('employer', {}).get('id'),
            'employer_name': vacancy.get('employer', {}).get('name'),
            'employer_accredited_it': vacancy.get('employer', {}).get('accredited_it_employer'),
            'employer_trusted': vacancy.get('employer', {}).get('trusted'),
            'employer_main_industry': main_industry,
            'employer_main_industry_id': main_industry_id,

            # Профессиональная роль (первая из списка)
            'professional_role': main_role,
            'professional_role_id': main_role_id,

            # Требования
            'experience_id': vacancy.get('experience', {}).get('id'),
            'experience_name': vacancy.get('experience', {}).get('name'),

            # Занятость и график (с маппингом на исторические значения)
            'employment_name': map_employment_name(vacancy.get('employment_form', {}).get('name')),
            'schedule_name': map_schedule_name(vacancy),

            # Условия работы
            'accept_temporary': vacancy.get('accept_temporary'),
            'accept_labor_contract': vacancy.get('accept_labor_contract'),
            'internship': vacancy.get('internship'),
            'night_shifts': vacancy.get('night_shifts'),

            # Формат работы (массивы объединяем в строки через запятую)
            'work_format': ', '.join([f.get('name', '') for f in vacancy.get('work_format', [])]),
            'working_hours': ', '.join([h.get('name', '') for h in vacancy.get('working_hours', [])]),
            'work_schedule_by_days': ', '.join([s.get('name', '') for s in vacancy.get('work_schedule_by_days', [])]),

            # Навыки
            'key_skills': ', '.join([s.get('name', '') for s in vacancy.get('key_skills', [])]),

            # Тесты и статусы
            'has_test': vacancy.get('has_test'),
            'test_required': vacancy.get('test', {}).get('required') if vacancy.get('test') else None,
            'archived': vacancy.get('archived'),
            'response_letter_required': vacancy.get('response_letter_required'),
            'premium': vacancy.get('premium'),
            'billing_type': vacancy.get('billing_type', {}).get('id') if vacancy.get('billing_type') else None,
        }

        vacancies_batch.append(row)
        print("✅")

        # -------------------------------------------------------------------
        # Шаг 6: батчевая вставка (каждые 20 вакансий)
        # -------------------------------------------------------------------
        # Вставляем не по одной, а пачками — так быстрее и меньше запросов к БД
        if len(vacancies_batch) >= 20:
            inserted = insert_vacancies_batch(vacancies_batch)
            inserted_total += inserted
            print(f"  💾 Вставлено в БД: {inserted} (всего: {inserted_total})")
            vacancies_batch = []
    else:
        errors.append(vac_id)
        print("❌")

    # -------------------------------------------------------------------
    # Логирование прогресса (каждые 10 вакансий)
    # -------------------------------------------------------------------
    if (i + 1) % 10 == 0:
        elapsed = time.time() - start_time
        avg = elapsed / (i + 1)
        remaining = (len(new_ids) - (i + 1)) * avg
        success_rate = ((i + 1 - len(errors)) / (i + 1)) * 100
        print(f"  ⏱️ Прогресс: {i+1}/{len(new_ids)} | Успех: {success_rate:.0f}% | Вставлено: {inserted_total} | Осталось: {remaining/60:.1f} мин")

# -----------------------------------------------------------------------
# Вставка остатков (если последний батч был меньше 20)
# -----------------------------------------------------------------------
if vacancies_batch:
    inserted = insert_vacancies_batch(vacancies_batch)
    inserted_total += inserted
    print(f"\n💾 Финальная вставка: {inserted} (всего: {inserted_total})")

# -----------------------------------------------------------------------
# Финальный отчёт
# -----------------------------------------------------------------------
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
