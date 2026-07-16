"""
Скрипт проверки вакансий через ансамбль v5.
Берёт все вакансии с is_fake IS NULL, прогоняет через ансамбль, проставляет TRUE/FALSE.

Запуск:
    python scripts/clean_vacancies.py
"""

import os
import sys
import re
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Отсутствуют переменные окружения")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# =====================================================================
# СЛОВАРИ ИЗ АНСАМБЛЯ v5 (без изменений)
# =====================================================================

WEIGHT_ANALYTICS_MARKERS = 0.9
WEIGHT_NGRAM_KEYWORDS = 0.6
WEIGHT_NGRAM_ROLE = 0.3
WEIGHT_RULES = 0.7

fake_words = [
    'бульдозер', 'погрузчик', 'штабелер', 'экскаватор', 'ричтрак',
    'сварщик', 'токарь', 'фрезеровщик', 'сантехник', 'электрик',
    'охранник', 'уборщик', 'грузчик', 'кладовщик', 'комплектовщик',
    'кассир', 'продавец', 'официант', 'бармен', 'бариста',
    'повар', 'кондитер', 'пекарь', 'курьер', 'доставщик',
    'медсестра', 'медбрат', 'санитар', 'нян', 'воспитатель',
    'тракторист', 'комбайнер', 'дворник', 'разнорабочий',
    'машинист', 'крановщик', 'стропальщик', 'монтажник',
    'штукатур', 'маляр', 'плотник', 'каменщик', 'бетонщик',
    'упаковщик', 'фасовщик', 'сортировщик', 'маркировщик',
    'мойщик', 'посудомойщик', 'горничная',
    'рубщик', 'резчик', 'обвальщик', 'жиловщик',
    'продуктолог', 'веб-менеджер', 'bitrix', 'битрикс',
]

analytics_markers = [
    'аналит', 'данны', 'дата', 'дашборд', 'отчёт', 'отчет',
    'статистик', 'метрик', 'визуализац', 'исследова',
    'прогнозировани', 'модел', 'машинн', 'нейросет',
    'геоаналит', 'медиааналит',
    'analyt', 'analyst', 'analysis',
    'data', 'dashboard', 'report', 'metric',
    'statistic', 'research', 'visualiz', 'model',
    'machine learning', 'deep learning', 'neural',
    'scientist', 'science',
    'sql', ' bi', 'dwh', 'etl', 'python', 'tableau', 'power bi',
    'excel', 'spss', 'datalens',
    'google sheets', 'jupyter', 'colab',
    'looker', 'qlik', 'metabase', 'superset',
    'airflow', 'dbt', 'kafka', 'spark',
    'ml', 'nlp', 'computer vision', 'mlops',
    'tensorflow', 'pytorch', 'scikit', 'pandas', 'numpy',
    'xgboost', 'lightgbm', 'catboost',
    'llm', 'gpt', 'bert', 'transformers',
    'git', 'docker', 'kubernetes',
    'data engineer', 'data scientist', 'database engineer',
    'ai engineer', 'ai инженер', 'ml engineer',
    'архитектор данных', 'инженер данных',
    'data architect', 'data steward', 'data governance',
    'аналитик данных', 'бизнес-аналитик', 'системный аналитик',
    'продуктовый аналитик', 'финансовый аналитик',
    'маркетинговый аналитик', 'веб-аналитик',
    'a/b', 'ab test', 'когорт', 'cohort', 'воронк', 'funnel',
    'retention', 'конверси', 'conversion',
    'amplitude', 'mixpanel', 'google analytics',
    'юнит', 'unit', 'логистическ', 'logistic', 'скоринг', 'scoring',
    'сегментация', 'кластеризац', 'регрессия', 'корреляц',
    'факторный анализ', 'временной ряд', 'time series',
    'kpi', 'crm', 'erp', 'cjm',
    'agile', 'scrum', 'kanban',
    'big data', 'большие данные',
    'озеро данных', 'data lake', 'data warehouse',
    'сбор данных', 'очистка данных', 'предобработка',
    'data mining', 'data quality',
    'алгоритм', 'библиотек', 'фреймворк',
    'классификац', 'регресси', 'кластеризац',
    'ансамбл', 'градиентный бустинг', 'случайный лес',
    'romi', 'roi', 'cac', 'ltv', 'arpu', 'mau', 'dau',
    'churn', 'отток', 'удержание',
]

role_keywords = {
    'Аналитик': [
        'аналитик', 'аналитика', 'данные', 'отчет', 'отчёт', 'sql', 'excel',
        'статистика', 'исследование', 'анализ', 'анализировать',
        'мониторинг', 'сбор данных', 'обработка данных', 'расчёт',
        'показатели', 'метрики', 'kpi', 'сводные таблицы',
        'analyst', 'data analyst', 'data', 'report', 'research',
        'reporting', 'ad-hoc', 'insights', 'trends',
    ],
    'Системный аналитик': [
        'системный аналитик', 'тз', 'техническое задание', 'требования',
        'архитектура', 'bpmn', 'uml', 'idef0', 'erd',
        'rest api', 'soap', 'интеграция', 'проектирование', 'спецификация',
        'микросервисы', 'монолит', 'бэкенд', 'фронтенд',
        'протокол', 'контракт', 'схема данных',
        'system analyst', 'systems analyst', 'it analyst', 'technical analyst',
        'requirements', 'architecture', 'solution architect', 'enterprise architect',
        'api design', 'integration', 'specification', 'use case',
        'user story', 'acceptance criteria', 'definition of done',
    ],
    'Бизнес-аналитик': [
        'бизнес-аналитик', 'бизнес-аналитика', 'бизнес-процесс',
        'оптимизация', 'автоматизация', 'цифровизация',
        'требования', 'стейкхолдер', 'заказчик', 'презентация',
        'road map', 'стратегия', 'развитие продукта',
        'as-is', 'to-be', 'gap-анализ',
        'business analyst', 'business analysis', 'business process',
        'agile', 'scrum', 'kanban', 'safe',
        'user story', 'epic', 'backlog', 'sprint',
        'stakeholder', 'workshop', 'facilitation',
        'swot', 'pest', 'competitor analysis',
    ],
    'BI-аналитик, аналитик данных': [
        'bi', 'bi-аналитик', 'dashboard', 'дашборд',
        'power bi', 'tableau', 'datalens', 'metabase', 'superset',
        'визуализация', 'kpi', 'etl', 'dwh', 'хранилище данных',
        'витрина данных', 'озеро данных', 'data lake', 'data warehouse',
        'отчет', 'отчёт', 'база данных', 'sql', 'аналитика',
        'ad-hoc', 'датасет', 'семантический слой',
        'bi analyst', 'data analyst', 'business intelligence',
        'reporting', 'dax', 'm-language', 'power query',
        'data modeling', 'star schema', 'snowflake schema',
    ],
    'Продуктовый аналитик': [
        'продукт', 'продуктовый аналитик', 'продуктовая аналитика',
        'a/b', 'ab test', 'a/b-тест', 'сплит-тест',
        'когорта', 'когортный анализ', 'cohort',
        'retention', 'удержание', 'конверсия', 'conversion',
        'воронка', 'funnel', 'воронка продаж', 'воронка конверсии',
        'юнит', 'unit', 'unit-экономика', 'cac', 'ltv', 'arpu',
        'amplitude', 'mixpanel', 'google analytics', 'appmetrica',
        'метрика', 'metric', 'пользователь', 'user',
        'сегментация пользователей', 'поведенческий анализ',
        'product analyst', 'growth analyst', 'web analyst',
        'product metrics', 'north star metric', 'okr',
        'activation', 'engagement', 'onboarding',
    ],
    'Дата-сайентист': [
        'data scientist', 'дата сайентист', 'дата-сайентист',
        'машинное обучение', 'machine learning', 'ml',
        'deep learning', 'глубокое обучение', 'нейросеть', 'нейронная сеть',
        'nlp', 'обработка естественного языка', 'текст',
        'computer vision', 'компьютерное зрение', 'изображения',
        'python', 'pytorch', 'tensorflow', 'pandas', 'scikit-learn',
        'numpy', 'jupyter', 'xgboost', 'lightgbm', 'catboost',
        'статистика', 'математика', 'теория вероятностей',
        'алгоритм', 'модель', 'предикт', 'прогноз',
        'инженер данных', 'data engineer', 'данные', 'data',
        'ai engineer', 'ml engineer', 'mlops',
        'research scientist', 'applied scientist',
        'генеративные модели', 'llm', 'gpt', 'bert', 'transformers',
        'recommender system', 'рекомендательные системы',
        'временные ряды', 'time series', 'аномалии', 'anomaly detection',
        'machine learning engineer', 'deep learning engineer',
        'research engineer', 'algorithm developer',
        'quantitative analyst', 'quant researcher',
    ],
}

POSITIVE_PATTERNS = [
    r'аналит', r'данны', r'data', r'sql', r'python', r'таблиц',
    r'отчёт', r'отчет', r'report', r'дашборд', r'dashboard',
    r'статистик', r'statistic', r'исследова', r'research',
    r'инженер данных', r'data engineer', r'архитектор данных',
    r'ai engineer', r'ai инженер', r'ml engineer',
    r'визуализац', r'visualiz', r'модел', r'model',
    r'геоаналит', r'медиааналит', r'финансовый аналитик',
    r'машинное обучение', r'machine learning', r'deep learning',
    r'нейросет', r'neural', r'прогнозировани', r'predict',
    r'system[\s-]?analyst', r'business[\s-]?analyst', r'product[\s-]?analyst',
    r'data[\s-]?analyst', r'research[\s-]?scientist', r'data[\s-]?scientist',
    r'bi[\s-]?analyst', r'growth[\s-]?analyst', r'web[\s-]?analyst',
    r'(помощник|стажер|стажёр|младший|junior|assistant|intern|trainee).*аналит',
    r'аналит.*(помощник|стажер|стажёр|младший|junior|assistant|intern|trainee)',
    r'ассистент.*аналит', r'аналит.*ассистент',
    r'аналитик.*1с', r'1с.*аналитик',
]

NEGATIVE_PATTERNS = [
    r'продуктолог',
    r'менеджер по продукту', r'product manager',
    r'менеджер по развитию', r'business development',
    r'бизнес-партнёр', r'бизнес-партнер', r'business partner',
    r'бизнес-консультант', r'business consultant',
    r'бизнес-советник', r'бизнес-сценарист',
    r'бизнес-координатор', r'координатор',
    r'делопроизводитель',
    r'бухгалтер', r'accountant',
    r'кадров', r'hr[\s-]', r'рекрутер', r'recruiter',
    r'секретарь', r'secretary',
    r'bim-', r'пто',
    r'военнослужащ', r'военносл',
    r'продав', r'кассир', r'менеджер по продаж', r'sales manager',
    r'консультант(?!.*аналит).*1с', r'1с(?!.*аналит).*консультант',
    r'экономист(?!.*аналит)',
]

# =====================================================================
# ФУНКЦИИ АНСАМБЛЯ
# =====================================================================

def substring_match(text, markers):
    text_lower = text.lower()
    for marker in markers:
        if marker.lower() in text_lower:
            return True
    return False

def get_trigrams(text):
    text = text.lower()
    text = re.sub(r'[^а-яёa-z0-9]', '', text)
    trigrams = set()
    for i in range(len(text) - 2):
        trigrams.add(text[i:i+3])
    return trigrams

def jaccard_similarity(set1, set2):
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0

def ngram_score_with_keywords(name, role):
    name_trigrams = get_trigrams(name)
    keywords = role_keywords.get(role, [role])
    scores = []
    for kw in keywords:
        kw_trigrams = get_trigrams(kw)
        scores.append(jaccard_similarity(name_trigrams, kw_trigrams))
    top_n = sorted(scores, reverse=True)[:3]
    return np.mean(top_n) if top_n else 0.0

def rule_based_score(name):
    name_lower = name.lower()
    score = 0.0
    has_negative = False
    strong_positive = False

    for pattern in POSITIVE_PATTERNS:
        if re.search(pattern, name_lower):
            score += 0.25
            if re.search(r'(помощник|стажер|стажёр|младший|junior|assistant|intern|trainee|ассистент).*аналит', name_lower):
                strong_positive = True
            if re.search(r'аналит.*(помощник|стажер|стажёр|младший|junior|assistant|intern|trainee|ассистент)', name_lower):
                strong_positive = True

    for pattern in NEGATIVE_PATTERNS:
        if re.search(pattern, name_lower):
            score -= 0.5
            has_negative = True

    if has_negative:
        score -= 0.4
    if strong_positive:
        score += 0.3

    return max(-1.0, min(1.0, score))

def classify_vacancy(name, role):
    """Возвращает (is_fake, reason, score)"""
    name_lower = name.lower()
    
    # Стоп-слова → сразу FAKE
    if substring_match(name_lower, fake_words):
        return True, 'stop_word', -1.0
    
    scores = {}
    scores['analytics_marker'] = 1.0 if substring_match(name_lower, analytics_markers) else 0.0
    
    role_trigrams = get_trigrams(role)
    name_trigrams = get_trigrams(name)
    scores['ngram_role'] = jaccard_similarity(name_trigrams, role_trigrams)
    scores['ngram_keywords'] = ngram_score_with_keywords(name, role)
    scores['rules'] = rule_based_score(name)
    
    weighted = (
        scores['analytics_marker'] * WEIGHT_ANALYTICS_MARKERS +
        scores['ngram_role'] * WEIGHT_NGRAM_ROLE +
        scores['ngram_keywords'] * WEIGHT_NGRAM_KEYWORDS +
        scores['rules'] * WEIGHT_RULES
    )
    normalized = weighted / (WEIGHT_ANALYTICS_MARKERS + WEIGHT_NGRAM_ROLE + WEIGHT_NGRAM_KEYWORDS + WEIGHT_RULES)
    
    if normalized > 0.30:
        return False, f'OK_{normalized:.3f}', normalized
    else:
        return True, f'FAKE_{normalized:.3f}', normalized


# =====================================================================
# ОСНОВНАЯ ЛОГИКА
# =====================================================================

def clean_vacancies():
    print("=" * 60)
    print("🧹 ОЧИСТКА ВАКАНСИЙ (ансамбль v5)")
    print("=" * 60)
    
    # 1. Получаем непроверенные вакансии
    print("\n📥 Загрузка непроверенных вакансий...")
    
    unchecked = []
    page = 0
    page_size = 1000
    
    while True:
        res = supabase.table("vacancies") \
            .select("id, name, professional_role") \
            .is_("is_fake", "null") \
            .range(page * page_size, (page + 1) * page_size - 1) \
            .execute()
        
        if not res.data:
            break
        
        unchecked.extend(res.data)
        page += 1
    
    total = len(unchecked)
    print(f"   Найдено: {total} непроверенных")
    
    if total == 0:
        print("✅ Нечего проверять")
        return
    
    # 2. Проверяем и обновляем батчами
    print(f"\n🔍 Проверка...")
    
    batch_size = 50
    processed = 0
    fake_count = 0
    ok_count = 0
    
    for i in range(0, total, batch_size):
        batch = unchecked[i:i+batch_size]
        updates = []
        
        for vac in batch:
            name = vac.get('name', '')
            role = vac.get('professional_role', '')
            
            is_fake, reason, score = classify_vacancy(name, role)
            
            updates.append({
                'id': vac['id'],
                'is_fake': is_fake,
                'fake_reason': reason,
                'fake_score': score,
            })
            
            if is_fake:
                fake_count += 1
            else:
                ok_count += 1
        
        # Отправляем батч в Supabase
        try:
            supabase.table("vacancies").upsert(updates, on_conflict='id').execute()
            processed += len(batch)
        except Exception as e:
            print(f"  ⚠️ Ошибка батча: {e}")
        
        if processed % 500 == 0:
            print(f"  📦 Прогресс: {processed}/{total}")
    
    print(f"\n{'='*60}")
    print(f"✅ ГОТОВО")
    print(f"   Проверено: {processed}")
    print(f"   OK: {ok_count}")
    print(f"   FAKE: {fake_count}")


if __name__ == "__main__":
    clean_vacancies()
