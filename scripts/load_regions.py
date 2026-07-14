"""
Загрузка справочника регионов РФ из CSV в Supabase.

Назначение:
    - Читает сырой CSV с полигонами регионов (разделитель ";")
    - Загружает данные в готовую таблицу regions
    - При повторе — перезаписывает существующие записи (UPSERT по id)

Запуск:
    python scripts/load_regions.py
"""

import sys
import os
import re
from supabase import create_client, Client
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Загрузка конфигурации
# ---------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Отсутствуют SUPABASE_URL или SUPABASE_SERVICE_ROLE_KEY")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CSV_FILE = "data/ru_regions.csv"
TABLE_NAME = "regions"

# Заголовки из CSV (уже на латинице, транслитерация не нужна)
HEADERS = ['name', 'type', 'id', 'region', 'coords_type', 'coords']


# ---------------------------------------------------------------------------
# Основная логика
# ---------------------------------------------------------------------------
def load_regions():
    print("=" * 60)
    print("🏗️  ЗАГРУЗКА РЕГИОНОВ РФ В SUPABASE")
    print("=" * 60)

    # 1. Читаем CSV
    print(f"\n📂 Чтение {CSV_FILE}...")

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"   Прочитано строк: {len(lines)}")

    # Пропускаем заголовок (первая строка)
    data_lines = lines[1:]
    
    # 2. Парсим данные
    print("\n📤 Парсинг и загрузка данных...")
    
    total = len(data_lines)
    inserted = 0
    errors = 0
    error_ids = []

    for i, line in enumerate(data_lines, start=1):
        line = line.strip()
        if not line:
            continue

        # Разбиваем строку на поля по разделителю ;
        parts = line.split(';')
        
        # Собираем словарь с данными
        record = {}
        for j, header in enumerate(HEADERS):
            if j < len(parts):
                val = parts[j].strip()
                # Убираем внешние кавычки если есть
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                record[header] = val
            else:
                record[header] = None

        try:
            # UPSERT: вставить или обновить по id
            supabase.table(TABLE_NAME).upsert(record, on_conflict='id').execute()
            inserted += 1
        except Exception as e:
            errors += 1
            error_ids.append(record.get('id', '?'))
            if errors <= 5:
                print(f"  ⚠️ Строка {i}: id={record.get('id')} — {str(e)[:150]}")

        if i % 20 == 0:
            print(f"  📦 Прогресс: {i}/{total} (успешно: {inserted}, ошибок: {errors})")

    # 3. Итоги
    print(f"\n{'='*60}")
    print(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА")
    print(f"{'='*60}")
    print(f"  Всего строк в CSV:    {total}")
    print(f"  Успешно загружено:    {inserted}")
    print(f"  Ошибок:               {errors}")
    
    if error_ids:
        print(f"  Проблемные id: {error_ids[:10]}")

    # 4. Проверяем результат
    try:
        result = supabase.table(TABLE_NAME).select("*", count="exact").limit(1).execute()
        print(f"\n📊 Всего записей в таблице: {result.count}")
    except Exception:
        pass

    print("\n✅ ГОТОВО")
    return inserted


if __name__ == "__main__":
    load_regions()
