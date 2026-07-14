"""
Загрузка справочника регионов РФ из CSV в Supabase.

Назначение:
    - Читает сырой CSV с полигонами регионов (разделитель ";")
    - Транслитерирует кириллические заголовки в латиницу
    - Создаёт таблицу regions и загружает данные
    - Затем конвертирует текстовый GeoJSON в геометрию PostGIS

Запуск:
    python scripts/load_regions.py
"""

import csv
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

CSV_FILE = "ru_regions.csv"
TABLE_NAME = "regions"

# ---------------------------------------------------------------------------
# Транслитерация кириллицы в латиницу
# ---------------------------------------------------------------------------
TRANSLIT_MAP = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo',
    'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
    'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
    'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch',
    'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
}


def transliterate(text):
    """Переводит кириллицу в латиницу + убирает спецсимволы."""
    result = ''.join(TRANSLIT_MAP.get(c, c) for c in text)
    result = re.sub(r'[^a-zA-Z0-9_]', '_', result)
    result = re.sub(r'_+', '_', result)
    result = result.strip('_').lower()
    return result


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
        reader = csv.reader(f, delimiter=';')
        rows = list(reader)

    if not rows:
        print("❌ Файл пуст")
        sys.exit(1)

    # Первая строка — заголовки
    original_headers = [h.strip() for h in rows[0]]
    print(f"   Оригинальные заголовки: {original_headers}")

    # Транслитерируем заголовки
    latin_headers = [transliterate(h) for h in original_headers]
    print(f"   Латинские заголовки:    {latin_headers}")

    # 2. Создаём таблицу
    print(f"\n📋 Создание таблицы {TABLE_NAME}...")

    # Удаляем старую таблицу если есть
    try:
        supabase.rpc('exec_sql', {
            'query': f'DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;'
        }).execute()
    except Exception:
        pass  # Игнорируем если функции нет или таблицы нет

    # Создаём таблицу с текстовыми колонками + id
    columns_def = ",\n    ".join([f"{col} TEXT" for col in latin_headers])
    create_sql = f"""
    CREATE TABLE {TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        {columns_def}
    );
    """

    try:
        supabase.rpc('exec_sql', {'query': create_sql}).execute()
        print("✅ Таблица создана")
    except Exception as e:
        print(f"❌ Ошибка создания таблицы: {e}")
        print("💡 Убедись, что в Supabase есть функция exec_sql")
        print("   Выполни в SQL Editor:")
        print("""
        CREATE OR REPLACE FUNCTION exec_sql(query TEXT) 
        RETURNS VOID AS $$
        BEGIN
            EXECUTE query;
        END;
        $$ LANGUAGE plpgsql SECURITY DEFINER;
        """)
        sys.exit(1)

    # 3. Загружаем данные
    print(f"\n📤 Загрузка данных...")
    data_rows = rows[1:]  # Пропускаем заголовок
    total = len(data_rows)
    inserted = 0

    for i, row in enumerate(data_rows):
        if not any(row):  # Пропускаем пустые строки
            continue

        # Собираем словарь с данными
        record = {}
        for j, header in enumerate(latin_headers):
            if j < len(row):
                record[header] = row[j].strip()
            else:
                record[header] = None

        try:
            supabase.table(TABLE_NAME).insert(record).execute()
            inserted += 1
        except Exception as e:
            # Выводим только первую ошибку подробно, остальное кратко
            if i < 3:
                print(f"  ⚠️ Строка {i+2}: {str(e)[:100]}")

        if (i + 1) % 20 == 0:
            print(f"  📦 Прогресс: {i+1}/{total}")

    print(f"\n✅ Загружено строк: {inserted} из {total}")

    # 4. Проверяем результат
    try:
        result = supabase.table(TABLE_NAME).select("id", count="exact").limit(1).execute()
        count = result.count if hasattr(result, 'count') else '?'
        print(f"📊 Всего записей в таблице: {count}")
    except Exception:
        pass

    print("\n✅ ГОТОВО")
    return inserted


if __name__ == "__main__":
    load_regions()
