"""
Загрузка справочника регионов РФ из CSV в Supabase.
Загружаются ТОЛЬКО полигоны (coords_type = ru_regions_poly).
"""

import sys
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Отсутствуют SUPABASE_URL или SUPABASE_SERVICE_ROLE_KEY")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CSV_FILE = "data/ru_regions.csv"
TABLE_NAME = "regions"
HEADERS = ['name', 'type', 'id', 'region', 'coords_type', 'coords']


def load_regions():
    print("=" * 60)
    print("🏗️  ЗАГРУЗКА РЕГИОНОВ РФ В SUPABASE")
    print("=" * 60)

    # 1. Читаем CSV
    print(f"\n📂 Чтение {CSV_FILE}...")

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    data_lines = lines[1:]  # Пропускаем заголовок
    total = len(data_lines)
    print(f"   Всего строк данных: {total}")

    # 2. Фильтруем — только полигоны
    poly_lines = []
    points_lines = []
    
    for line in data_lines:
        if 'ru_regions_poly' in line:
            poly_lines.append(line)
        elif 'ru_regions_points' in line:
            points_lines.append(line)
    
    print(f"   Полигонов: {len(poly_lines)}")
    print(f"   Точек: {len(points_lines)}")
    print(f"   → Загружаем только полигоны")

    # 3. Очищаем таблицу
    print(f"\n🧹 Очистка таблицы {TABLE_NAME}...")
    try:
        supabase.table(TABLE_NAME).delete().neq("id", "0").execute()
        print("   ✅ Таблица очищена")
    except Exception as e:
        print(f"   ⚠️ {e}")

    # 4. Загружаем полигоны
    print(f"\n📤 Загрузка полигонов...")
    
    inserted = 0
    errors = 0

    for i, line in enumerate(poly_lines, start=1):
        line = line.strip()
        if not line:
            continue

        parts = line.split(';')
        
        record = {}
        for j, header in enumerate(HEADERS):
            if j < len(parts):
                val = parts[j].strip()
                if val.startswith('"') and val.endswith('"'):
                    val = val[1:-1]
                record[header] = val
            else:
                record[header] = None

        try:
            supabase.table(TABLE_NAME).upsert(record, on_conflict='id').execute()
            inserted += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  ⚠️ Строка {i}: id={record.get('id')} — {str(e)[:120]}")

        if i % 20 == 0:
            print(f"  📦 Прогресс: {i}/{len(poly_lines)} (успешно: {inserted}, ошибок: {errors})")

    # 5. Итоги
    print(f"\n{'='*60}")
    print(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА")
    print(f"{'='*60}")
    print(f"  Полигонов загружено: {inserted}")
    print(f"  Ошибок:              {errors}")

    try:
        result = supabase.table(TABLE_NAME).select("*", count="exact").limit(1).execute()
        print(f"  Записей в таблице:   {result.count}")
    except Exception:
        pass

    print("\n✅ ГОТОВО")


if __name__ == "__main__":
    load_regions()
