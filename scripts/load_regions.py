"""
Загрузка справочника регионов РФ из CSV в Supabase.
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
    
    print(f"   Прочитано строк: {len(lines)}")

    # Пропускаем заголовок
    data_lines = lines[1:]
    
    # 2. Отладка: проверим первую строку
    print("\n🔍 Отладка первой строки:")
    first_line = data_lines[0].strip()
    print(f"   Длина строки: {len(first_line)} символов")
    
    parts = first_line.split(';')
    print(f"   Полей после split(';'): {len(parts)}")
    
    if len(parts) >= 6:
        coords_raw = parts[5]
        print(f"   coords_raw начинается с: {coords_raw[:80]}")
        print(f"   coords_raw заканчивается на: {coords_raw[-40:]}")
        print(f"   Длина coords_raw: {len(coords_raw)} символов")
        
        # Убираем кавычки
        if coords_raw.startswith('"') and coords_raw.endswith('"'):
            coords_clean = coords_raw[1:-1]
            print(f"   После удаления кавычек, длина: {len(coords_clean)}")
            print(f"   Начало: {coords_clean[:80]}")
            print(f"   Конец: {coords_clean[-40:]}")
    
    # 3. Загружаем ТОЛЬКО первую строку для проверки
    print("\n📤 Тестовая загрузка первой строки...")
    
    record = {}
    for j, header in enumerate(HEADERS):
        if j < len(parts):
            val = parts[j].strip()
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            record[header] = val
    
    print(f"   Записываем id={record.get('id')}, region={record.get('region')}")
    print(f"   Длина coords: {len(record.get('coords', ''))}")
    
    try:
        supabase.table(TABLE_NAME).upsert(record, on_conflict='id').execute()
        print("   ✅ Первая строка загружена успешно")
        
        # Проверяем что сохранилось
        result = supabase.table(TABLE_NAME).select("id, region, coords").eq("id", record['id']).execute()
        if result.data:
            saved = result.data[0]
            print(f"   📊 Проверка: id={saved.get('id')}, region={saved.get('region')}")
            print(f"   Длина coords в БД: {len(saved.get('coords', ''))}")
            print(f"   Начало coords: {saved.get('coords', '')[:80]}")
            print(f"   Конец coords: {saved.get('coords', '')[-40:]}")
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")

    print("\n⏸️  Отладка завершена. Остальные строки пока не загружаем.")
    print("   Если всё ок — убираем отладку и грузим все строки.")


if __name__ == "__main__":
    load_regions()
