"""
Ежедневный сбор курсов валют с сайта Центрального Банка РФ.

Назначение:
    - Загружает официальные курсы валют к рублю за текущую дату
    - Сохраняет их в таблицу exchange_rates в Supabase
    - Используется триггерами БД для пересчёта зарплат из исходной валюты в рубли

Источник данных:
    https://www.cbr.ru/scripts/XML_daily.asp — XML-фид ЦБ РФ с курсами на заданную дату

Запуск:
    - Автоматически: GitHub Actions, ежедневно в 07:00 MSK
    - Вручную: python scripts/daily_currency.py

Таблица назначения (exchange_rates):
    - currency_code   — код валюты (USD, EUR, BYN, KZT, UZS, AMD, KGS)
    - currency_name   — полное название валюты из справочника ЦБ
    - rate_to_rub     — курс за 1 единицу валюты (с учётом номинала)
    - nominal         — номинал (для рубля = 1)
    - rate_date       — дата, на которую установлен курс
    - source          — источник данных ("cbr")
    - created_at      — дата создания записи (автоматически)
    - updated_at      — дата последнего обновления (автоматически)

Уникальность: сочетание (currency_code, rate_date), вставка через upsert
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Подключение к Supabase
# ---------------------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# Маппинг валют: какие коды нас интересуют в XML-фиде ЦБ
# ---------------------------------------------------------------------------
# Берём только те валюты, которые реально встречаются в вакансиях hh.ru.
# BYN (белорусский рубль) в hh.ru приходит как BYR — это историческое
# обозначение, мы конвертируем его в BYN при сохранении зарплат.
CURRENCY_MAPPING = {
    "USD": "USD",
    "EUR": "EUR",
    "BYN": "BYN",   # В hh.ru обозначается как BYR
    "KZT": "KZT",
    "UZS": "UZS",
    "AMD": "AMD",
    "KGS": "KGS"
}


def fetch_cbr_rates(date):
    """
    Загружает и парсит курсы валют с сайта ЦБ РФ за указанную дату.
    
    Особенности XML-фида ЦБ:
        - Кодировка windows-1251
        - Дробная часть отделяется запятой (нужно заменить на точку)
        - Номинал может быть больше 1 (например, 100 итальянских лир)
        - Курс пересчитывается на 1 единицу валюты делением на номинал
    
    Args:
        date: объект datetime.date — дата, за которую запрашиваются курсы
    
    Returns:
        dict | None: словарь с курсами в формате для вставки в БД
                     или None, если ЦБ не ответил
    """
    date_str = date.strftime("%d/%m/%Y")
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"

    response = requests.get(url)
    response.encoding = "windows-1251"

    if response.status_code != 200:
        print(f"Ошибка загрузки курсов за {date_str}: {response.status_code}")
        return None

    # Парсим XML-ответ ЦБ
    root = ET.fromstring(response.text)
    rates = {}

    for valute in root.findall("Valute"):
        char_code = valute.find("CharCode").text

        # Фильтруем: сохраняем только нужные нам валюты
        if char_code in CURRENCY_MAPPING:
            nominal = int(valute.find("Nominal").text)
            # ЦБ использует запятую как десятичный разделитель — исправляем
            value = float(valute.find("Value").text.replace(",", "."))

            # Пересчитываем курс на 1 единицу валюты
            rate = value / nominal

            rates[char_code] = {
                "currency_code": char_code,
                "currency_name": valute.find("Name").text,
                "rate_to_rub": rate,
                "nominal": nominal,
                "rate_date": date.strftime("%Y-%m-%d"),
                "source": "cbr"
            }

    return rates


def save_rates_to_supabase(rates):
    """
    Сохраняет курсы валют в таблицу exchange_rates.
    
    Использует операцию upsert — если курс на эту дату для этой валюты
    уже существует, он будет обновлён, а не продублирован.
    Уникальность обеспечивается сочетанием (currency_code, rate_date).
    
    Args:
        rates: словарь с курсами из функции fetch_cbr_rates
    """
    if not rates:
        return

    for currency, data in rates.items():
        try:
            supabase.table("exchange_rates").upsert(data).execute()
            print(f"  ✅ {currency}: {data['rate_to_rub']:.4f} руб.")
        except Exception as e:
            print(f"  ❌ Ошибка сохранения {currency}: {e}")


def main():
    """
    Точка входа.
    
    Логика:
        1. Берём сегодняшнюю дату
        2. Загружаем курсы с сайта ЦБ
        3. Сохраняем в Supabase через upsert
    """
    # Время UTC, но курс ЦБ на текущий день уже опубликован
    today = datetime.now().date()

    print("=" * 80)
    print("ЕЖЕДНЕВНЫЙ СБОР КУРСОВ ВАЛЮТ ЦБ РФ")
    print("=" * 80)
    print(f"Дата сбора: {today.strftime('%Y-%m-%d')}")

    print(f"\n--- {today.strftime('%Y-%m-%d')} ---")
    rates = fetch_cbr_rates(today)

    if rates:
        save_rates_to_supabase(rates)
        print(f"\n✅ Курсы за {today} сохранены")
    else:
        print("  ❌ Данные не получены")

    print("\n✅ Завершено")


if __name__ == "__main__":
    main()
