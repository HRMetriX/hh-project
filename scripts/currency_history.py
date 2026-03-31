import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Подключение к Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Валюты, которые нужны для HH
CURRENCY_MAPPING = {
    "USD": "USD",
    "EUR": "EUR",
    "BYN": "BYN",   # в HH приходит как BYR
    "KZT": "KZT",
    "UZS": "UZS",
    "AMD": "AMD",
    "KGS": "KGS"
}

def fetch_cbr_rates(date):
    """Получает курсы валют с ЦБ РФ за указанную дату"""
    date_str = date.strftime("%d/%m/%Y")
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
    
    response = requests.get(url)
    response.encoding = "windows-1251"
    
    if response.status_code != 200:
        print(f"Ошибка загрузки курсов за {date_str}: {response.status_code}")
        return None
    
    root = ET.fromstring(response.text)
    rates = {}
    
    for valute in root.findall("Valute"):
        char_code = valute.find("CharCode").text
        if char_code in CURRENCY_MAPPING:
            nominal = int(valute.find("Nominal").text)
            value = float(valute.find("Value").text.replace(",", "."))
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
    """Сохраняет курсы в Supabase"""
    if not rates:
        return
    
    for currency, data in rates.items():
        try:
            supabase.table("exchange_rates").upsert(data).execute()
            print(f"  ✅ {currency}: {data['rate_to_rub']:.4f} руб.")
        except Exception as e:
            print(f"  ❌ Ошибка сохранения {currency}: {e}")

def main():
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    print("=" * 80)
    print("ЗАГРУЗКА КУРСОВ ВАЛЮТ ЦБ РФ")
    print("=" * 80)
    
    for date in [yesterday, today]:
        print(f"\n--- {date.strftime('%Y-%m-%d')} ---")
        rates = fetch_cbr_rates(date)
        if rates:
            save_rates_to_supabase(rates)
        else:
            print("  ❌ Данные не получены")
    
    print("\n✅ Завершено")

if __name__ == "__main__":
    main()
