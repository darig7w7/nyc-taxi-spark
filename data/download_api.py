import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")
APP_TOKEN = os.getenv("NYC_APP_TOKEN")
URL       = "https://data.cityofnewyork.us/resource/uacg-pexx.json"
LIMITE    = 50000

def descargar_mes(anio, mes, total=100000):
    mes_str = str(mes).zfill(2)
    dias    = {1:31,2:28,3:31,4:30,5:31,6:30,
               7:31,8:31,9:30,10:31,11:30,12:31}
    fecha_i = f"{anio}-{mes_str}-01T00:00:00.000"
    fecha_f = f"{anio}-{mes_str}-{dias[mes]}T23:59:59.000"

    print(f"\nDescargando {fecha_i[:10]} → {fecha_f[:10]}...")
    todos  = []
    offset = 0

    while offset < total:
        print(f"  Filas {offset:,} → {offset+LIMITE:,}...")
        params = {
            "$$app_token": APP_TOKEN,
            "$limit":      LIMITE,
            "$offset":     offset,
            "$where":      f"tpep_pickup_datetime >= '{fecha_i}' AND tpep_pickup_datetime <= '{fecha_f}'",
            "$order":      "tpep_pickup_datetime ASC"
        }

        r = requests.get(URL, params=params, timeout=30)

        if r.status_code != 200:
            print(f"  Error {r.status_code}: {r.text}")
            break

        data = r.json()
        if not data:
            print("  Sin mas datos")
            break

        todos.extend(data)
        print(f"  {len(todos):,} filas")
        offset += LIMITE
        time.sleep(1)

    return pd.DataFrame(todos)

print("=" * 50)
print("  NYC TAXI — DESCARGA INTERACTIVA VIA API")
print("=" * 50)
print(f"Token: {'OK' if APP_TOKEN else 'falta en .env'}\n")

anio  = int(input("Año  (ej: 2016): "))
mes_i = int(input("Mes inicio (1=Ene): "))
mes_f = int(input("Mes fin    (3=Mar): "))
total = int(input("Filas por mes (ej: 100000): "))

todos = []
for mes in range(mes_i, mes_f + 1):
    df_mes = descargar_mes(anio, mes, total)
    todos.append(df_mes)
    print(f"  Mes {mes}: {len(df_mes):,} filas")

df_final = pd.concat(todos, ignore_index=True)
output   = f"nyc_taxi_{anio}_mes{mes_i}_mes{mes_f}.csv"
df_final.to_csv(output, index=False)

size = os.path.getsize(output)/1024/1024
print(f"\n{'='*50}")
print(f" Total:    {len(df_final):,} filas")
print(f" Archivo:  {output} ({size:.1f} MB)")
print(f"{'='*50}")
