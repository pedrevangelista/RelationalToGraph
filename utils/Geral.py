import time
import decimal
from datetime import date, datetime

def formatar_coluna(nome):
    return nome.replace(" ", "")

def medir_tempo(func):
    def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fim = time.time()
        print(f"{func.__name__}: {fim - inicio:.6f} segundos")
        return resultado
    return wrapper

def tratar_valor(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.hex()
    if v is None:
        return "None"
    if isinstance(v, str):
        return v.replace("'", "\\'")
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v
