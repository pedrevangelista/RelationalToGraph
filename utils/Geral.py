import time

def formatar_coluna(nome):
    return nome.replace(" ", "")

def medir_tempo(func):
    def wrapper(*args, **kwargs):
        inicio = time.perf_counter()
        resultado = func(*args, **kwargs)
        fim = time.perf_counter()
        print(f"{func.__name__}: {fim - inicio:.6f} segundos")
        return resultado
    return wrapper
