from decimal import Decimal

def tratar_decimais(parametros):
    parametros_formatados = []
    for param in parametros:
    # Converter Decimal para float antes de enviar
        param_formatado = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in param.items()}
        parametros_formatados.append(param_formatado)
    return parametros_formatados