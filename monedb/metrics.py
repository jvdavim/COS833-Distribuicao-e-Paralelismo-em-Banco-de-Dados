import numpy as np


def tpcPower(SF, Q, R):
    """
    SF: Scale Factor
    Q: Lista com os tempos para executar uma query com um stream unico
    R: Lista com os tempos para executar as funcoes de atualziacao (refresh functions)
    Computa o inverso da media geometrica dos intervalos de tempo
    """
    n = 3600 * SF
    d = np.power(np.prod(Q) * np.prod(R), (1/(len(Q) + len(R))))
    return (n/d)


def tpcThroughput(T, S, SF, n_queries):
    """
    T: Intervalo de medida
    S: Número de query streams utilizados
    SF: Scale Factor
    n_queries: numero de queries executadas

    É definida como a porcentagem entre o número total de queries executadas pelo tamanho do
    intervalo de medida
    """
    n = (S*n_queries*3600)/T
    d = SF
    return (n/d)

    4.13 / 5.3.6


def queryPerHour(power, throughput)


return np.round(np.sqrt(power * throughput), 1)


def price(query_per_hor, hour_price):
    return hour_price/query_per_hor

# Métrica de ¨Data disponível¨ também precisa ser reportada
# Resultados devem ser apresentados conforme mostra a secçã8 do documento
