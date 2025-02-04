import cx_Oracle
from woocommerce import API
from time import sleep
import os
from datetime import datetime, timedelta

# Configurações de conexão
credential_bd = { # Acesso ao BD
    "username": os.environ['BD_USERNAME'],
    "password": os.environ['BD_PASSWORD'],
    "dsn": os.environ['BD_DSN']
    }

# Conexão com API do website
wcapi = API(
    url=os.environ['WC_API_URL'],
    consumer_key=os.environ['WC_API_CONSUMER_KEY'],
    consumer_secret=os.environ['WC_API_CONSUMER_SECRET'],
    wp_api=True,
    version="wc/v3"
)

# Funções
def hora_requisicao():
    # Obtém a hora atual
    hora_atual = datetime.now()
    # Soma 5 minutos
    hora_futura = hora_atual + timedelta(minutes=5)
    # Exibe os resultados
    print("Horário final da requisição:", hora_atual.strftime("%H:%Mh"), "=---=","Horário da próx. requisição:", hora_futura.strftime("%H:%Mh"))
    # Pausa o scrip em 5min
    sleep(300)

def sql_update_ultqt_ultpreco():
    # Update campo ultqt
    update_ultqt = """UPDATE WEBSITE_TEMP W
                    SET W.ULTQT = (
                        SELECT (E.QTESTGER - (E.QTBLOQUEADA + E.QTRESERV))
                        FROM PCEST E
                        WHERE E.CODPROD = W.CODPROD
                        AND E.CODFILIAL = 1
                    )
                    WHERE NVL(W.ULTQT, -1) != NVL(
                        (SELECT (E.QTESTGER - (E.QTBLOQUEADA + E.QTRESERV))
                        FROM PCEST E
                        WHERE E.CODPROD = W.CODPROD
                        AND E.CODFILIAL = 1), 
                        -1
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM PCEST E
                        WHERE E.CODPROD = W.CODPROD
                        AND E.CODFILIAL = 1
                    )"""
    cursor.execute(update_ultqt)
    connection.commit()
    # Update campo ultpreco
    update_ultpreco = """UPDATE WEBSITE_TEMP W
                    SET W.ULTPRECO = (
                        SELECT TRUNC(P.PTABELA, 2)
                        FROM PCTABPR P
                        WHERE P.CODPROD = W.CODPROD
                        AND P.NUMREGIAO = 9
                    )
                    WHERE NVL(W.ULTPRECO, 0) != (
                        SELECT TRUNC(P.PTABELA, 2)
                        FROM PCTABPR P
                        WHERE P.CODPROD = W.CODPROD
                        AND P.NUMREGIAO = 9
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM PCTABPR P
                        WHERE P.CODPROD = W.CODPROD
                        AND P.NUMREGIAO = 9
                    )"""
    cursor.execute(update_ultpreco)
    connection.commit()

def sql_select_ultqt_ultpreco():
    sql_select_ultqt_ultpreco = """SELECT 
                    W.IDPRODUCT
                    ,E.QTESTGER - (E.QTBLOQUEADA + E.QTRESERV) AS STOCK_QUANTITY
                    ,TRUNC(T.PTABELA, 2) AS PRICE
                    ,W.IDPRODUCTPRINC
                    FROM WEBSITE_TEMP W
                    INNER JOIN PCTABPR T ON T.CODPROD = W.CODPROD
                    AND T.NUMREGIAO = 9
                    INNER JOIN PCEST E ON E.CODPROD = W.CODPROD
                    AND E.CODFILIAL = 1
                    WHERE 1 = 1
                    AND W.ULTQT <> E.QTESTGER - (E.QTBLOQUEADA + E.QTRESERV) OR TRUNC(NVL(W.ULTPRECO, 0), 2) <> TRUNC(T.PTABELA, 2)"""
    # Executa a consulta SQL
    cursor.execute(sql_select_ultqt_ultpreco)
    # Obtém os resultados
    rows = cursor.fetchall()
    return rows

# Scrip
while True:
    hora_atual = datetime.now()
    hora_formatada = hora_atual.strftime('%H')
    if hora_formatada == '23':
        break
    connection = None
    cursor = None
    try:
        # Estabelece a conexão
        connection = cx_Oracle.connect(credential_bd['username'], credential_bd['password'], credential_bd['dsn'])

        # Cria um cursor
        cursor = connection.cursor()

        # Processa os resultados
        for row in sql_select_ultqt_ultpreco():
            if row[0] != row[3]:
                data = {
                            "regular_price": str(row[2]),
                            "stock_quantity": int(row[1])
                        }
                wcapi.put(f"products/{row[3]}/variations/{row[0]}", data).json()
            else:
                data = {
                            "regular_price": str(row[2]),
                            "stock_quantity": int(row[1])
                        }
                wcapi.put(f"products/{row[0]}", data).json()
            print(f"IDPRODUCTPRINC: {row[3]} / IDPRODUCT: {row[0]} / QTEST: {row[1]} / VLVENDA: R${row[2]}")

        # Executa a atualização SQL
        if len(sql_select_ultqt_ultpreco()) > 0:
            sql_update_ultqt_ultpreco()
            print("Dados Atualizados no Banco de Dados e Ecommerce")
        else:
            print("Aplicação Não Possui Atualizações de Estoque ou Preço")
            pass

        # Fecha o cursor e a conexão
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    except cx_Oracle.DatabaseError as e:
        # Trata os erros de conexão e execução
        error, = e.args
        print(f"Erro ao conectar ao banco de dados: {error.message}")

    finally:
        hora_requisicao()