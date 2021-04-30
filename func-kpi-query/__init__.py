import logging
import pyodbc
import json
import pandas as pd
from moz_sql_parser import parse
import azure.functions as func

from . import funcAuxiliares as fcAux
from . import connection as conn

#MASTER BRANCH

query_incremental = ("select login_time, login_name, text as query "
                 " from stt_gov.LogEvent "
                 " where"
                 " text NOT LIKE '%INSERT%'"
                " AND text NOT LIKE '%CREATE%'"
                " AND text NOT LIKE'%INSERT%'"
                    " AND text NOT LIKE '%UPDATE%'"
                    " AND text NOT LIKE '%DELETE%'"
                    " AND text NOT LIKE '%MERGE%'"
                    " AND text NOT LIKE '%MATCH%'"
                    " AND text NOT LIKE '%SYS%'"
                    " AND text NOT LIKE '%TRANCOUNT%'"
                    " AND text NOT LIKE '%IMPLICIT_TRANSACTIONS%'"
                    " AND text NOT LIKE '%GRANT%'"
                    " AND text NOT LIKE '%REVOKE%'"
                    " AND text NOT LIKE '%DENY%'"
                    " AND text NOT LIKE '%TOP 0%'"
                    " AND text NOT LIKE '%FROM ##%'"
                   	" AND text NOT LIKE '%TRUNCATE%'"
		            " AND text NOT LIKE '%ALTER%'"
		            " AND text NOT LIKE '%DROP%'"
                    " AND text NOT LIKE '%SET NOEXEC OFF%'"
                    " AND login_time > (select max(dt_data) from stt_gov.Kpi_Querys)"
                 )

query_inicial = ("select login_time, login_name, text as query "
                 " from stt_gov.LogEvent "
                 " where"
                 " text NOT LIKE '%INSERT%'"
                " AND text NOT LIKE '%CREATE%'"
                " AND text NOT LIKE'%INSERT%'"
                    " AND text NOT LIKE '%UPDATE%'"
                    " AND text NOT LIKE '%DELETE%'"
                    " AND text NOT LIKE '%MERGE%'"
                    " AND text NOT LIKE '%MATCH%'"
                    " AND text NOT LIKE '%SYS%'"
                    " AND text NOT LIKE '%TRANCOUNT%'"
                    " AND text NOT LIKE '%IMPLICIT_TRANSACTIONS%'"
                    " AND text NOT LIKE '%GRANT%'"
                    " AND text NOT LIKE '%REVOKE%'"
                    " AND text NOT LIKE '%DENY%'"
                    " AND text NOT LIKE '%TOP 0%'"
                    " AND text NOT LIKE '%FROM ##%'"
                   	" AND text NOT LIKE '%TRUNCATE%'"
		            " AND text NOT LIKE '%ALTER%'"
		            " AND text NOT LIKE '%DROP%'"
                    " AND text NOT LIKE '%SET NOEXEC OFF%'"
                 )

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    req_body = req.get_json()
    tipoCarga = req_body.get('tipoCarga')

    if(tipoCarga == 'CargaInicial'):
        query = query_inicial
    elif(tipoCarga == 'CargaIncremental'):
        query = query_incremental
    else:
        return func.HttpResponse(
             "Necessário passar um parâmentro : { tipoCarga : <'CargaInicial'>/<'CargaIncremental'> }",
             status_code=200
        )
    try:
        
        cnxn = pyodbc.connect('DRIVER='+conn.linux_sql_driver+';SERVER='+conn.server +
                              ';DATABASE='+conn.database+';UID='+conn.username+';PWD=' + conn.password)

        logging.info('Conexão ao banco realizada com sucesso!\nIniciando query...')

        df = pd.read_sql(query, cnxn)
        logging.info('TAMANHO DF ' + str(len(df)))

    except Exception as e:
        if hasattr(e, 'message'):
            msgErro = str(e)
        else:
            msgErro = str(e)
        return func.HttpResponse(
            "Erro ao conectar-se com o banco de dados\n " +msgErro,
            status_code=200
        )

    if len(df) > 0:

        countErro = 0
        valido = 0
        
        for row in df.iterrows():
            queryStr = fcAux.preparaString(row[1]['query'])

            if len(queryStr) > 0:
                try:
                    queryDic = parse(queryStr)
                    valido += 1
                    fcAux.analyseQuery(
                        row[1]['login_time'], row[1]['login_name'], queryDic, cnxn)
                except:
                    countErro += 1
        response = {"Mensagem": "Ok"}
     
        return func.HttpResponse(json.dumps(response, indent=2, sort_keys=True, ensure_ascii=False),
            status_code=200
            )
        
    else:
        response = {"Mensagem": "Não há novos eventos de log para processar"}
        return func.HttpResponse(
           json.dumps(response, indent=2, sort_keys=True, ensure_ascii=False),
            status_code=200
        )
