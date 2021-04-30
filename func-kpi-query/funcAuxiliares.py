def ExistChave(dicionario, chave):
    return chave in dicionario

def TipoValor(valor):
    return type(valor)

def isStatement(queryDic):
    if 'select' in queryDic and 'from' in queryDic:
        if('name' in queryDic['from']) and (type(queryDic['from']['name']) is str) and ('value' in queryDic['from']) and (type(queryDic['from']['value']) is str):
            return True
    else:
        return False

def getTables(FromLista):
    tabelas = []
    if type(FromLista) is list:
        for tableName in FromLista:
            if "name" in tableName:
                tabelas.append({"alias": tableName['name'], "nome" : tableName['value'] })
            elif "join" in tableName:
                tabelas.append({"alias": tableName['join']['name'], "nome" : tableName['join']['value'] })
    elif 'name' in FromLista:
        tabelas.append({"alias": FromLista['name'], "nome" : FromLista['value'] })
    return tabelas

def getColumns(SelectLista):
    colunas = []
    if type(SelectLista) is list:
        for col in SelectLista:
            if type(col['value']) is str:
                tabela, coluna = col['value'].split(".")
                colunas.append({"coluna": coluna, "tabela": tabela})
    return colunas
        

def identifyJoin(fromItem):
    joinkey = [value for key, value in fromItem.items() if 'join' in key.lower()]
    return joinkey

def limpaSys(tabelas):
    return [x for x in tabelas if x['nome'].split(".")[0] != "sys"]     


##########################  ANALYSES ##########################

def analyseQuery(dt_data, ds_user, statement, cnxn):
    if isStatement(statement):
        colunas = getColumns(statement['select'])
        tabelas = getTables(statement['from'])
        createRows(dt_data, ds_user, colunas, tabelas, cnxn)

    elif(ExistChave(statement,'from')):

        tipoFrom = TipoValor(statement['from'])

        if(tipoFrom == list):
            tabelasFromJoin = []

            for item in statement['from']:
                if 'value' in item and type(item['value']) is dict:
                    analyseQuery(dt_data, ds_user, item['value'], cnxn)
                join = identifyJoin(item)
                if  len(join) == 1 and 'value' in join[0] and type(join[0]['value']) is dict:
                    analyseQuery(dt_data, ds_user, join[0]['value'], cnxn)
                if  len(join) == 1 and 'value' in join[0] and type(join[0]['value']) is str:
                    tabelasFromJoin.append(join[0])
                if 'value' in item and type(item['value']) is str:
                    tabelasFromJoin.append(item)

            if len(tabelasFromJoin) > 0:
                tabelas = getTables(tabelasFromJoin)
                colunas = getColumns(statement['select'])
                createRows(dt_data, ds_user, colunas, tabelas, cnxn)

        if(tipoFrom == dict and ExistChave(statement['from'],'value')):
            if(TipoValor(statement['from']['value']) == dict):
                analyseQuery(dt_data, ds_user, statement['from']['value'], cnxn)

def createRows(dt_data, ds_user,colunas, tabelas, cnxn):
    linhas = []
    tabelasLimpas = limpaSys(tabelas)
    for col in colunas:
        for tb in enumerate(tabelasLimpas):
            if tb[1]['alias'] == col['tabela']:
                schema, tabela = tb[1]['nome'].split(".")
                linhas.append(
                    {
                        "dt_data": dt_data,
                        "ds_user": ds_user,
                        "ds_schema" : schema,
                        "ds_tabela": tabela,
                        "ds_coluna": col['coluna']
                    }
                )
    saveDFSql(cnxn, linhas)


def saveDFSql(cnxn, linhas):
    cursor = cnxn.cursor()
    for row in linhas:
        try:
            cursor.execute("INSERT INTO stt_gov.Kpi_Querys (dt_data,ds_user,ds_schema,ds_tabela,ds_coluna) values(?,?,?,?,?)",
            row['dt_data'],row['ds_user'], row['ds_schema'], row['ds_tabela'], row['ds_coluna'])
        except pyodbc.Error as err:
            logging.info(err)
        
        cnxn.commit()
    #cursor.close()

def preparaString(qs):
    wordsToRemove = ['CREATE','ALTER','DROP','TRUNCATE','INSERT','UPDATE','DELETE','MERGE','MATCH','SYS','TRANCOUNT','IMPLICIT_TRANSACTIONS ON ','GRANT','REVOKE','DENY']
    if qs.upper().find('FROM') == -1 or qs.upper().find('SELECT') == -1:
        return ''
    if any(w in qs.upper() for w in wordsToRemove):
        return ''

    inicioComentario, fimComentario = qs.find("/*"), qs.find("*/")
    qs = qs if (inicioComentario == -1 and fimComentario == -1) else qs[0:inicioComentario] + qs[fimComentario:0]
    qs = qs[qs.upper().find('SELECT'):]
    qs = qs if qs.find(';') == -1 else qs[:qs.find(';')] 
    qs = qs.replace(" N'", " '")

    return qs



    