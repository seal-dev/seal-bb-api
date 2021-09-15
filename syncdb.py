from connectdb import Query
from endpoints import APIseal
from time import sleep

query = Query()
api =  APIseal()

def consulta_db_bico():
    fields = ['id_bico', 'id_produto', 'nomeproduto', 'vlrunit', 
            'ip', 'porta', 'ladopulser', 
            'idpulser', 'idlocal', 'portaeletvalv']

    select = query.select('bico', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []
    
    for i in response:
        
        dicionario = {
                    "id_bico": i[0],
                    "id_produto": i[1],
                    "idlocal": i[8],
                    "idpulser": i[7],
                    "ip": "{}".format(i[4]),
                    "ladopulser": i[6],
                    "nomeproduto": "{}".format(i[2]),
                    "porta": i[5],
                    "portaeletvalv": "{}".format(i[9]),
                    "vlrunit": int(i[3])
                }

        lista.append(dicionario)
    return lista

def update_bico():
    fields = ['id_bico', 'id_produto', 'nomeproduto', 'vlrunit', 
            'ip', 'porta', 'ladopulser', 
            'idpulser', 'idlocal', 'portaeletvalv']

    response_api = api.bicos_comboio()
    
    consulta_db = consulta_db_bico()
    
    #print(consulta_db, '\n \n', response_api)
    if response_api == consulta_db:
        print("sem alterações na tabela bico !!!")
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:

        try:
            sql = "DELETE FROM bico WHERE id_bico > 0 "
            query.cursor.execute(sql)
            query.db.commit()
            for i in response_api:
            
                valores_campos = (i['id_bico'], i['id_produto'], i['nomeproduto'], i['vlrunit'], 
                                i['ip'], i['porta'], i['ladopulser'], 
                                i['idpulser'], i['idlocal'], i['portaeletvalv'])

                insert = query.insert('bico', ', '.join(fields), valores_campos)                    
            print('Tabela bico está atualizada !!!')
        except Exception as e:
            print(e, 'o')
            pass

def consulta_db_localabast():
    fields = ['*']

    select = query.select('localabastecimento', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []
    
    bolean = False
    
    for i in response:
        
        if i[2] == 1:
            bolean = True
        
        dicionario = {
            'ativo': bolean,
            'descricao': i[1],
            'id_localabastecimento' : i[0] 
        }

        lista.append(dicionario)
    return lista

def update_localabast():
    fields = ['id_localabastecimento', 'descricao', 'ativo']

    response_api = api.localabastecimento()
    
    consulta_db = consulta_db_localabast()
    #print(response_api, '\n\n', consulta_db)
    if response_api == consulta_db:
        print('sem alterações na tabela localabastecimento !!!')
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:
        try:
            sql = 'DELETE FROM localabastecimento WHERE id_localabastecimento > 0;'
            query.cursor.execute(sql)
            query.db.commit()
            for i in response_api:
                if i['ativo'] is True:
                    i['ativo'] = 1
                else:
                    i['ativo'] = 0

                valores_campos = (i['id_localabastecimento'], i['descricao'], i['ativo'])
                insert = query.insert('localabastecimento', ', '.join(fields), valores_campos)

            print('Tabela localoabastecimento está atualizada !!!')
        except Exception as e:
            print(e)
            pass

def consulta_db_filial():
    fields = ['*']

    select = query.select('filial', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []
    
    bolean = False

    for i in response:
        
        if i[2] == 1:
            bolean = True
        
        dicionario = {
            "id": i[0],
            "id_filial": i[1],
            "razaosocialfilial": i[2],
            "endereco": i[3],
            "cidade": i[4],
            "uf": i[5],
            "cnpj": i[6],
            "ie": i[7],
            "idcomboio": i[8],
            "idcompserie": i[9],
            "nrocompatual": i[10]
        }

        lista.append(dicionario)
    return lista

def update_filial():

    idfilial = api.lendo_ini()['filial']

    fields = [
            'id', 
            'id_filial', 
            'razaosocialfilial', 
            'endereco', 
            'cidade', 
            'uf', 
            'cnpj', 
            'ie', 
            'idcomboio', 
            'idcompserie', 
            'nrocompatual'
        ]

    response_api = api.get_filial()
    consulta_db = consulta_db_filial()
    
    #print(consulta_db, '\n \n', response_api)

    if consulta_db == response_api:
        print('sem alterações na tabela filial !!!')
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:
        sql = 'DELETE FROM filial WHERE id > 0;'
        query.cursor.execute(sql)
        query.db.commit()
        for i in response_api:
            valores_campos = (
                i['id'],
                i['id_filial'],
                i["razaosocialfilial"],
                i['endereco'],
                i['cidade'],
                i['uf'],
                i['cnpj'],
                i['ie'],
                i['idcomboio'],
                i['idcompserie'],
                i['nrocompatual'],
                )
            
            insert = query.insert('filial', ', '.join(fields), valores_campos)
        print('Tabela filial atualizada com sucesso !!!')

def consulta_db_funcionario():
    fields = ['id_funcionarios', 'id_usuario', 'senha', 'nome', 'ativo']

    select = query.select('funcionarios', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []

    for i in response:
        
        dicionario = {
            "ativo": i[4],
            "id_funcionarios": i[0],
            "id_usuario": i[1],
            "nome": '{}'.format(i[3]),
            "senha": "{}".format(i[2])
        }

        lista.append(dicionario)

    return lista

def update_funcionario():
    fields = ['id_funcionarios', 'id_usuario', 'senha', 'nome', 'ativo']

    response_api = api.funcionario()
    consulta_db = consulta_db_funcionario()
    
    if consulta_db == response_api:
        print('sem alterações na tabela funcionarios !!!')
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:
        for i in response_api:

            sql =  " UPDATE funcionarios" + \
                " SET id_usuario={}, senha='{}',".format(i['id_usuario'], i['senha']) + \
                " nome='{}', ativo={}".format(i['nome'], i['ativo']) + \
                " WHERE id_funcionarios={};".format(i['id_funcionarios']) 
            
            
            query.cursor.execute(sql)
            query.db.commit()

        diferenca = []
        delete = False
        
        if len(response_api) > len(consulta_db):
            for i in response_api:
                if i not in consulta_db:
                    diferenca.append(i)
        elif len(consulta_db) > len(response_api):
            delete = True
            for i in consulta_db:
                if i not in response_api:
                    diferenca.append(i)

        if delete is False:

            for i in diferenca:
                valores_campos = (i['id_funcionarios'], i['id_usuario'], i['senha'], i['nome'], i['ativo'])
                insert = query.insert('funcionarios', ', '.join(fields), valores_campos)

        else:
            for i in diferenca:
                delete = query.delete('funcionarios', 'id_funcionarios', i['id_funcionarios'])
        print('Tabela funcionarios altualizada !!!')

def consulta_db_operadores():
    fields = ['*']

    select = query.select('operadores', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []
    
    for i in response:
        
        dicionario = {
            "id_usuario": i[0],
            "senha": i[2],
            "login": i[1],
            "tipo": i[3]
        }

        lista.append(dicionario)

    return lista

def update_operadores():
    fields = ['id', 'login', 'senha', 'tipo']

    response_api = api.operador()
    consulta_db = consulta_db_operadores()
    #print(response_api, '\n \n \n', consulta_db)
    if response_api == consulta_db:
        print('sem alterações na tabela operadores !!!')
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:
        try:
            for i in response_api:
                sql = " UPDATE operadores" + \
                    " SET login='{}', senha='{}', tipo={}".format(i['login'], i['senha'], i['tipo']) + \
                    " WHERE id={};".format(i['id_usuario'])
            
                query.cursor.execute(sql)
                query.db.commit()

            diferenca = []
            delete = False
            
            if len(response_api) > len(consulta_db):
                for i in response_api:
                    if i not in consulta_db:
                        diferenca.append(i)
            elif len(consulta_db) > len(response_api):
                delete = True
                for i in consulta_db:
                    if i not in response_api:
                        diferenca.append(i)

            if delete is False:

                for i in diferenca:
                    valores_campos = (i['id_usuario'], i['login'], i['senha'], i['tipo'])
                    insert = query.insert('operadores', ', '.join(fields), valores_campos)
                    print(insert)
            else:
                for i in diferenca:
                    delete = query.delete('operadores', 'id', i['id_usuario'])
            print('Tabela operadores está atualizada !!!')
        except Exception as e:
            print('ola', e)
            pass


def consulta_db_tanqueveiculos():
    fields = ['id_tanquesveiculos', 'id_veiculos', 'id_produtos1', 'id_produtos2', 'cod_xpid', 'tipo', 'ativo']

    select = query.select('tanquesveiculos', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []

    bolean = False
    for i in response:
        
        if i[6] == 1:
            bolean = True
        
        dicionario = {
            "ativo": bolean,
            "cod_xpid": i[4],
            "id_produtos1": i[2],
            "id_produtos2": i[3],
            "id_tanquesveiculos": i[0],
            "id_veiculos": i[1],
            "tipo": i[5]
        }

        lista.append(dicionario)

    return lista

def update_tanqueveiculos():
    fields = ['id_tanquesveiculos', 'id_veiculos', 'id_produtos1', 'id_produtos2', 'cod_xpid', 'tipo', 'ativo']

    response_api = api.tanques()
    consulta_db = consulta_db_tanqueveiculos()

    #print(consulta_db, '\n \n', response_api)
    
    if consulta_db == response_api:
        print('sem alterações na tabela tanquesveiculos !!!')
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:
        for i in response_api:
            if i['ativo'] is True:
                i['ativo'] = 1
            else:
                i['ativo'] = 0
            sql = " UPDATE tanquesveiculos" + \
                " SET id_veiculos={},".format(i['id_veiculos']) + \
                " id_produtos1={},".format(i['id_produtos1']) + \
                " id_produtos2={}, ".format(i['id_produtos2']) + \
                " cod_xpid='{}', ".format(i['cod_xpid']) + \
                " tipo={}, ".format(i['tipo']) + \
                " ativo={}".format(i['ativo']) + \
                " WHERE id_tanquesveiculos={};".format(i['id_tanquesveiculos'])

            query.cursor.execute(sql)
            query.db.commit()

        diferenca = []
        delete = False
        
        if len(response_api) > len(consulta_db):
            for i in response_api:
                if i not in consulta_db:
                    diferenca.append(i)

        elif len(consulta_db) > len(response_api):
            delete = True
            for i in consulta_db:
                if i not in response_api:
                    diferenca.append(i)

        if delete is False:

            for i in diferenca:
                valores_campos = (i['id_tanquesveiculos'], i['id_veiculos'], i['id_produtos1'], i['id_produtos2'], i['cod_xpid'], i['tipo'], i['ativo'])
                insert = query.insert('tanquesveiculos', ', '.join(fields), valores_campos)

        else:
            for i in diferenca:
                delete = query.delete('tanquesveiculos', 'id_tanquesveiculos', i['id_tanquesveiculos'])
        
        print('Tabela tanquesveiculos altualizada !!!')

def consulta_db_placas():
    fields = ['id_placa', 'nroplaca', 'ultimoodometro', 'tag', 'veiculo', 'motorista', 
    'id_entidade', 'horimetro', 'emitecompnf', 'tpliberacao', 'ctrlconsumo',
     'id_checklist', 'exibemedia', 'ctrlhrkm', 'codteclado', 'ativo', 'pulsoskm',
      'nropulsos', 'iptmct', 'kmbase', 'hrbase']

    select = query.select('placas', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []

    for i in response:
        
        if i[15] == 1:
            bolean = True
        else:
            bolean = False
        
        if i[11] == 'null':
            checklist = None
        else:
            checklist = i[11]

        if i[18] == 'null':
            iptlm = None
        else:
            iptlm = i[18]

        dicionario = { 
                "ativo": bolean,
                "codteclado": i[14],
                "ctrlconsumo": i[10],
                "ctrlhrkm": i[13],
                "emitecompnf": i[8],
                "exibemedia": i[12],
                "horimetro": i[7],
                "hrbase": i[19],
                "id_checklist": checklist,
                "id_entidade": i[6],
                "id_placas": i[0],
                "iptmct": iptlm,
                "kmbase": i[19],
                "motorista": i[5],
                "nroplaca": i[1],
                "nropulsos": float(i[17]),
                "pulsoskm": i[16],
                "tag": i[3],
                "tpliberacao": i[9],
                "ultimoodometro": i[2],
                "veiculo": i[4],
            }

        lista.append(dicionario)

    return lista

def update_placas():
    fields = ['id_placa', 'nroplaca', 'ultimoodometro', 'tag', 'veiculo', 'motorista', 
    'id_entidade', 'horimetro', 'emitecompnf', 'tpliberacao', 'ctrlconsumo',
    'id_checklist', 'exibemedia', 'ctrlhrkm', 'codteclado', 'ativo', 'pulsoskm',
    'nropulsos', 'iptmct', 'kmbase', 'hrbase']

    response_api = api.veiculos()
    consulta_db = consulta_db_placas()

    #for i in range(0, len(response_api)):
    #    print(i, list(frozenset(consulta_db[i].items()) - frozenset(response_api[i].items())))
    #print(consulta_db[2], '\n\n', response_api[2])

    if consulta_db == response_api:
        print('sem alterações na tabela placas !!!')
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:
        sql = "DELETE FROM placas WHERE id_placa > 0; "
        query.cursor.execute(sql)
        query.db.commit()
        for i in response_api:
            if i['ativo'] is True:
                i['ativo'] = 1
            else:
                i['ativo'] = 0
            
            if i['id_checklist'] == None:
                i['id_checklist'] = 'null'
                
            if i['iptmct'] == None:
                i['iptmct'] = 'null'

            valores_campos = (
                i['id_placas'], i['nroplaca'], i['ultimoodometro'], i['tag'], i['veiculo'], i['motorista'], 
                i['id_entidade'], i['horimetro'], i['emitecompnf'], i['tpliberacao'], i['ctrlconsumo'],
                i['id_checklist'], i['exibemedia'], i['ctrlhrkm'], i['codteclado'], i['ativo'], i['pulsoskm'],
                i['nropulsos'], i['iptmct'], i['kmbase'], i['hrbase']
            )

            insert = query.insert('placas', ', '.join(fields), valores_campos)                    
        
        print('Tabela placas altualizada !!!')

def envia_abastecimento():
    fields = ['id', 'id_bico', 'id_placa', 'qtde', '"data"', 'idfuncionario', 
            'idoperador', 'nrocomp', 'cfop', 'semtag', 'odometro', 'horimetro', 
            'tag', 'idlocal', 'tipotq', 'tipolib', 'telemetria']

    select = query.select('abast', ', '.join(fields))

    response = query.cursor.fetchall()

    for i in response:
        retorno = {
                    "id": i[0],
                    "idbico": i[1],
                    "data": "{}".format(i[4]),
                    "qtde": i[3],
                    "idplaca": i[2],
                    "idfuncionario": i[5],
                    "idoperador": i[6],
                    "semtag": i[9],
                    "odometro": i[10],
                    "horimetro": i[11],
                    "tag": "{}".format(i[12]),
                    "local": 1,
                    "tipotq": i[14],
                    "tipolib": i[15],
                    "telemetria": i[16]
                }

        try:
            api.post_abastecimento(dicionario=retorno)
            print('enviado')
        except Exception as e:
            print(e)
    
def consulta_abastecimento():
    fields = ['id']

    select = query.select('abast', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []

    bolean = False

    if response is not None:
        for i in response:
            
            lista.append(i[0])

        return lista

def api_abastecimentos():

    response_api = api.get_abastecimentos()

    lista = []
    if response_api is not None:
        for i in response_api:
            
            lista.append(i['id'])

        return lista

def compara_abastecimentos():
    db_abastecimentos = consulta_abastecimento()
    api_abast = api_abastecimentos()
    if api_abast is not None:
        if len(db_abastecimentos) > 0:
            for i in db_abastecimentos:
                if i in api_abast:
                    
                    sql = 'delete from abast WHERE id = {};'.format(i)

                    query.cursor.execute(sql)
                    query.db.commit()
                    print('deletou abastecimentos !!!')
                else:
                    sleep(5)
                    envia_abastecimento()
                    print('enviou abastecimentos !!!')
        else:
            print('sem abastecimentos!!!')
    else:
        print('sem conexão com api a externa!')

def consulta_db_config():
    fields = ['*']

    select = query.select('cfgcomboio', ', '.join(fields))

    response = query.cursor.fetchall()

    lista = []
    
    bolean = False
    
    for i in response:
        dicionario = {
            'bloqkmhr' : i[3],
            'login' : i[4],
            'preodo' : i[2],
            'senha' : i[1]
        }

        lista.append(dicionario)
    return lista

def update_config():
    fields = ['senha', 'loginop', 'preodo', 'bloqkmhr']

    response_api = api.config()
    consulta_db = consulta_db_config()
    #print(list(frozenset(consulta_db[4].items()) - frozenset(response_api[4].items())))
    #print(consulta_db, '\n\n', response_api)
    if response_api == consulta_db:
        print('sem alterações na tabela cfgcomboio !!!')
    elif response_api is None:
        print('sem conexão com a api externa!')
    else:
        try:
            for i in response_api:
                sql = " UPDATE cfgcomboio" + \
                   " SET senha={}, preodo=1,".format(i['senha']) + \
                   " bloqkmhr=1, loginop={}".format(i['login']) + \
                   " WHERE id=1;".format(i['id'])
            
                query.cursor.execute(sql)
                
                query.db.commit()

            diferenca = []
            delete = False
            
            if len(response_api) > len(consulta_db):
                for i in response_api:
                    if i not in consulta_db:
                        diferenca.append(i)
            elif len(consulta_db) > len(response_api):
                delete = True
                for i in consulta_db:
                    if i not in response_api:
                        diferenca.append(i)

            if delete is False:

                for i in diferenca:
                    valores_campos = (1, i['login'], i['senha'], i['login'], i['bloqkmhr'])
                    insert = query.insert('cfgcomboio', ', '.join(fields), valores_campos)

            else:
                for i in diferenca:
                    delete = query.delete('cfgcomboio', 'id_localabastecimento', 1)
            print('Tabela cfgcomboio está atualizada !!!')
        except Exception as e:
            print('erro', e)
            pass


if __name__ == '__main__':
    while True:
        update_bico()
        update_config()
        update_filial()
        update_funcionario()
        update_localabast()
        update_operadores()
        update_placas()
        update_tanqueveiculos()
        compara_abastecimentos()
        sleep(8)
