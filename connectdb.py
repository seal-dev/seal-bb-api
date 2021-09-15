import sqlite3

class Query():
    def __init__(self):

        self.db = sqlite3.connect('../db/dados.db')

        self.cursor = self.db.cursor()

    def insert(self, table, fields, values):
        try:
            
            sql = "INSERT INTO {} ({}) VALUES {};".format(table, fields, values)
            #print(sql)
            self.cursor.execute(sql)
            self.db.commit()

            return {'Success': 'The values was insert into table abastecimento'}
        except Exception as e:
            return {'erro ao inserir!': e}

    def select(self, table, fields, operador=None, *args):
        try:
            condicionais = []
            for arg in args:
                condicionais.append(arg)
    
            if len(condicionais) == 0:
                sql = "select {} from {};".format(fields, table)
            else:
                condition = " {} ".format(operador).join(condicionais)
                sql = "select {} from {} where {};".format(fields, table, condition)
            
            self.cursor.execute(sql)
            self.db.commit 
            
        except Exception as e:
            print('Erro ao consultar!', e)
    
    def delete(self, table, field_filter, field_value):
        try:
            sql = "DELETE FROM {} WHERE {}={};".format(table, field_filter, field_value)
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()

        except Exception as e:
            print('Erro ao deletar!', e)
