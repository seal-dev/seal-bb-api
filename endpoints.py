import requests
import json
import configparser


class APIseal():

    def __init__(self):

        
        self.route = 'http://192.168.1.58:7676/v1'
       
    
    def lendo_ini(self):
        cfg = configparser.ConfigParser()
        cfg.read('../config.ini')
        filial = cfg.getint('CFG', 'IDFilial')
        ccs = cfg.getint('CFG', 'IDComboio')
        idbico = cfg.getint('CFG', 'IDBico')
        matriz = cfg.getint('CFG', 'IDMatriz')

        data = {
            'filial': filial,
            'ccs': ccs,
            'idbico': idbico,
            'matriz': matriz
        }

        return data

    def token(self):
        
        request = requests.get('{}/auth/refreshtoken/{}'.format(self.route, self.lendo_ini()['ccs']))
        
        response = request.json()
        
        return response['token']

    def localabastecimento(self):
        try:
            
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/localabast/get/{}'.format(self.route, self.lendo_ini()['matriz']), headers=self.authorization)

            response = request.json()
            
            return response 
        except Exception as e:
            print(e)
            pass
    

    def get_abastecimentos(self):
        try:
            
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/abastecimentos/get/{}'.format(self.route, self.lendo_ini()['filial']), headers=self.authorization)

            response = request.json()
            
            return response 
            
        except Exception as e:
           
            pass


    def bico(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/bicos/get/{}'.format(self.route, self.lendo_ini()['idbico']), headers=self.authorization)
            response = request.json()

            return response
        except Exception as e:
            
            pass

    def veiculos(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/placas/get/{}'.format(self.route, self.lendo_ini()['matriz']), headers=self.authorization)
            response = request.json()

            return response
        except Exception as e:
            
            pass
    
    def config(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/config/get/{}'.format(self.route, self.lendo_ini()['matriz']), headers=self.authorization)
            response = request.json()

            return response
        except Exception as e:
            
            pass

    def tanques(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/tanques/get/{}'.format(self.route, self.lendo_ini()['matriz']), headers=self.authorization)
            response = request.json()

            return response
        except Exception as e:
            
            pass
    def funcionario(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }

            request = requests.get(url='{}/funcionarios/get/{}'.format(self.route, self.lendo_ini()['matriz']), headers=self.authorization)
            response = request.json()

            return response
        except Exception as e:
            
            pass

    def operador(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }

            request = requests.get(url='{}/operadores/get/{}'.format(self.route, self.lendo_ini()['matriz']), headers=self.authorization)
            response = request.json()

            return response
        except Exception as e:
            
            pass

        
    def get_filial(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/filial/get/{}'.format(self.route, self.lendo_ini()['filial']), headers=self.authorization)
        
            response = request.json()

            return response
        except Exception as e:
            print(e)
            pass

    def bicos_comboio(self):
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.get(url='{}/bicos/bicoscomboio/{}/{}'.format(self.route, self.lendo_ini()['filial'], self.lendo_ini()['idbico']), headers=self.authorization)
            response = request.json()
            
            return response
        except Exception as e:
            
            pass
    def post_abastecimento(self, dicionario=dict):
        
        retorno = {
                "id": dicionario['id'],
                "idfilial": self.lendo_ini()['filial'],
                "idcomboio": 0,
                "idbico": dicionario['idbico'],
                "data": "{}".format(dicionario['data']),
                "qtde": dicionario['qtde'],
                "idplaca": dicionario['idplaca'],
                "idfuncionario": dicionario['idfuncionario'],
                "idoperador": dicionario['idoperador'],
                "semtag": dicionario['semtag'],
                "odometro": dicionario['odometro'],
                "horimetro": dicionario['horimetro'],
                "tag": "{}".format(dicionario['tag']),
                "local": dicionario['local'],
                "tipotq": dicionario['tipotq'],
                "tipolib": dicionario['tipolib'],
                "telemetria": dicionario['telemetria']
            }
        print(retorno)
        try:
            self.authorization = {
                'Authorization': 'Bearer ' + self.token()
            }
            request = requests.post(url='{}/abastecimento/salvar'.format(self.route), json=retorno, headers=self.authorization)
        except Exception as e:
            return e



if __name__ == '__main__':
   conect = APIseal()
   