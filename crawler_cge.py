from bs4 import BeautifulSoup
import urllib.request
from requests import get
from datetime import date, timedelta
import numpy as np
import pandas as pd
import googlemaps    
import sys, os
import requests

class Crawler_CGE:

    def __init__(self, lim_inferior, lim_superior):
        self.lim_inferior = lim_inferior
        self.lim_superior = lim_superior

    #-------------GET ALL DAYS DATES BETWEEN TWO DATES-------------
    def getAlldays(self):
        
        data_inferior = self.lim_inferior.split("/")

        data_superior = self.lim_superior.split("/")

        data_inicio = date(int(data_inferior[2]), int(data_inferior[1]), int(data_inferior[0]))
        data_fim = date(int(data_superior[2]), int(data_superior[1]), int(data_superior[0]))

        delta = data_fim - data_inicio 
        dias = []
        for i in range(delta.days + 1):
            day = data_inicio + timedelta(days=i)
            dias.append(day)
        
        return dias

    #-------------GET DATA FROM CGE FLASH FLOODS-------------
    def getData(self, dias):
        
        #--------create dataframe--------
        colunas = ['data','periodo', 'endereco', 'sentido', 'referencia', 'status', 'url_base', 'string']
        df = pd.DataFrame(columns = colunas)

        for i in dias:
            
            print(i)
            
            #--------create url_base CGE--------
            data = str(i).split('-')

            dia = data[2]
            mes = data[1]
            ano = data[0]

            url_base = "https://www.cgesp.org/v3/alagamentos.jsp?dataBusca="+dia+"/"+mes+"/"+ano+"+&enviaBusca=Buscar"

            response = get(url_base)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            lista_alagamentos = soup.find_all(class_='tb-pontos-de-alagamentos')

            if(len(lista_alagamentos) == 0):
                print('Não há alagamentos neste dia!')
            
            else:

                #-------find flood list-------
                for j in lista_alagamentos:
                    
                    pontos_alagamentos = j.find_all(class_='ponto-de-alagamento')
                    
                    #-------find flood point-------
                    for k in pontos_alagamentos:
                        
                        #-------find status-------
                        if (k.find(class_='ativo-transitavel')) or (k.find(class_='inativo-transitavel')):
                            status = 'transitavel'
                        elif (k.find(class_='ativo-intransitavel')) or (k.find(class_='inativo-intransitavel')):
                            status = 'intransitavel'

                        #-------find addres and reference-------
                        end_ref = k.find_all(class_='arial-descr-alag')
                        
                        cont_end_ref = 0
                        for l in end_ref:
                            features = l.find_all(text=True)

                            cont_features = 0
                            for m in features:
                                
                                if cont_end_ref == 0 and cont_features == 0:
                                    periodo = m
                                elif cont_end_ref == 1 and cont_features == 0:
                                    endereco = m
                                elif cont_end_ref == 0 and cont_features == 1:
                                    sentido = m
                                elif cont_end_ref == 1 and cont_features == 1:
                                    referencia = m

                                cont_features +=1
                            cont_end_ref+=1

                        #--------------------Save into DF--------------------
                        tam = len(df)+1
                        
                        df.loc[tam, 'data'] = i
                        df.loc[tam, 'periodo'] = periodo
                        df.loc[tam, 'endereco'] = endereco
                        df.loc[tam, 'sentido'] = sentido
                        df.loc[tam, 'referencia'] = referencia
                        df.loc[tam, 'status'] = status
                        df.loc[tam, 'url_base'] = url_base
                        df.loc[tam, 'string'] = end_ref
                        
                        print(tam)
                        print('data: ', i)
                        print('periodo: ', periodo)
                        print('endereco: ', endereco)
                        print('sentido: ', sentido)
                        print('referencia: ', referencia)
                        print('status: ', status)
                        print('url_base: ', url_base)
                        print('string: ', end_ref)
                        print('--------------------------')

        return df

    def PreprocessingData(self, df):
        
        print(df)

        #----------Flood Type----------
        df['tipo_alagamento'] = df.apply(lambda x: 1 if x.status == 'transitavel' else 0, axis=1)

        #----------Período de Alagamento----------
        df['periodo_inicial'] = 0
        df['periodo_final'] = 0

        for i in range(len(df)):
            
            periodo = df.loc[i]['periodo'].split('a')

            periodo_inicio = periodo[0].replace('De ', '')
            periodo_final = periodo[1].replace(' ','')

            df.loc[i, 'periodo_inicial'] = periodo_inicio
            df.loc[i, 'periodo_final'] = periodo_final

        #----------PREPROCESS REFERENCIA----------
        df['referencia_modify'] = 0

        aux_num = [5,10,15,20,35,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100,105,110,115,120,125,130,135,140,145,150]
        
        for i in range(len(df)):
            
            print(i)

            referencia = df.loc[i]['referencia'].split('Referência:')
            
            ref = referencia[1].replace('ALTURA DO NÚMERO', '')
            ref = ref.replace('ALTURA DO N.', '')
            ref = ref.replace('ALTURA DO Nº', '')
            ref = ref.replace('ALTURA DO NUMERO', '')
            ref = ref.replace('ALTURA DO NUMERO.', '')
            ref = ref.replace('ALTURA DO NUMERO.', '')
            ref = ref.replace('ALT. Nº', '')
            ref = ref.replace('ALT Nº', '')
            ref = ref.replace('ALT. NÚMERO', '')
            ref = ref.replace('ALT. DO N.', '')
            ref = ref.replace('ALT. DO Nº', '')
            ref = ref.replace('ALT. NR', '')
            ref = ref.replace('ALT. N.', '')
            ref = ref.replace('ALT N', '')
            ref = ref.replace('ALT ', '')
            ref = ref.replace('ALTURA', '')
            ref = ref.replace('Altura', '')
            ref = ref.replace('Altura', '')
            ref = ref.replace('nº', '')
            ref = ref.replace('ACESSO', '')
            ref = ref.replace('TODA EXTENSÃO', '')
            ref = ref.replace('<', '')
            ref = ref.replace('1,5KM ANTES', '')
            ref = ref.replace('SOB', '')
            ref = ref.replace('PROX.','')
            ref = ref.replace('INICIO DO MESMO','')
            ref = ref.replace('NO MEMSO','')
            ref = ref.replace('NO MESMO','')
            ref = ref.replace('MEIO DO MESMO','')
            ref = ref.replace('DO No','')
            ref = ref.replace('No ','')
            ref = ref.replace('ENTRE ','')

            for j in aux_num:

                aux = j*10
                
                ref = ref.replace('DESEMBOQUE '+str(j)+' M ANTES', '')
                ref = ref.replace(str(j)+'M ANTES DA', '')
                ref = ref.replace(str(j)+'M ANTES DO DESEMBOQUE', '')
                ref = ref.replace('E '+str(j)+' M ANTES DA MESMA', '')
                ref = ref.replace(str(j)+' METROS ANTES DA', '')
                ref = ref.replace('ATÉ '+str(j)+'M ANTES', '')
                ref = ref.replace('ATÉ '+str(j)+'M APÓS', '')
                ref = ref.replace('ATÉ '+str(j)+'m APÓS', '')
                ref = ref.replace('ATÉ '+str(j)+'m ANTES', '')
                ref = ref.replace(str(j)+'M ANTES', '')
                ref = ref.replace(str(j)+'M APÓS', '')
                ref = ref.replace(str(j)+' M ANTES', '')
                ref = ref.replace(str(j)+' M APÓS', '')
                ref = ref.replace(str(j)+'m APÓS', '')
                ref = ref.replace(str(j)+'m ANTES', '')
                ref = ref.replace(str(j)+'. ANTES', '')
                ref = ref.replace(str(j)+'. APÓS', '')
                ref = ref.replace(str(j)+'. APÓS', '')
                ref = ref.replace(str(j)+'. ANTES', '')

            ref = ref.split('-')
            ref = ref[0]
            
            ref = ref.split(':')
            ref = ref[0]

            df.loc[i, 'referencia_modify'] = ref
        
        df['endereco_modify'] = 0
        
        for i in range(len(df)):
            
            ref = df.loc[i, 'endereco']
            ref2 = df.loc[i, 'referencia_modify']

            ref = ref.replace('AV.', 'AVENIDA')
            ref = ref.replace('PTE.', 'PONTE')
            ref = ref.replace('PT.', 'PONTE')
            ref = ref.replace('R.', 'RUA')
            ref = ref.replace('PC.', 'PRAÇA')
            ref = ref.replace('TN.', 'TUNEL')
            ref = ref.replace('JORN.', 'JORNALISTA')
            ref = ref.replace('PROF.', 'PROFESSOR')
            ref = ref.replace('ES.', 'ESTRADA')
            ref = ref.replace('LG.', 'LARGO')
            ref = ref.replace('VD.', 'VIADUTO')
            ref = ref.replace('VELHA FEPASA', 'COMUNIDADE HUNGARA')

            ref2 = ref2.replace('AV.', 'AVENIDA')
            ref2 = ref2.replace('PTE.', 'PONTE')
            ref2 = ref2.replace('PT.', 'PONTE')
            ref2 = ref2.replace('R.', 'RUA')
            ref2 = ref2.replace('PC.', 'PRAÇA')
            ref2 = ref2.replace('TN.', 'TUNEL')
            ref2 = ref2.replace('JORN.', 'JORNALISTA')
            ref2 = ref2.replace('PROF.', 'PROFESSOR')
            ref2 = ref2.replace('ES.', 'ESTRADA')
            ref2 = ref2.replace('LG.', 'LARGO')
            ref2 = ref2.replace('VD.', 'VIADUTO')
            ref2 = ref2.replace('VELHA FEPASA', 'COMUNIDADE HUNGARA')

            df.loc[i, 'endereco_modify'] = ref
            df.loc[i, 'referencia_modify'] = ref2

        return df

    def getGeocodeData(self, dados, api_code):

        d = dados

        colunas = ['data','periodo','endereco','referencia','tipo_alagamento','tipo',
        'periodo_inicial','periodo_final','referencia_CERTA',
        'endereco_CERTO','endereco_formatado', 'latitude', 
        'longitude', 'acuracia', 'google_place_id', 'tipo', 'numero_resultados', 'status']
        
        df = pd.DataFrame(columns = colunas)        

        for i in range(len(d)):
            
            try:
                tam = len(df) + 1

                endereco = ""

                if self.tem_numeros(d.loc[i,'referencia_modify']):
                    endereco = d.loc[i, 'endereco_modify']+','+d.loc[i,'referencia_modify']+', São Paulo, Brasil'
                else:
                    endereco = d.loc[i, 'endereco_modify']+' COM '+d.loc[i,'referencia_modify']+', São Paulo, Brasil'

                resultado_vec = self.get_google_results(endereco, api_code)

                #---------------CREATE DATAFRAME-----------------
                df.loc[tam, 'data'] = d.loc[i, 'data']
                df.loc[tam, 'periodo'] = d.loc[i, 'periodo']
                df.loc[tam, 'endereco'] = d.loc[i, 'endereco']
                df.loc[tam, 'referencia'] = d.loc[i, 'referencia']
                df.loc[tam, 'tipo_alagamento'] = d.loc[i, 'tipo_alagamento']
                df.loc[tam, 'tipo'] = d.loc[i, 'status']
                df.loc[tam, 'periodo_inicial'] = d.loc[i, 'periodo_inicial']
                df.loc[tam, 'periodo_final'] = d.loc[i, 'periodo_final']
                df.loc[tam, 'endereco_CERTO'] = d.loc[i, 'endereco_modify']
                df.loc[tam, 'referencia_CERTA'] = d.loc[i, 'referencia_modify']
                df.loc[tam, 'endereco_formatado'] = resultado_vec[0]
                df.loc[tam, 'latitude'] = resultado_vec[1]
                df.loc[tam, 'longitude'] = resultado_vec[2]
                df.loc[tam, 'acuracia'] = resultado_vec[3]
                df.loc[tam, 'google_place_id'] = resultado_vec[4]
                df.loc[tam, 'tipo'] = resultado_vec[5]
                df.loc[tam, 'numero_resultados'] = resultado_vec[6]
                df.loc[tam, 'status'] = resultado_vec[7]

                print(df)
                
            except ValueError as e:
                print('Erro:', e)
        
        return df
        
    def tem_numeros(self, string):
        return any(char.isdigit() for char in string)

    
    def get_google_results(self, address, api_key=None, return_full_response=False):

        # Set up your Geocoding url
        geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
        
        if api_key is not None:
            geocode_url = geocode_url + "&key={}".format(api_key)
            
        # Ping google for the reuslts:
        results = requests.get(geocode_url)
        results = results.json()

        result_vector = []

        if len(results['results']) == 0:
            result_vector.append(0)
            result_vector.append(0)
            result_vector.append(0)
            result_vector.append(0)
            #result_vector.append(0)
            result_vector.append(0)
            result_vector.append(0)
            result_vector.append(0)
            result_vector.append(0)

        else:    
            answer = results['results'][0]
            result_vector.append(answer.get('formatted_address'))
            result_vector.append(answer.get('geometry').get('location').get('lat'))
            result_vector.append(answer.get('geometry').get('location').get('lng'))
            result_vector.append(answer.get('geometry').get('location_type'))
            #result_vector.append(answer.get("place_id"))
            result_vector.append(answer.get('types'))
            result_vector.append(address)
            result_vector.append(len(results['results']))
            result_vector.append(results.get('status'))
        
        return result_vector

    

if __name__ == "__main__":

    local = '/home/thiago-costa/projects/crawler_cge/'

    data_inf = '01/10/2019'
    data_sup = '31/10/2019'
    api_key = "SUA API GOOGLE GEOCODING"
    
    cge = Crawler_CGE(data_inf, data_sup)

    #--------------CGE GET DATA--------------
    '''
    dias = cge.getAlldays()
    result = cge.getData(dias)
    
    print(result)

    result.to_csv(local+'alagamentos_2018_2019.csv')
    '''
    #-------------Coordinates-------------- 
    
    aux = pd.read_csv(local+'alagamentos_2018_2019.csv')
    
    aux = aux.reset_index(drop=True)

    result = cge.PreprocessingData(aux)

    result = cge.getGeocodeData(aux, api_key)

    print(result)

    result.to_csv(local+'alagamentos_2018_2019_COORDS.csv')
    