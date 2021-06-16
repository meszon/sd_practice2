import lithops
import pandas as pd
from io import StringIO
from lithops import Storage
from lithops import FunctionExecutor

###
#   Raul Mesa - Victor Sentis
#   SD Practica 2 - 2021
###

#Strings con los objetos de IBM Cloud
objectCSV_1 = 'Registre_de_casos_de_COVID-19_a_Catalunya_per_municipi_i_sexe.csv'
objectCSV_2 = 'Incid_ncia_de_la_COVID-19_a_Catalunya.csv'


#Funcion para Preprocesar los datos CSV
def processData(nu):
    storage = Storage()

    data = storage.get_object('task2-sd', objectCSV_1)
    format_data = str(data[0:-1], 'utf-8')
    database_old = pd.read_csv(StringIO(format_data))
    database1 = database_old[['TipusCasData','ComarcaDescripcio','MunicipiCodi','MunicipiDescripcio','NumCasos']].copy()

    data = storage.get_object('task2-sd', objectCSV_2)
    format_data = str(data[0:-1], 'utf-8')
    database_old = pd.read_csv(StringIO(format_data))
    database2 = database_old[['Data','Defuncions diàries','Altes diàries','Total de defuncions','Total d\'altes',]].copy()
    database2.rename(columns={'Data':'TipusCasData','Defuncions diàries':'DefuncionsDiaries','Altes diàries':'AltesDiaries','Total de defuncions':'TotalDefuncions',
    'Total d\'altes': 'TotalAltes'}, inplace=True)

    final_database1 = pd.merge(left=database1, right=database2, how='left', left_on='TipusCasData', right_on='TipusCasData')

    final_database1.to_csv('database.csv', index = False)
    database = open('database.csv', 'r')
    storage.put_object('task2-sd', 'database.csv', database.read())
    database.close()



if __name__ == '__main__':

    fexec = lithops.FunctionExecutor(runtime='meszon/lithops-custom-runtimev38:0.1', runtime_memory=2048)
    fexec.call_async(processData, "None")
    fexec.wait()

    




