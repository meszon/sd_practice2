import lithops
import pandas as pd
from io import StringIO
from pandasql import sqldf
from lithops import Storage
from lithops import FunctionExecutor
from pylab import *
import matplotlib.pyplot as plt
from lithops.multiprocessing import Pool
from config_file import config

###
#   Raul Mesa - Victor Sentis
#   SD Practica 2 - 2021
###

#Strings con los objetos de IBM Cloud
objectCSV_1 = 'Registre_de_casos_de_COVID-19_a_Catalunya_per_municipi_i_sexe.csv'
objectCSV_2 = 'Dades_di_ries_de_COVID-19_per_comarca.csv'
objectCSV_3 = 'Incid_ncia_de_la_COVID-19_a_Catalunya.csv'

#Funcion para mostrar la grafica de una consulta
def graph_plot(query, x, y):
    fig, ax = plt.subplots(figsize=(16, 7))
    query[x] = query[x].str.replace('00:00:00.000000','',regex=True)
    query[x] = query[x].str.replace('2020','20',regex=True)
    query[x] = query[x].str.replace('2021','21',regex=True)
    ax.plot(query[x], query[y])
    plt.show()


#Funcion para Preprocesar los datos CSV
def processData(select):
    storage = Storage(config=config)

    data = storage.get_object('task2-sd', objectCSV_1)
    format_data = str(data[0:-1], 'utf-8')
    database_old = pd.read_csv(StringIO(format_data))
    database1 = database_old[['TipusCasData','ComarcaDescripcio','MunicipiCodi','MunicipiDescripcio','SexeDescripcio','NumCasos']].copy()
    #database1["TipusCasData"]= pd.to_datetime(database1["TipusCasData"])
    database1 = database1.sort_values(by="TipusCasData")

    data2 = storage.get_object('task2-sd', objectCSV_2)
    format_data2 = str(data2[0:-1], 'utf-8')
    database_old = pd.read_csv(StringIO(format_data2))
    database2 = database_old[['DATA','NOM','GRUP_EDAT','CASOS_CONFIRMAT','PCR','EXITUS']].copy()
    database2.rename(columns={'DATA':'TipusCasData', 'EXITUS':'Defuncions', 'NOM':'Comarca','GRUP_EDAT':'Rang_edat', 'CASOS_CONFIRMAT':'Casos_confirmats', 'PCR':'N_PCR'}, inplace=True)
    #database2["TipusCasData"]= pd.to_datetime(database2["TipusCasData"])
    database2 = database2.sort_values(by="TipusCasData")

    #final_database = pd.merge(left=database1, right=database2, left_on='TipusCasData', right_on='TipusCasData')

    query = sqldf(select)
    return query








#Funcion para obtener los datos del IBM COS
def getData(select):
    storage = Storage(config=config)
    data = storage.get_object('task2-sd', objectCSV_1)

    format_data = str(data[0:-1], 'utf-8')
    database = pd.read_csv(StringIO(format_data))

    database["TipusCasData"]= pd.to_datetime(database["TipusCasData"])
    database = database.sort_values(by="TipusCasData")
    query = sqldf(select)

    return query

if __name__ == '__main__':
    fexec = lithops.FunctionExecutor()
    
    fexec.call_async(processData, "SELECT * FROM database2 GROUP BY TipusCasData LIMIT 100")
    print(fexec.get_result())
    
'''
    #Query consulta n casos por tiempo en una comarca
    fexec.call_async(getData, "SELECT ComarcaDescripcio FROM database GROUP BY ComarcaDescripcio")
    comarques = fexec.get_result()
    print(comarques)
    print("Indica la comarca ha buscar entre les mostrades:")
    comarca = input().upper()
    fexec.call_async(getData, "SELECT NumCasos, TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"' GROUP BY TipusCasData")
    query = fexec.get_result()

    graph_plot(query, 'TipusCasData', 'NumCasos')
    #---------------------------------------------------------------------------------------------------------------

    #Query consulta n. casos por comarca
    print("Indica el rang de comarques (1.A-L, 2.M-Z):")  
    rango = input()
    if rango == '1': rango = '<'
    else: rango = '>='
    fexec.call_async(getData, "SELECT SUM(NumCasos) AS TotalCasos, ComarcaDescripcio FROM database WHERE ComarcaDescripcio " + rango + " 'L%'  GROUP BY ComarcaDescripcio")
    query = fexec.get_result()

    fig, ax = plt.subplots(figsize=(16, 7))
    ax.barh(query['ComarcaDescripcio'], query['TotalCasos'])
    plt.show()
    #---------------------------------------------------------------------------------------------------------------

    #Query consulta n. casos en un mes de todas las comarcas
    #Recomendado no superar dos meses
    print("Indica la data d'inici (YYYY-MM-DD):")  
    data_inici = input()
    print("Indica la data de fi (YYYY-MM-DD):")  
    data_fi = input()
    fexec.call_async(getData, "SELECT SUM(NumCasos) AS TotalCasos, TipusCasData FROM database WHERE TipusCasData BETWEEN '" + data_inici + "' AND '" + data_fi + "' GROUP BY TipusCasData")
    query = fexec.get_result()

    graph_plot(query, 'TipusCasData', 'TotalCasos')
    #---------------------------------------------------------------------------------------------------------------

    #Queries consulta n. casos de una comarca
    print(comarques)
    print("Indica la comarca ha buscar entre les mostrades:")
    comarca = input().upper()
    query = Pool().map(getData, [
        "SELECT SUM(NumCasos) AS TotalCasos, TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"' AND TipusCasData BETWEEN '2020-01-01' AND '2020-03-01' GROUP BY TipusCasData",
        "SELECT SUM(NumCasos) AS TotalCasos, TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"' AND TipusCasData BETWEEN '2020-03-01' AND '2020-05-01' GROUP BY TipusCasData",
        "SELECT SUM(NumCasos) AS TotalCasos, TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"' AND TipusCasData BETWEEN '2020-05-01' AND '2020-07-01' GROUP BY TipusCasData",
        "SELECT SUM(NumCasos) AS TotalCasos, TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"' AND TipusCasData BETWEEN '2020-07-01' AND '2020-09-01' GROUP BY TipusCasData",
        "SELECT SUM(NumCasos) AS TotalCasos, TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"' AND TipusCasData BETWEEN '2020-09-01' AND '2020-11-01' GROUP BY TipusCasData",
        "SELECT SUM(NumCasos) AS TotalCasos, TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"' AND TipusCasData BETWEEN '2020-11-01' AND '2021-01-01' GROUP BY TipusCasData"
    ])
    
    for q in query:
        graph_plot(q, 'TipusCasData', 'TotalCasos')
    #---------------------------------------------------------------------------------------------------------------
'''