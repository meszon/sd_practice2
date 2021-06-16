import lithops
import pandas as pd
from io import StringIO
from pandasql import sqldf
from lithops import Storage
from lithops import FunctionExecutor
from pylab import *
import matplotlib.pyplot as plt
from lithops.multiprocessing import Pool
#from config_file import config

###
#   Raul Mesa - Victor Sentis
#   SD Practica 2 - 2021
###

#Strings con los objetos de IBM Cloud
objectCSV_1 = 'Registre_de_casos_de_COVID-19_a_Catalunya_per_municipi_i_sexe.csv'
objectCSV_2 = 'Incid_ncia_de_la_COVID-19_a_Catalunya.csv'
objectCSV_3 = 'Vacunacio_COVID.csv'

objectCSV_4 = 'Dades_del_mapa_urban_stic_de_Catalunya.csv'
objectCSV_6 = 'Preu_mitja_lloguer_municipi.csv'

objectCSV_DB = "database.csv"

#Funcion para mostrar la grafica de una consulta
#def graph_plot(query, x, y):
    #fig, ax = plt.subplots(figsize=(16, 7))
    #query[x] = query[x].str.replace('00:00:00.000000','',regex=True)
    #query[x] = query[x].str.replace('2020','20',regex=True)
    #query[x] = query[x].str.replace('2021','21',regex=True)
    #ax.plot(query[x], query[y])
    #plt.show()

def graph_plot(query, x, y):
    fig, ax = plt.subplots(figsize=(16, 7))
    query[x] = formatar(query[x])
    ax.plot(query[x], query[y])
    plt.show()

def graph_plot_multiline(query1, x1, y1, label1, query2, x2, y2, label2):
    fig, ax = plt.subplots(figsize=(16, 7))
    query1[x1] = formatar(query1[x1])
    ax.plot(query1[x1], query1[y1], label = label1)
    query2[x2] = formatar(query2[x2])
    ax.plot(query2[x2], query2[y2], label = label2)
    plt.legend()
    plt.show()

def formatar(query):
    query = query.str.replace('00:00:00.000000','',regex=True)
    query = query.str.replace('2020','20',regex=True)
    query = query.str.replace('2021','21',regex=True)
    return query

#Funcion para Preprocesar los datos CSV
def processData(nu):
    storage = Storage(config=config)

    data = storage.get_object('task2-sd', objectCSV_1)
    format_data = str(data[0:-1], 'utf-8')
    database_old = pd.read_csv(StringIO(format_data))
    database1 = database_old[['TipusCasData','ComarcaDescripcio','MunicipiCodi','MunicipiDescripcio','NumCasos']].copy()
    #database1["TipusCasData"]= pd.to_datetime(database1["TipusCasData"])
    #database1.sort_values(by="TipusCasData", inplace=True)

    data = storage.get_object('task2-sd', objectCSV_2)
    format_data = str(data[0:-1], 'utf-8')
    database_old = pd.read_csv(StringIO(format_data))
    database2 = database_old[['Data','Defuncions diàries','Altes diàries','Total de defuncions','Total d\'altes',]].copy()
    database2.rename(columns={'Data':'TipusCasData','Defuncions diàries':'DefuncionsDiaries','Altes diàries':'AltesDiaries','Total de defuncions':'TotalDefuncions',
    'Total d\'altes': 'TotalAltes'}, inplace=True)
    #database2["TipusCasData"]= pd.to_datetime(database2["TipusCasData"])
    #database2.sort_values(by="TipusCasData", inplace=True)

    final_database1 = pd.merge(left=database1, right=database2, how='left', left_on='TipusCasData', right_on='TipusCasData')

    final_database1.to_csv('database.csv', index = False)
    database = open('database.csv', 'r')
    storage.put_object('task2-sd', 'database.csv', database.read())
    database.close()
    #storage.put_object('task2-sd', 'database.csv', final_database1.to_string())

    #query = sqldf(select)
    #return query
    return final_database1


#Funcion para obtener los datos del IBM COS
def getData(select):
    storage = Storage(config=config)
    #data = storage.get_object('task2-sd', objectCSV_1)
    data = storage.get_object('task2-sd', 'database.csv')

    format_data = str(data[0:-1], 'utf-8')
    database = pd.read_csv(StringIO(format_data))

    database["TipusCasData"]= pd.to_datetime(database["TipusCasData"])
    database = database.sort_values(by="TipusCasData")
    query = sqldf(select)

    return query


if __name__ == '__main__':

    fexec = lithops.FunctionExecutor()

    #fexec.call_async(processData, "None")
    #print(fexec.get_result)

    #Query de altas diarias durante el año 2020
    query = Pool().map(getData, ["SELECT DISTINCT TipusCasData, AltesDiaries FROM database WHERE AltesDiaries IS NOT NULL",
                              "SELECT DISTINCT TipusCasData, DefuncionsDiaries FROM database WHERE AltesDiaries IS NOT NULL"])
    #print(query)

    graph_plot_multiline(query[0], 'TipusCasData', 'AltesDiaries', 'Altes diàries', query[1], 'TipusCasData', 'DefuncionsDiaries', 'Defuncions diàries')

