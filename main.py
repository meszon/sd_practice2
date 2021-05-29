import lithops
import pandas as pd
from io import StringIO
from pandasql import sqldf
from lithops import Storage
from lithops import FunctionExecutor
from pylab import *
import matplotlib.pyplot as plt
from lithops.multiprocessing import Pool

###
#   Raul Mesa - Victor Sentis
#   SD Practica 2 - 2021
###

#IBM config
config = {'lithops' : {'storage_bucket' : 'task2-sd'},

          'ibm_cf':  {'endpoint': 'https://eu-gb.functions.cloud.ibm.com/api/v1/namespaces/raul.mesa%40estudiants.urv.cat_dev/actions/task2-sd-test/prova',
                      'namespace': 'raul.mesa@estudiants.urv.cat_dev',
                      'api_key': '3da6529e-b610-4665-8e4d-3ad27ece6698:LHMIbuRZY9iEdB6URVyxUZGd9iZK8togDqPqr11mtryoutCnAizwV8vCfdgCLpZY'},

          'ibm_cos': {'region': 'eu-de',
                      'api_key': 'o2sIhjEWhaGB7AS1pH0XIF0ChfZg9Dks0go9eu937Y59'}
        }

#Funcion para mostrar la grafica de una consulta
def graph_plot(query, x, y):
    fig, ax = plt.subplots(figsize=(16, 7))
    query[x] = query[x].str.replace('00:00:00.000000','',regex=True)
    query[x] = query[x].str.replace('2020','20',regex=True)
    query[x] = query[x].str.replace('2021','21',regex=True)
    ax.plot(query[x], query[y])
    plt.show()

#Funcion para obtener los datos del IBM COS
def getData(select):
    storage = Storage(config=config)
    data = storage.get_object('task2-sd', 'Registre_de_casos_de_COVID-19_a_Catalunya_per_municipi_i_sexe.csv')

    format_data = str(data[0:-1], 'utf-8')
    database = pd.read_csv(StringIO(format_data))
    database["TipusCasData"]= pd.to_datetime(database["TipusCasData"])
    database = database.sort_values(by="TipusCasData")
    query = sqldf(select)

    return query

if __name__ == '__main__':
    fexec = lithops.FunctionExecutor()
    
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