import lithops
import pandas as pd
from io import StringIO
from pandasql import sqldf
from lithops import Storage
from lithops import FunctionExecutor
from pylab import *
from PIL import Image
import datetime

#pip install -U pandasql
#test final

config = {'lithops' : {'storage_bucket' : 'task2-sd'},

          'ibm_cf':  {'endpoint': 'https://eu-gb.functions.cloud.ibm.com/api/v1/namespaces/raul.mesa%40estudiants.urv.cat_dev/actions/task2-sd-test/prova',
                      'namespace': 'raul.mesa@estudiants.urv.cat_dev',
                      'api_key': '3da6529e-b610-4665-8e4d-3ad27ece6698:LHMIbuRZY9iEdB6URVyxUZGd9iZK8togDqPqr11mtryoutCnAizwV8vCfdgCLpZY'},

          'ibm_cos': {'region': 'eu-de',
                      'api_key': 'o2sIhjEWhaGB7AS1pH0XIF0ChfZg9Dks0go9eu937Y59'}
        }

def getData(nu):
    storage = Storage(config=config)
    #data = storage.get_object('task2-sd', 'Registre_de_casos_de_COVID-19_a_Catalunya_per_municipi_i_sexe.csv')
    data = storage.get_object('task2-sd', 'registres.csv')
    return data

if __name__ == '__main__':
    fexec = lithops.FunctionExecutor()
    fexec.call_async(getData, '1')
    data = fexec.get_result()
    format_data = str(data[0:-1], 'utf-8')
    #print(format_data)
    
    #pysqldf = lambda q: sqldf(q, globals())   ----------> Parece que funciona bien sin esto
    database = pd.read_csv(StringIO(format_data))
    #df1 = pysqldf("SELECT * FROM df")         ----------> Se puede utilizar sqldf en lugar de pysqldf
    database["TipusCasData"]= pd.to_datetime(database["TipusCasData"])
    database = database.sort_values(by="TipusCasData")
    #database.groupby(database['TipusCasData'].dt.strftime('%M'))['NumCasos'].sum()
    
    #query = sqldf("SELECT * FROM database")
    #query2 = sqldf("SELECT * FROM database WHERE ComarcaDescripcio='BAIX LLOBREGAT'")
    #print(query)
    #print(query2)
    comarca = "BAIX LLOBREGAT"
    numCasos = sqldf("SELECT NumCasos FROM database WHERE ComarcaDescripcio='"+comarca+"'")
    tipusCasData = sqldf("SELECT TipusCasData FROM database WHERE ComarcaDescripcio='"+comarca+"'")

    print(numCasos)
    print(tipusCasData)

    listNumCasos = str(numCasos.to_numpy()).replace('\n',',').replace('[','').replace(']','').split(',')

    listTipusCasData = str(tipusCasData.to_numpy()).replace('\n',',').replace('[','').replace(']','').replace('00:00:00.000000','').replace('2020','20').replace('2021','21').replace("'",'').replace(' ','').split(',')
    print (listTipusCasData)
    
    #si hacemos esto hay dos valores Y para cada X
    newList = []
    for element in listTipusCasData:
        element = element[:-5]
        newList.append(element)

    #listTipusCasData = newList
    #print (listTipusCasData)

    plot(listTipusCasData, listNumCasos)
    
    xlabel('Temps historic')
    ylabel('Casos detectats')
    title('Casos covid')
    draw()
    savefig("graficoSD",dpi=400)
    close()
    img = Image.open("./graficoSD.png")
    img.show()





