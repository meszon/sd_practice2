import lithops
import pandas as pd
from io import StringIO
from pandasql import sqldf
from lithops import Storage
from lithops import FunctionExecutor

#pip install -U pandasql

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
    query = sqldf("SELECT * FROM database")
    query2 = sqldf("SELECT * FROM database WHERE ComarcaDescripcio='BAIX LLOBREGAT'")
    print(query)
    print(query2)
