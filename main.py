import lithops
from lithops import Storage

config = {'lithops' : {'storage_bucket' : 'task2-sd'},

          'ibm_cf':  {'endpoint': 'https://eu-gb.functions.cloud.ibm.com/api/v1/namespaces/raul.mesa%40estudiants.urv.cat_dev/actions/task2-sd-test/prova',
                      'namespace': 'raul.mesa@estudiants.urv.cat_dev',
                      'api_key': '3da6529e-b610-4665-8e4d-3ad27ece6698:LHMIbuRZY9iEdB6URVyxUZGd9iZK8togDqPqr11mtryoutCnAizwV8vCfdgCLpZY'},

          'ibm_cos': {'region': 'eu-de',
                      'api_key': 'o2sIhjEWhaGB7AS1pH0XIF0ChfZg9Dks0go9eu937Y59'}
        }

def hello_world(name):
    return 'Hello {}!'.format(name)

if __name__ == '__main__':
    #fexec = lithops.FunctionExecutor(config=config)
    #fexec.call_async(hello_world, 'World')
    #print(fexec.get_result())
    storage = Storage(config=config)
    data = storage.get_object('task2-sd', 'Registre_de_casos_de_COVID-19_a_Catalunya_per_municipi_i_sexe.csv')
    print(data)