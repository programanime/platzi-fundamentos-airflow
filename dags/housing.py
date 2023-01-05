from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import os
from io import BytesIO
import zipfile
# import pandas as pd
# import requests

# from sklearn.preprocessing import MinMaxScaler
# from sklearn.model_selection import train_test_split


from airflow import DAG
from datetime import datetime
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    
@dag(dag_id="primer_housing",
    description="primer housing process",
    start_date=datetime.now(),
    schedule_interval="@daily",
    default_args={
        "depends_on_past":True
    }
)
def PrimerHousing():
    @task()
    def start():
        print("iniciando...")

    @task()
    def criar_diretorio():
       os.makedirs('data/housing', exist_ok=True)

    @task()
    def download_housing_file():
        install("requests")
        import requests
        url = "https://github.com/ozeiasgodoy/notebooks/blob/main/dados/housing.zip?raw=true"

        filebytes_housing = BytesIO(
            requests.get(url).content
        )
        with open("data/housing/housing.zip", "wb") as outfile:
            outfile.write(filebytes_housing.getbuffer())

    @task()
    def extract_housing_file():
        myzip = zipfile.ZipFile("data/housing/housing.zip")
        myzip.extractall('data/housing')

    @task()
    def criar_novos_campos():
        install("pandas")
        import pandas as pd
        housing = pd.read_csv('data/housing/housing.csv')
        housing['rooms_per_household'] = housing['total_rooms']/housing['households']
        housing['bedrooms_per_room'] = housing['total_bedrooms']/housing['total_rooms']
        housing['population_per_household'] = housing['population']/housing['households']
        housing.to_csv("data/housing/housing_campos_novos.csv")

    @task()
    def tratar_campos_nulos():
        install("pandas")
        import pandas as pd
        housing = pd.read_csv('data/housing/housing_campos_novos.csv')
        housing['total_bedrooms'] = housing['total_bedrooms'].fillna(housing['total_bedrooms'].mean())
        housing['bedrooms_per_room'] =housing['bedrooms_per_room'].fillna(housing['bedrooms_per_room'].mean())
        housing.to_csv("data/housing/housing_sem_campos_nulos.csv")

    @task()
    def aplicar_one_hot_encoding():
        install("pandas")
        import pandas as pd
        housing = pd.read_csv('data/housing/housing_sem_campos_nulos.csv')
        housing  = pd.get_dummies(housing, columns=['ocean_proximity'])
        housing.to_csv("data/housing/housing_hot_encoding.csv")

    @task()
    def normalizar_dados():
        install("scikit-learn")
        install("pandas")
        import pandas as pd
        from sklearn.preprocessing import MinMaxScaler
        housing = pd.read_csv('data/housing/housing_hot_encoding.csv')
        min_max_scaler = MinMaxScaler()
        min_max_scaler.fit(housing)

        housing[['longitude', 'latitude', 'housing_median_age', 'total_rooms',	'total_bedrooms', 'population',
         'households', 	'median_income', 'rooms_per_household','bedrooms_per_room',
         'population_per_household']] = min_max_scaler.fit_transform(
             housing[['longitude', 'latitude', 'housing_median_age', 'total_rooms',	'total_bedrooms', 'population',
            'households', 	'median_income', 'rooms_per_household','bedrooms_per_room','population_per_household']]
         )
        housing.to_csv("data/housing/housing_normalizado.csv")

    @task()
    def dividir_dados_treino_teste():
        install("scikit-learn")
        install("pandas")
        import pandas as pd
        from sklearn.model_selection import train_test_split
        housing = pd.read_csv('data/housing/housing_normalizado.csv')
        housing_train, housing_test = train_test_split(housing, test_size=0.3, random_state=42)
        housing_train.to_csv("data/housing/housing_train.csv")
        housing_test.to_csv("data/housing/housing_test.csv")

    @task()
    def treinar_LinearRegression():
        install("scikit-learn")
        install("pandas")
        import pandas as pd
        from sklearn.linear_model import LinearRegression
        from joblib import dump
        housing = pd.read_csv('data/housing/housing_train.csv')
        X_train = housing.drop(["median_house_value"], axis=1)
        Y_train = housing["median_house_value"]
 
        #Modelos de classificação
        lr = LinearRegression()
        lr.fit(X_train, Y_train)
        dump(lr, "data/housing/LinearRegression_housing.joblib")

    @task()
    def treinar_DecisionTreeRegressor():
        install("scikit-learn")
        install("pandas")
        import pandas as pd
        from sklearn.tree import DecisionTreeRegressor
        from joblib import dump
        housing = pd.read_csv('data/housing/housing_train.csv')
        X_train = housing.drop(["median_house_value"], axis=1)
        Y_train = housing["median_house_value"]
 
        lr = DecisionTreeRegressor()
        lr.fit(X_train, Y_train)
        dump(lr, "data/housing/DecisionTreeRegressor(housing.joblib")

    @task()
    def treinar_RandomForestRegressor():
        install("scikit-learn")
        install("pandas")
        import pandas as pd
        from joblib import dump
        from sklearn.ensemble import RandomForestRegressor
        housing = pd.read_csv('data/housing/housing_train.csv')
        X_train = housing.drop(["median_house_value"], axis=1)
        Y_train = housing["median_house_value"]
 
        #Modelos de classificação
        rf = RandomForestRegressor(n_estimators=50, random_state=42)
        rf.fit(X_train, Y_train)
        dump(rf, "data/housing/RandomForestRegressor_housing.joblib")

    @task()
    def treinar_SVC():
        install("scikit-learn")
        install("pandas")
        import pandas as pd
        from joblib import dump
        from sklearn.svm import SVC
        housing = pd.read_csv('data/housing/housing_train.csv')
        X_train = housing.drop(["median_house_value"], axis=1)
        Y_train = housing["median_house_value"]
 
        svc = SVC(kernel='linear', gamma='scale', random_state=42)
        svc.fit(X_train, Y_train)
        dump(svc, "data/housing/SVC_housing.joblib")
    
    @task()
    def treinar_KNeighborsRegressor():
        install("scikit-learn")
        install("pandas")
        import pandas as pd
        from joblib import dump
        from sklearn.neighbors import KNeighborsRegressor
        housing = pd.read_csv('data/housing/housing_train.csv')
        X_train = housing.drop(["median_house_value"], axis=1)
        Y_train = housing["median_house_value"]
        
        #Modelos de classificação
        knn = KNeighborsRegressor()
        knn.fit(X_train, Y_train)
        dump(knn, "data/housing/KNeighborsRegressor_housing.joblib")

    (
        start() >> 
        criar_diretorio() >> 
        download_housing_file() >> 
        extract_housing_file() >> 
        criar_novos_campos() >> 
        tratar_campos_nulos() >> 
        aplicar_one_hot_encoding() >>
        normalizar_dados() >>
        dividir_dados_treino_teste() >>
        [ 
            treinar_LinearRegression(),
            treinar_DecisionTreeRegressor(),
            treinar_RandomForestRegressor(),
            treinar_SVC(),
            treinar_KNeighborsRegressor()
        ]
    )

dag = PrimerHousing()