import os
import re
import sqlite3
import time
import queue
import threading
from bs4 import BeautifulSoup
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def extract_data_from_parsed_table(data):
    """
    Extraigo los datos de de la tabla. Guardo los datos en una lista de diccionarios por cada fila.
    Return:
        header_list: Cabeceras de la tabla, las columnas 
        table: Los datos de la tabla

    """
    head = data.thead.tr # head row of the table. Columnas
    head = head.find_all('th')
    header_list = []
    for i in head:
        #print(type(i))
        #print(i.text) # Accedo a los datos.
        header_list.append(i.text)

    print(f"\n{header_list}\n")
    #body = data.tbody   # Datos
    table = []  # La lista final que tendrá todos los diccionarios
    for i in data.tbody.find_all("tr"): # columns
        row_list = {}   # Crear un nuevo diccionario que usaré para cada iteración
        #print(type(i))
        row = i.find_all("td")  # rows
        # en cada iteración guardo el tipo de dato al que corresponde en el diccionario
        for j, text in enumerate(row):
            #print(text.get_text())
            if(j==0):
                row_list["year"] = int(text.get_text())
            elif(j==1):
                # [0-9.] permite solo numeros del 0-0 y puntos
                # [^0-9.] niega lo que está dentro con ^
                row_list["revenue"] = float(re.sub(r'[^0-9.-]',"",text.get_text()))
            elif(j==2):
                _text = re.sub(r'[^0-9.-]',"",text.get_text())
                if(_text != ""):
                    row_list["change"] = float(_text)
                else:
                    row_list["change"] = 0.0
        table.append(row_list)
        
    print("TABLA:",table)
    return header_list, table


def create_tables(cur):
    # Creo la tabla revenue y earnings si no existe en la bd tesla.db
    # Revenue
    table_revenue = "revenue"
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_revenue,))
    exist_revenue = cur.fetchone() is not None  # Retorna True si la tabla existe, False si no
    if(not exist_revenue):
        cur.execute("CREATE TABLE revenue (year, revenue, change);")
    # Earnings
    table_earnings = "earnings"
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_earnings,))
    exist_earnings = cur.fetchone() is not None  # Retorna True si la tabla existe, False si no
    if(not exist_earnings):
        cur.execute("CREATE TABLE earnings (year, earnings, change);")
    

# def get_fila(row):
#     """ Obtiene los datos de una fila y lo devuelve como una tupla"""
#     return (row["Year"], row["Revenue"], row["Change"])
def get_fila(row):
    """ Obtiene los datos de una fila y lo devuelve como una tupla"""
    return (row.iloc[0], row.iloc[1], row.iloc[2])

def insert_data(df,cur,conexion):
    if(df.columns[1] == "Revenue"):
        row_earnings = df.apply(get_fila, axis=1).to_list()
        print("\nFila en insert_data:\n",df.columns[1],"\n")
        cur.executemany(
        """
        INSERT INTO revenue VALUES (?,?,?)
        """, row_earnings
        )
        conexion.commit()
    elif(df.columns[1] == "Earnings"):
        row_revenue = df.apply(get_fila, axis=1).to_list()
        cur.executemany(
        """
        INSERT INTO earnings VALUES (?,?,?)
        """, row_revenue
        )
        print("\nFila en insert_data:\n",df.columns[1],"\n")
        conexion.commit()
    # for i,data in enumerate(df):
    #     row = data.apply(get_fila, axis=1).to_list()
    #     print("Fila en insert_data:{i}\n",data,"\n")
    # INSERT data de forma masiva
    
    # cur.executemany(
    #     """
    #     INSERT INTO revenue VALUES (?,?,?)
    # """, row
    # )
    
    # cur.executemany(
    #     """
    #     INSERT INTO earnings VALUES (?,?,?)
    # """, row
    # )
    # conexion.commit()
def connect_create_insert_db(df):
    """
    If tesla.db exists connect to the database, create the tables, insert data in the tables
    """
    # conexión a la BD tesla
    db_name = "tesla.db"
    con = sqlite3.connect(db_name)
    # cursor
    cur = con.cursor()
    if os.path.exists(db_name):
        print("La base de datos ya existe\n")
        # elimino la tabla para evitar problemas. Solución parcial. 
        # Comprobar si existe la tabla mejor
        # cur.execute("DROP TABLE revenue;")
        # cur.execute("DROP TABLE earnings;")
        
        # creo las tablas si no existen
        create_tables(cur = cur)
        # inserto los datos en la base de datos
        insert_data(df, cur = cur, conexion = con)
        # Muestro los datos de la BD.
        res_revenues = cur.execute("SELECT * FROM revenue;")
        print("\n",res_revenues.fetchall(),"\n")
        res_earnings = cur.execute("SELECT * FROM earnings;")
        print("\n",res_earnings.fetchall(),"\n")
        # cierro el cursor y la conexión
        cur.close()
        con.close()
    else:
        print("La base de datos no existe\n")
        # creo las tablas
        create_tables(cur = cur)
        # inserto los datos en la base de datos
        insert_data(df, cur = cur, conexion = con)
        # Muestro los datos de la BD.
        res_revenues = cur.execute("SELECT * FROM revenue;")
        print("\n",res_revenues.fetchall(),"\n")
        res_earnings = cur.execute("SELECT * FROM earnings;")
        print("\n",res_earnings.fetchall(),"\n")
        # cierro el cursor y la conexión
        cur.close()
        con.close()
# COMPLETAR FUNCIÓN DE GRAFICAR
def plot_dataframe_data(dataframe):
    ## GRAFICAR
    # Gráfica de lineas
    fig, axes = plt.subplots(1, 1, figsize = (10,5))
    # Primer gráfico - OK
    sns.lineplot(dataframe, x="Year", y=dataframe.iloc[:,1])
    sns.regplot(dataframe, x="Year", y=dataframe.iloc[:,1])
    plt.tight_layout()
    plt.show()
    # Segundo gráfico
    #print(df.dtypes)
    # plt.figure(figsize=(10,5))
    # sns.scatterplot(data=df, x="Revenue", y="Change")
    # plt.tight_layout()
    # plt.show()
    # Gráfico 3
    # plt.figure(figsize=(10,5))
    # sns.barplot(data=df, x="Year", y="Revenue", color="blue", label = "Revenue")
    # sns.barplot(data=df, x="Year", y="Change", color="red", label = "Change")
    # plt.tight_layout()
    # plt.legend()
    # plt.show()
    # Gráfico 4
    # plt.figure(figsize=(10,5))
    # sns.boxplot(data=df, x="Year", y="Revenue")
    # plt.tight_layout()
    # plt.legend()
    # plt.show()
# Graficar Concurrentemente
def plot_dataframe_data_threading(dataframes):
    ## GRAFICAR
    # Gráfica de lineas
    fig, axes = plt.subplots(2, 1, figsize = (10,5))
    
    # Primer gráfico - OK
    dataframe_aux = [pd.DataFrame(data=d) for d in dataframes ]
    print("Esto tiene el df concurrente:",dataframe_aux)
    for i,data in enumerate(dataframe_aux):
        #plt.figure(figsize=(8,5))
        sns.lineplot(data, x="Year", y=data.iloc[:,1], ax = axes[i])
        sns.regplot(data, x="Year", y=data.iloc[:,1], ax = axes[i])
    plt.tight_layout()
    plt.show(block=False)
    # Crear nueva figura para el scatterplot
    _, axes_scatterplot = plt.subplots(2, 1, figsize = (10,8))
    for i,data in enumerate(dataframe_aux):
        sns.scatterplot(data=data, x=data.iloc[:, 1], y="Change",ax=axes_scatterplot[i])
        sns.regplot(data, x=data.iloc[:,1], y="Change", ax = axes_scatterplot[i])
    plt.title("Scatterplot Change",loc='right')
    plt.show(block=False)
    
    df_earnings, df_revenue = dataframe_aux
    
    df_combined = df_earnings.merge(df_revenue, on="Year", suffixes=("_earnings", "_revenue"))
    df_combined = df_combined.sort_values(by="Year", ascending=True)
    df_combined.plot(x="Year", y=["Earnings", "Revenue"], kind="bar", figsize=(12, 6))
    plt.title("Comparación de Earnings y Revenue por año")
    plt.show()

    

def get_data_from_url(url):
    start = time.time()
    # GET al servidor
    response = requests.get(url = url, timeout = 10 )
    # Si todo es correcto
    if response.status_code == 200:
        ## WEB SCRAPING
        # Parseo los datos para que se puedan recorrer
        soup = BeautifulSoup(response.text, 'html.parser')
        # Encuentra la primera tabla y guarda los datos parseados
        data = soup.find_all("table", class_ = "table", limit=1)[0] # devuelve una lista por eso el [0]
        # Extraigo los datos de la tabla que pasaré al DataFrame
        headers, table_data = extract_data_from_parsed_table(data)
        df = pd.DataFrame(data=table_data)
        df.columns = headers
        # df = df.dropna() 
        print("\nDATAFRAME\n",df)
        ## BBDD
        connect_create_insert_db(df=df)
        # Tiempo de ejecución del programa
        end = time.time()
        print(f"Tiempo de ejecución (Sin concurrencia):{end-start:.4f} segundos")
        ## GRAFICAR
        plot_dataframe_data(dataframe=df)
    # Si no es correcto
    else:
        print("Something went wrong.")
## CONCURRENTE
def get_data_from_url_threading(url, result_queue):
    # GET al servidor
    response = requests.get(url = url, timeout = 10 )
    # Si todo es correcto
    if response.status_code == 200:
        ## WEB SCRAPING
        # Parseo los datos para que se puedan recorrer
        soup = BeautifulSoup(response.text, 'html.parser')
        # Encuentra la primera tabla y guarda los datos parseados
        data = soup.find_all("table", class_ = "table", limit=1)[0] # devuelve una lista por eso el [0]
        # Extraigo los datos de la tabla que pasaré al DataFrame
        headers, table_data = extract_data_from_parsed_table(data)
        df = pd.DataFrame(data=table_data)
        df.columns = headers
        # df = df.dropna() 
        print("\nDATAFRAME\n",df)
        ## BBDD
        connect_create_insert_db(df=df)
        ## GRAFICAR
        #plot_dataframe_data(dataframe=df)
        result_queue.put(df) # añado el df a la cola
    # Si no es correcto
    else:
        print("Something went wrong.")

def concurrence_execution_get_data_from_url(urls):
    """Function execution of threads in concurrence of the function get_data_from_url_threading."""
    start = time.time()
    threads = []
    # Concurrente
    result_queue = queue.Queue() # cola para pasar datos entre hilos
    ##
    for url_dict in urls :
        url_arg = url_dict["url"] # Extrae la URL del diccionario
        thread = threading.Thread(target=get_data_from_url_threading, args=(url_arg,result_queue)) # paso la cola al thread
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    results = []
    while not result_queue.empty(): # si la no está vacía
        dict_queue = dict(result_queue.get())   # diccionario de df (put)
        results.append(dict_queue)              # lista de diccionarios
    
    end = time.time()
    print(f"Tiempo de ejecución con concurrencia:{end-start:.4f} segundos")

    plot_dataframe_data_threading(dataframes=results)

## MAIN CODE
try:
    # Sin Concurrencia
    # url = "https://companies-market-cap-copy.vercel.app/index.html" # Revenue
    # url_earnings = "https://companies-market-cap-copy.vercel.app/earnings.html" # Earnings
    
    # get_data_from_url(url)
    # get_data_from_url(url_earnings)

    # Con Concurrencia
    
    urls_list = [
        {'url': 'https://companies-market-cap-copy.vercel.app/index.html'},
        {'url': 'https://companies-market-cap-copy.vercel.app/earnings.html'}
    ]
    
    concurrence_execution_get_data_from_url(urls=urls_list)
    
    
# Timeout del requests.get se cumple y el servidor lo supera
except requests.exceptions.Timeout:
    print("Time out")
