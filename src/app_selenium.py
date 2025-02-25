import os
import re
import sqlite3
import time
import queue
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

options = webdriver.ChromeOptions()

# driver = webdriver.Chrome()
# print(driver.capabilities['browserVersion'])  # Verifica la versión de Chrome


driver = webdriver.Chrome(
    service = Service(ChromeDriverManager().install()),
    options = options
)



URL = "https://companies-market-cap-copy.vercel.app/index.html"
driver.get(URL)
table_data = driver.find_elements(By.TAG_NAME, "table")
def get_data_table(table_data):
    lista = []
    for row in table_data:
        lista.append(row.text)
    return lista

lista = get_data_table(table_data)

lista_2=lista[0].split(f"\n") # separar en salto de linea y convertir la lista en otra lista con cada row
#print(lista_2)
# Quitar la letra B para que cuadren las filas y columnas
def delete_B(lista):
    for i in range(len(lista)): 
        lista[i] = lista[i].replace(' B', '')
    return lista
lista_2 = delete_B(lista_2)

resultado = ' '.join(lista_2).split()   # convertir cada elemento de la lista en un string independiente
resultado.append(None)                   # nos daba error en change en el último elemento
#print(resultado)
def arrange_dataframe(rows):
    data = []
    for i in range(0, len(rows), 3):   # separar cada fila en la columna que corresponde

        year = rows[i]
        revenue = rows[i+1]
        change = rows[i+2]
        data.append([year, revenue, change])
    return data

data = arrange_dataframe(rows = resultado)

df = pd.DataFrame(data=data)
df.columns = df.iloc[0] # asignar la primera fila como columnas
df = df.drop(0).reset_index(drop=True)  # eliminar la primera fila
#df = df[1:].reset_index(drop=True)
df = df.dropna() # elimino el último elemento que tiene valores none
# limpiar datos
df = df.map(lambda x: re.sub(r"[^0-9.-]","",x))
# convertir a float
df = df.map(lambda x: float(x))
df.info()