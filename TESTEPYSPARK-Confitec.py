#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importanto as bibliotecas
import pyarrow as pa
import pandas as pd
from pyarrow import feather
import numpy as np
import pyarrow.parquet


# In[2]:


#Abra o arquivo parquet usando a função parquet_dataset:
dataset = pa.parquet.ParquetDataset('C:\\Users\\maced\\Desktop\\Confitec\\OriginaisNetflix - Python.parquet')


# In[3]:


#Ler o dataset
table = dataset.read()


# In[4]:


#Converta a tabela PyArrow para um DataFrame do pandas:
df = table.to_pandas()


# In[5]:


# A partir deste ponto, você pode manipular os dados no DataFrame df de acordo com suas necessidades.
# Salvar as mudanças no DataFrame em um novo arquivo parquet:
table = pa.Table.from_pandas(df)


# In[6]:


with pa.parquet.ParquetWriter('C:\\Users\\maced\\Desktop\\Confitec\\OriginaisNetflix - Python.parquet', table.schema) as writer:
    writer.write_table(table)


# In[7]:


# Para transformar os campos "Premiere" e "dt_inclusao" de string para datetime em um DataFrame do pandas, 
# você pode usar a função pd.to_datetime(). 
# Essa função converte uma série de valores em formato de data/hora para o tipo datetime.

df['Premiere'] = pd.to_datetime(df['Premiere'])


# In[8]:


#  Assumindo que você tenha um DataFrame chamado df com as colunas "Premiere" e "dt_inclusao" como strings, 
#  você pode converter esses campos para datetime da seguinte maneira:

# Convertendo a coluna "dt_inclusao" para datetime
df['dt_inclusao'] = pd.to_datetime(df['dt_inclusao'])

#Agora, as colunas "Premiere" e "dt_inclusao" do DataFrame df estão no formato datetime. 


# In[9]:


print(df.keys())


# In[10]:


# Para ordenar os dados por ativos e gênero de forma decrescente, primeiro você precisa converter a coluna "ativos" em um 
# tipo numérico, onde 1 representa ativo e 0 representa inativo. Em seguida, você pode ordenar o DataFrame com a 
# função sort_values() do pandas.

# Assumindo que você tenha um DataFrame chamado df com as colunas "ativos" e "gênero", 
#você pode ordenar os dados da seguinte maneira:

# Converter a coluna "ativos" para numérico
df['Active'] = pd.to_numeric(df['Active'])


# In[11]:


# Ordenar o DataFrame por ativos e gênero de forma decrescente
df = df.sort_values(['Active', 'Genre'], ascending=[False, False])


# In[12]:


#Neste exemplo, a função pd.to_numeric() é usada para converter a coluna "ativos" de uma string para um tipo numérico. 
#Em seguida, a função sort_values() é usada para ordenar o DataFrame em ordem decrescente por ativos e gênero. 
#A opção ascending=[False, False] é usada para ordenar a coluna "ativos" em ordem decrescente 
#(com os valores 1 aparecendo primeiro) e a coluna "gênero" em ordem alfabética decrescente.
# Com este código, o DataFrame df estará ordenado por ativos e gênero de forma decrescente, 
# com os ativos com valor 1 aparecendo primeiro.


# In[13]:


# Para remover linhas duplicadas e trocar o resultado das linhas que tiverem a coluna "Seasons" de "TBA" 
# para "a ser anunciado", você pode usar as funções drop_duplicates() e replace() do pandas.

# Remover linhas duplicadas
df = df.drop_duplicates()

# Substituir "TBA" por "a ser anunciado" na coluna "Seasons"
df['Seasons'] = df['Seasons'].replace('TBA', 'a ser anunciado')

# In[14]:

#Neste exemplo, a função drop_duplicates() é usada para remover linhas duplicadas do DataFrame df.
#Em seguida, a função replace() é usada para substituir todos os valores "TBA" na coluna "Seasons"por "a ser anunciado".
#Desta forma o DataFrame df terá as linhas duplicadas removidas e os valores "TBA" na coluna "Seasons" 
#substituídos por "a ser anunciado".

df.write.format('csv').option('header','true').mode("append").save("s3://arn:aws:s3:sa-east-1:227804544926:accesspoint/teste-confitec/teste/")

#https://testespark-confitec.s3.sa-east-1.amazonaws.com/1.csv

