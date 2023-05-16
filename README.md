# teste-confitec

Avaliação Confitec
-- PySpark

Ler o arquivo parquet e manipular os dados de acordo com os próximos passos utilizando python com pysparrow:

OriginaisNetflix.parquet - Python

1. Transformar os campos "Premiere" e "dt_inclusao" de string para datetime.

2. Ordenar os dados por ativos e gênero de forma decrescente, 0 = inativo e 1 = ativo, todos
com número 1 devem aparecer primeiro.

3. Remover linhas duplicadas e trocar o resultado das linhas que tiverem a coluna "Seasons"
de "TBA" para "a ser anunciado".

4. Criar uma coluna nova chamada "Data de Alteração" e dentro dela um timestamp.

5. Trocar os nomes das colunas de inglês para português, exemplo: "Title" para "Título"
(com acentuação).

6. Testar e verificar se existe algum erro de processamento do spark e identificar onde
pode ter ocorrido o erro.

7. Criar apenas 1 .csv com as seguintes colunas que foram nomeadas anteriormente "Title
Genre, Seasons, Premiere, Language, Active, Status, dt_inclusao, Data de Alteração" as
colunas devem estar em português com header e separadas por ";".

8. Inserir esse .csv dentro de um bucket do AWS s3

9. Subir o código no github com o nome TESTEPYSPARK-Confitec



#### Avaliar qual linguagem é melhor para resolver o problem  ####

Algumas linguagens de programação que suportam a leitura e escrita de arquivos parquet incluem:

Python: o Python tem suporte nativo para trabalhar com arquivos parquet por meio das bibliotecas pandas e PyArrow. A biblioteca pandas fornece uma interface de alto nível para trabalhar com dados em arquivos parquet, enquanto PyArrow é uma biblioteca de baixo nível que oferece recursos mais avançados para trabalhar com arquivos parquet.

R: a linguagem R também tem suporte para arquivos parquet por meio das bibliotecas readr e Arrow. A biblioteca readr é uma das mais populares para leitura de dados em arquivos parquet no R, enquanto a biblioteca Arrow é uma opção de baixo nível para manipulação de dados em arquivos parquet.

Java: a biblioteca Apache Parquet, que foi originalmente desenvolvida para a linguagem Java, é uma das bibliotecas mais populares para trabalhar com arquivos parquet. A biblioteca fornece uma API Java para leitura e escrita de arquivos parquet.

Scala: como o Scala é executado na JVM (Java Virtual Machine), ele também pode usar a biblioteca Apache Parquet para trabalhar com arquivos parquet.

C++: a biblioteca Apache Arrow fornece suporte para manipulação de dados em arquivos parquet em C++.

## Irei utilizar python com a biblioteca PyArrow ##
