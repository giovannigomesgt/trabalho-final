# trabalho-final

O que deve ser feito?
- Criar um Bucket S3 - OK
- Criar um Cluster EMR (criar manualmente e enviar steps depois ou criar na DAG) - OK
- Criar um cluster Kubernetes - OK
- Deployar o Airflow no cluster Kubernetes - OK
- Criar um usuário chamado `airflow-user` com permissão de administrador da conta - OK
- Escolher um dataset (livre escolha) - OK
- Subir o dataset em um bucket S3 - OK
- Pensar e implementar construção de indicadores e análises sobre esse dataset (produzir 2 ou mais indicadores no mesmo grão) - OK
- Escrever no S3 arquivos parquet com as tabelas de indicadores produzidos - OK
- Escrever outro job spark que lê todos os indicadores construídos, junta tudo em uma única tabela - OK
- Escrever a tabela final de indicadores no S3 - OK

- Subir um notebook dentro do cluster EMR - PENDENTE
- Ler a tabela final de indicadores e dar um `.show()` - PENDENTE
- Todo o processamento de dados orquestrado pelo AIRFLOW no K8s - PENDENTE

Entregáveis
Link do repositório git (Github, Gitlab) com os códigos - ok
Print do Bucket S3 criado - ok
Print do Cluster EMR criado - ok 
Print da DAG no Airflow concluída (visão do GRID)
Print do Notebook mostrando a tabela final com `.show()`
Para envio só serão aceitos arquivos com extensão 'jpg', 'jpeg' ou 'png'.

 

DEADLINE - 16 de novembro de 2022
