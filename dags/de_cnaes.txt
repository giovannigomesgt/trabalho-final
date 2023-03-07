from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import os
import requests
from zipfile import ZipFile
import re
from tqdm import tqdm
from bs4 import BeautifulSoup
#from time import sleep
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from os import walk
from airflow.models import Variable
import boto3
from pytz import timezone


###
aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')
client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def getLinks():
    url = 'http://200.152.38.155/CNPJ/'
    page = requests.get(url)   
    data = page.text
    soup = BeautifulSoup(data)
    soup1 = BeautifulSoup(page.content, 'html.parser')
    table = soup1.find('table')

    links = []
    for link in soup.find_all('a'):
        if str(link.get('href')).endswith('.zip'): 
            cam = link.get('href')
            if not cam.startswith('http'):
                print(url+cam)
                links.append(url+cam)
            else:
                print(cam)
    return links
   
def cnaes(task_instance):
    links = task_instance.xcom_pull(task_ids='Get_Links')
    filescnaes = []
    for lista in links:
        if 'cnaes' in lista.lower():
            filescnaes.append(lista)
    return filescnaes

def versioning(task_instance):
    now = datetime.now()
    file = task_instance.xcom_pull(task_ids='Find_Cnaes')
    objectname = file[0].split('/')[-1].replace('zip','CSV')

    bucket_name = 'pottencial-datalake-dev-raw'
    object_name = f"dados_publicos_cnpj/Cnaes/{objectname}"
    try:
        # Obtém informações do objeto
        response = client.head_object(Bucket=bucket_name, Key=object_name)
            # Imprime a data da última modificação
        last_modified = response['LastModified'].replace(tzinfo=timezone('UTC'))
    except:
        hoje = now.replace(tzinfo=timezone('UTC'))
        # Subtraindo um dia da data:
        um_dia = timedelta(days=30)
        last_modified = hoje - um_dia
        print('Arquivo não encontrado')

    url = 'https://dadosabertos.rfb.gov.br/CNPJ/'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table = soup.find('table')
    versao = []
    for row in table.find_all('tr'):
        cells = row.find_all('td')   
        if cells:
            if '.zip' in cells[1].text:
                versao.append({
                    'file' : cells[1].text.replace(' ',''),
                    'version' : cells[2].text.replace('  ','')
                    })
    for i in versao:
        if file[0].split('/')[-1] in i['file']:
            date_object = datetime.strptime(i['version'], "%Y-%m-%d %H:%M")
            date_object = date_object.replace(tzinfo=timezone('UTC'))

            if date_object > last_modified:
                print('*'*100)
                print(f'Data no S3 {last_modified}')
                print(f'Ultima publicação no Gov {date_object}')
                print('*'*100)
                return 'Downloading'
            else:
                print('*'*100)
                print(f'Data no S3 {last_modified}')
                print(f'Ultima publicação no Gov {date_object}')
                print('*'*100)
                return 'Version_OK'
    
def download(task_instance):
    file = task_instance.xcom_pull(task_ids='Find_Cnaes')
    for item in file: # INICIA O PROCESSO DE DOWNLOAD
        # URL do arquivo que deseja baixar
        url = item
        file_name = url.split('/')[-1]
        folder = file_name.replace('.zip','')
        endereco = re.sub(u'[0123456789]', '',folder)
        os.makedirs(endereco, exist_ok=True)
        checkfile = os.listdir(endereco)
        if folder+'.zip' in checkfile or folder+'.CSV' in checkfile:
            print('Arquivo já eixte')
            pass
        else:
            response = requests.head(url)
            file_size = int(response.headers.get("Content-Length"))
            # Envia uma solicitação GET para baixar o arquivo
            response = requests.get(url, stream=True)
            print('*'*100)
            print("Downloading %s" % file_name)
            print('*'*100)
            # Cria um objeto tqdm para mostrar a barra de progresso
            progress_bar = tqdm(total=file_size, unit="B", unit_scale=True)
            # Escreve o conteúdo do arquivo em um arquivo local
            with open(f'{endereco}/{file_name}', "wb") as f:
            #    open(file_name, "wb") as f:
            #    open(f'./{endereco}/{file_name} as f:', 
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:  # filtra os pacotes vazios
                        f.write(chunk)
                        progress_bar.update(len(chunk))

            # Fecha o objeto tqdm
            progress_bar.close()
            print('*'*100)
            print('Download Concluido!')
            print('*'*100)   
            #extrair_arquivo(f'{endereco}/{file_name}')
    print('*'*100)
    print("Downloading")
    print('*'*100)
    return f'{endereco}/{file_name}'

def skip():
    print('*'*50)
    print('A versão do arquivo está ok')
    print('*'*50)
    return 'fim'

def extract(task_instance):
    # RECEBE O NOME DA PASTA
    folder = task_instance.xcom_pull(task_ids='Find_Cnaes')[0].split('/')[-1].replace('.zip','')
    #nomedoarquivo = folder.split('/')[-1].replace('.zip','')
    folder = re.sub(u'[0123456789]', '',folder)
    print('*'*150)
    print('Iniciando Extração')
    # VERIFICA A EXISTÊNCIA DA PASTA
    print('*'*150)
    if os.path.exists(folder):
        checkfile = os.listdir(folder)
        for file in checkfile:
            if '.zip' in file:
                filename = f'{folder}/{file}'
                print(filename)
                # Abre o arquivo ZIP
                with ZipFile(filename, "r") as z:
                    # Imprime o nome de cada arquivo dentro do arquivo ZIP
                    for compressed in z.namelist():
                        compressedfile = compressed
                    z.extractall(folder)
                    print(f'{filename} extraído')
                os.remove(filename)
                print(f'{filename} excluído')
                nomedoarquivo = filename.split('/')[-1].replace('.zip','')
            #print(f'caminho_do_arquivo {folder}')
            #print(f'Caminho do novo arquivo {nomedoarquivo}')     
                try:
                    # Renomeando Arquivo
                    caminho_do_arquivo = f'{folder}/{compressedfile}'
                    novo_nome = f'{folder}/{nomedoarquivo}.CSV'
                    os.rename(caminho_do_arquivo, novo_nome)
                    print(f'Arquivo {compressedfile} renomeado para {nomedoarquivo}.CSV')
                except:
                    print('arquivo já Existe')
    else:
        print('*'*50)
        print('Pasta Não encontrada')
        print('*'*50)
    print('*'*50)
    return 'Upload_to_S3'

def uploadS3(task_instance):
    # RECEBE O NOME DA PASTA
    file = task_instance.xcom_pull(task_ids='Find_Cnaes')[0].split('/')[-1].replace('.zip','')
    file = re.sub(u'[0123456789]', '',file)
    # VERIFICA A EXISTÊNCIA DA PASTA
    if os.path.exists(file):
        checkfile = os.listdir(file)
        for diretorio, subpasta, arquivos in walk(file):
            for arquivo in arquivos:
                print('*'*150)
                print(f'ARQUIVO ATUAL: {arquivo}')
                pastas3 = diretorio.split('\\')[0].replace('.','').replace('/','')
                pathlocal = f'{diretorio}/{arquivo}'
                paths3 = f'dados_publicos_cnpj/{pastas3}/{arquivo}'
                #print(pathlocal)
                print(f"Enviando {pathlocal} para o bucket 'pottencial-datalake-dev-raw' endereço da pasta: {paths3}")
                client.upload_file(pathlocal, 'pottencial-datalake-dev-raw', paths3)
                print(f'Arquivo {arquivo} Enviado!')
                print(f'Excluindo ./{pathlocal}')
                os.remove(f'./{pathlocal}')
                print('*'*150)
    else:
        pass
    checkfile = os.listdir(file)
    print('*'*150)
    print('Itens na pasta:')
    for files in checkfile: # LISTA OS ARQUIVOS NA PASTA
        print(f'{files}')
        #os.remove(files)
    print('*'*150)
 
triggerdag = TriggerDagRunOperator(
    task_id="de_motivos",
    trigger_dag_id="de_motivos")

with DAG('de_cnaes', start_date=datetime(2022,12,16),
    schedule_interval='0 0 * * 2-6', catchup= False, tags=['TREINAMENTO','GOV']) as dag:
    # Task de Execução de Script Python
    fim =  DummyOperator(task_id = "fim", trigger_rule=TriggerRule.NONE_FAILED)
        
    taskgetLinks = PythonOperator(
        task_id = 'Get_Links',
        python_callable = getLinks
    )

    taskdownloadcnaes = PythonOperator(
        task_id = 'Find_Cnaes',
        python_callable = cnaes
    )

    taskversioning = BranchPythonOperator(
        task_id = 'Versioning',
        python_callable = versioning
    )

    taskdownload = PythonOperator(
        task_id = 'Downloading',
        python_callable = download
    )

    taskskip = BranchPythonOperator(
    task_id = 'Version_OK',
    python_callable = skip
    )


    taskextract = BranchPythonOperator(
        task_id='Extracting',
        python_callable=extract,
        #trigger_rule = TriggerRule.ALL_DONE,
        trigger_rule = TriggerRule.NONE_FAILED
    )

    tasks3 = PythonOperator(
        task_id='Upload_to_S3',
        python_callable=uploadS3,
        trigger_rule = TriggerRule.ALL_SUCCESS
    )

    #download

    # Crie uma tarefa do tipo "Trigger DAG" que trigga a DAG 'target_dag'
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_target_dag',
        trigger_dag_id='de_motivos'
    )


taskgetLinks >> taskdownloadcnaes >> taskversioning >> [taskdownload, taskskip] >> taskextract >> tasks3 >> fim >> trigger_task





    
# def motivos(task_instance):
#     arquivos = task_instance.xcom_pull(task_ids='Get_Links')
#     for lista in arquivos:
#         if 'motivos' in lista.lower():
#             print(lista)
#     sleep(5)
#     return 'Downloading_municipios'

# def municipios(task_instance):
#     arquivos = task_instance.xcom_pull(task_ids='Get_Links')
#     for lista in arquivos:
#         if 'municipios' in lista.lower():
#             print(lista)
#     sleep(15)
#     return 'Downloading_naturezas'
    
# def naturezas(task_instance):
#     arquivos = task_instance.xcom_pull(task_ids='Get_Links')
#     for lista in arquivos:
#         if 'naturezas' in lista.lower():
#             print(lista)
#     sleep(5)
#     return 'Downloading_paises'

# def paises(task_instance):
#     arquivos = task_instance.xcom_pull(task_ids='Get_Links')
#     for lista in arquivos:
#         if 'paises' in lista.lower():
#             print(lista)
#     sleep(5)
#     return 'Downloading_qualificacoes'

# def qualificacoes(task_instance):
#     arquivos = task_instance.xcom_pull(task_ids='Get_Links')
#     for lista in arquivos:
#         if 'qualificacoes' in lista.lower():
#             print(lista)
