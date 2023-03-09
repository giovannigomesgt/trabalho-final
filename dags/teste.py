from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3
import time

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

client = boto3.client(
    'emr', region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': "Giovanni",
    "depends_on_past": False,
    'start_date': datetime(2022, 11, 10)  # YYYY, MM, DD
}


@dag(
    default_args=default_args,
    schedule_interval="@once",
    description="Executa um job Spark no EMR",
    catchup=False,
    tags=['Spark', 'EMR', 'Processamento', 'Gov']
)
def testeEmr():

    inicio = DummyOperator(task_id='inicio')

    @task
    def criando_cluster_emr():
        cluster_id = client.run_job_flow(
            Name='Processamento_Dados_Gov',
            LogUri='s3://256240406578-datalake/emr-logs/',
            ReleaseLabel='emr-6.8.0',
            Applications=[{'Name': 'Spark'}],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.2xlarge',
                        'InstanceCount': 2,
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-00709a3ade46a24c7'
            },
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            AutoTerminationPolicy={
                'IdleTimeout': 60
            },
            Tags=[
                {"Key": "BusinessDepartment", "Value": "Pottencial"}, {
                    "Key": "CostCenter", "Value": "N/A"}, {"Key": "environment", "Value": "Prd"},
                {"Key": "ProjectName", "Value": "Data Lake"}, {
                    "Key": "TechnicalTeam", "Value": "Arquitetura"}
            ],
            StepConcurrencyLevel=3
        )
        return cluster_id["JobFlowId"]

    @task
    def aguardando_criacao_cluster(cid: str):
        waiter = client.get_waiter('cluster_running')

        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )
        return True

    @task
    def enviando_dados_para_processamento(cid: str):
        newstep = client.add_job_flow_steps(
            JobFlowId=cid,
            Steps=[
                {
                    'Name': 'Processa dados do Gov - Cnaes',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Cnaes.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Empresas',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Empresas.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Estabelecimentos',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Estabelecimentos.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Motivos',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Motivos.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Municipios',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Municipios.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Naturezas',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Naturezas.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Paises',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Paises.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Qualificacoes',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Qualificacoes.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Simples',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Simples.py'
                                     ]
                    }
                },
                {
                    'Name': 'Processa dados do Gov - Socios',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     's3://notebooks-256240406578/sparkcode/etlgov/Socios.py'
                                     ]
                    }
                }
            ]
        )
        return newstep['StepIds'][-1]

    @task
    def aguardando_execucao_do_job(cid: str, stepId: str):
        step_ids = []
        stepId = stepId
        state = ['COMPLETED', 'CANCELLED', 'FAILED','INTERRUPTED']

        steps_response = client.list_steps(ClusterId=cid)
        for step in steps_response['Steps']:  # Captura todos os Ids dos steps
            step_ids.append(step['Id'])

        while True:
            if len(step_ids) > 0:
                for step in step_ids:
                    response = client.describe_step(
                        ClusterId=cid,
                        StepId=step
                    )
                    if response['Step']['Status']['State'] in state:
                        print(response['Step']['Name'])
                        print(response['Step']['Status']['State'])
                        step_ids.remove(step)
            else:
                break


             # print('Cluster ID: {}\nStep ID: {}\nStatus: {}\n'.format(cluster_id, step['Id'], step['Status']['State']))

        # response = client.describe_step(
        #     ClusterId='string',
        #     StepId='string'
        # )
        # response['Step']['Status']['State] --> 'PENDING'|'CANCEL_PENDING'|'RUNNING'|'COMPLETED'|'CANCELLED'|'FAILED'|'INTERRUPTED',

    processoSucess = DummyOperator(task_id="processamento_concluido")

    @task
    def terminando_cluster_emr(cid: str):
        res = client.terminate_job_flows(
            JobFlowIds=[cid]
        )

    fim = DummyOperator(task_id="fim")

    # Orquestração

    cluster = criando_cluster_emr()
    inicio >> cluster

    esperacluster = aguardando_criacao_cluster(cluster)

    indicadores = enviando_dados_para_processamento(cluster)

    esperacluster >> indicadores

    wait_step = aguardando_execucao_do_job(cluster, indicadores)

    terminacluster = terminando_cluster_emr(cluster)
    wait_step >> processoSucess >> terminacluster >> fim


execucao = testeEmr()
