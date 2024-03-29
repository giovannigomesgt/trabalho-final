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
            Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
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
                'IdleTimeout': 180
            },
            Tags=[
                {"Key": "BusinessDepartment", "Value": "Pottencial"}, {
                    "Key": "CostCenter", "Value": "N/A"}, {"Key": "environment", "Value": "Prd"},
                {"Key": "ProjectName", "Value": "Data Lake"}, {
                    "Key": "TechnicalTeam", "Value": "Arquitetura"}
            ],
            StepConcurrencyLevel=2
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
                # Cnaes
                {
                    'Name': 'Processa dados do Gov - Cnaes',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Cnaes.py'
                                     ]
                    }
                },
                # Motivos
                {
                    'Name': 'Processa dados do Gov - Motivos',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Motivos.py'
                                     ]
                    }
                },
                # Municipios
                {
                    'Name': 'Processa dados do Gov - Municipios',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Municipios.py'
                                     ]
                    }
                },
                # Naturezas
                {
                    'Name': 'Processa dados do Gov - Naturezas',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Naturezas.py'
                                     ]
                    }
                },
                # Paises
                {
                    'Name': 'Processa dados do Gov - Paises',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Paises.py'
                                     ]
                    }
                },
                # Qualificacoes
                {
                    'Name': 'Processa dados do Gov - Qualificacoes',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Qualificacoes.py'
                                     ]
                    }
                },
                # Empresas
                {
                    'Name': 'Processa dados do Gov - Empresas',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Empresas.py'
                                     ]
                    }
                },
                # Estabelecimentos
                {
                    'Name': 'Processa dados do Gov - Estabelecimentos',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Estabelecimentos.py'
                                     ]
                    }
                },
                # Simples
                {
                    'Name': 'Processa dados do Gov - Simples',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Simples.py'
                                     ]
                    }
                },
                # Socios
                {
                    'Name': 'Processa dados do Gov - Socios',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['spark-submit',
                                     '--deploy-mode', 'cluster',
                                     '--master', 'yarn',
                                     '--conf', 'spark.executor.cores=8',
                                     '--conf', 'spark.executor.memory=16g',
                                     '--conf', 'spark.executor.memoryOverhead=4g',
                                     '--conf', 'spark.executor.instances=1',
                                     '--conf', 'spark.driver.cores=2',
                                     '--conf', 'spark.driver.memory=8g',
                                     '--conf', 'spark.driver.memoryOverhead=1g',
                                     '--conf', 'spark.default.parallelism=48',
                                     '--conf', 'spark.driver.maxResultSize=5g',
                                     '--conf', 'spark.sql.execution.arrow.pyspark.enabled=true',
                                     "--conf", "spark.dynamicAllocation.enabled=false",
                                     's3://notebooks-256240406578/sparkcode/etlgov/Socios.py'
                                     ]
                    }
                }
                # Fim
            ]
        )
        return newstep['StepIds'][-1]

    @task
    def aguardando_execucao_do_job(cid: str, remover: str):
        remover = remover
        step_ids = []
        while True:
            # Obtém informações sobre o cluster
            response = client.describe_cluster(ClusterId=cid)
            # RUNNING or WAITING
            state = response['Cluster']['Status']['State']
            print('-' * 150)
            print(f'Situação do Cluster EMR {state}')

            if state == 'WAITING':
                # Captura todos os IDs dos steps
                steps_response = client.list_steps(ClusterId=cid)
                step_ids = [step['Id'] for step in steps_response['Steps']]
                # Remove IDs dos steps que foram concluídos ou cancelados
                step_ids = [step_id for step_id in step_ids
                            if client.describe_step(ClusterId=cid, StepId=step_id)['Step']['Status']['State'] not in ['COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED']]

                if not step_ids:  # Se não houver etapas pendentes, saia do loop
                    break

            elif state == 'RUNNING':
                # Captura todos os IDs dos steps
                steps_response = client.list_steps(ClusterId=cid)
                for step in steps_response['Steps']:
                    print(
                        f"Nome do Step: {step['Name']} - Status: {step['Status']['State']}")
                print('-' * 150)

            # Aguarda um tempo antes de verificar novamente o estado do cluster
            time.sleep(10)

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
