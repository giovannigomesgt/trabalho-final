steps = [{
    'Id': 's-37IX900HV63MH',
    'Name': 'Processa dados do Gov - Socios',
    'Config': {'Jar': 'command-runner.jar',
    'Properties': {},
    'Args': ['spark-submit', 's3://notebooks-256240406578/sparkcode/etlgov/Socios.py']},
    'ActionOnFailure': 'CONTINUE',
    'Status': {'State': 'PENDING', 'StateChangeReason': {},
    'Timeline': {'CreationDateTime':(2023, 3, 8, 18, 49, 25, 510000)}}}
    ]

print(type(steps))
