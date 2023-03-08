import datetime
step_states = {
    'Cluster': {
        'Id': 'string',
        'Name': 'string',
        'Status': {
            'State': 'STARTING | BOOTSTRAPPING | RUNNING | WAITING | TERMINATING | TERMINATED | TERMINATED_WITH_ERRORS',
            'StateChangeReason': {
                'Code': 'INTERNAL_ERROR | VALIDATION_ERROR | INSTANCE_FAILURE | INSTANCE_FLEET_TIMEOUT | BOOTSTRAP_FAILURE | USER_REQUEST | STEP_FAILURE | ALL_STEPS_COMPLETED',
                'Message': 'string'
            },
            'Timeline': {
                'CreationDateTime': (2015, 1, 1),
                'ReadyDateTime': (2015, 1, 1),
                'EndDateTime': (2015, 1, 1)
            }
        },
        'Ec2InstanceAttributes': {
            'Ec2KeyName': 'string',
            'Ec2SubnetId': 'string',
            'RequestedEc2SubnetIds': [
                'string',
            ],
            'Ec2AvailabilityZone': 'string',
            'RequestedEc2AvailabilityZones': [
                'string',
            ],
            'IamInstanceProfile': 'string',
            'EmrManagedMasterSecurityGroup': 'string',
            'EmrManagedSlaveSecurityGroup': 'string',
            'ServiceAccessSecurityGroup': 'string',
            'AdditionalMasterSecurityGroups': [
                'string',
            ],
            'AdditionalSlaveSecurityGroups': [
                'string',
            ]
        },
        'InstanceCollectionType': 'INSTANCE_FLEET | INSTANCE_GROUP',
        'LogUri': 'string',
        'LogEncryptionKmsKeyId': 'string',
        'RequestedAmiVersion': 'string',
        'RunningAmiVersion': 'string',
        'ReleaseLabel': 'string',
        'AutoTerminate': True,
        'TerminationProtected': True,
        'VisibleToAllUsers': True,
        'Applications': [
            {
                'Name': 'string',
                'Version': 'string',
                'Args': [
                    'string',
                ],
                'AdditionalInfo': {
                    'string': 'string'
                }
            },
        ],
        'Tags': [
            {
                'Key': 'string',
                'Value': 'string'
            },
        ],
        'ServiceRole': 'string',
        'NormalizedInstanceHours': 123,
        'MasterPublicDnsName': 'string',
        'Configurations': [
            {
                'Classification': 'string',
                'Configurations': {'... recursive ...'},
                'Properties': {
                    'string': 'string'
                }
            },
        ],
        'SecurityConfiguration': 'string',
        'AutoScalingRole': 'string',
        'ScaleDownBehavior': 'TERMINATE_AT_INSTANCE_HOUR | TERMINATE_AT_TASK_COMPLETION',
        'CustomAmiId': 'string',
        'EbsRootVolumeSize': 123,
        'RepoUpgradeOnBoot': 'SECURITY | NONE',
        'KerberosAttributes': {
            'Realm': 'string',
            'KdcAdminPassword': 'string',
            'CrossRealmTrustPrincipalPassword': 'string',
            'ADDomainJoinUser': 'string',
            'ADDomainJoinPassword': 'string'
        },
        'ClusterArn': 'string',
        'OutpostArn': 'string',
        'StepConcurrencyLevel': 123,
        'PlacementGroups': [
            {
                'InstanceRole': 'MASTER | CORE | TASK',
                'PlacementStrategy': 'SPREAD | PARTITION | CLUSTER | NONE'
            },
        ],
        'OSReleaseLabel': 'string'
    }
}


# while True:
#     step_states = [step['Status']['State'] for step in step_states['Cluster']]

#     if all(state in ['COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED'] for state in step_states):
#         print("Todos os steps foram concluídos.")
#         break


print(step_states['Cluster']['Status']['State'])