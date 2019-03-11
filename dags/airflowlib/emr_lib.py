import boto3, json, pprint, requests, textwrap, time, logging, requests
from datetime import datetime


def get_region():
    '''
    fetches the aws metadata for the region that the ec2 instance is running
     - must be run inside the instance because the IP is an APIPA
    '''
    r = requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document")
    response = r.json()
    return response.get("region")


def client(region_name):
    '''
    creates an instance of the emr client to be used
     - target resources must be passed when using client object
    '''
    global emr
    emr = boto3.client('emr', region_name=region_name)


def get_security_group_id(group_name, region_name):
    '''
    used to get the security groups for EMR master and slave
    '''
    ec2 = boto3.client('ec2', region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']


def create_cluster(region_name, cluster_name='Airflow-' + str(datetime.now()), release_label='emr-5.21.0', master_instance_type='m4.xlarge', num_core_nodes=2, core_node_instance_type='m4.xlarge'):
    '''
    creates a standard EMR cluster with the emr client object and default roles
    '''
    emr_master_security_group_id = get_security_group_id('AirflowEMRMasterSG', region_name=region_name)
    emr_slave_security_group_id = get_security_group_id('AirflowEMRSlaveSG', region_name=region_name)
    cluster_response = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName': 'aws_key',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            {'Name': 'hadoop'},
            {'Name': 'spark'},
            {'Name': 'hive'},
            {'Name': 'livy'},
            {'Name': 'zeppelin'}
        ]
    )
    return cluster_response['JobFlowId']


def get_cluster_dns(cluster_id):
    '''
    describe_cluster provides cluster details: status, hardware,
    software config, VPC settings, etc.
     - use request to return dns name
    '''
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def wait_for_cluster_creation(cluster_id):
    '''
    returns an object that will wait until describe_cluster returns
    a successful state
    '''
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


# Creates an interactive scala spark session.
# Python(kind=pyspark), R(kind=sparkr) and SQL(kind=sql) spark sessions can also be created by changing the value of kind.
def create_spark_session(master_dns, kind='spark'):
    # Livy server runs on port 8998 for interactive session
    host = 'http://' + master_dns + ':8998'
    data = {'kind': kind}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.headers


def wait_for_idle_session(master_dns, response_headers):
    # waits for the session state to be idle or ready for submissions
    status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location']
    while status != 'idle':
        time.sleep(3)
        status_response = requests.get(session_url, headers=response_headers)
        status = status_response.json()['state']
        logging.info('Session status: ' + status)
    return session_url


def kill_spark_session(session_url):
    requests.delete(session_url, headers={'Content-Type': 'application/json'})


# reads the file and submits the code as a JSON command the the livy server
def submit_statement(session_url, statement_path):
    statements_url = session_url + '/statements'
    with open(statement_path, 'r') as f:
        code = f.read()
    data = {'code': code}
    response = requests.post(statements_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    logging.info(response.json)
    return response


# checks the statement status every 10 seconds until it is available
def track_statement_progress(master_dns, response_headers):
    statement_status = ''
    host = 'http://' + master_dns + ':8998'
    session_url = host + response_headers['location']
    # poll the status of the submitted code
    while statement_status != 'available':
        # If a statement takes longer than a few milliseconds to execute,
        # livy returns early and provides a statement URL that
        # can be polled until it is complete:
        statement_url = host + response_headers['location']
        statement_response = requests.get(statement_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        logging.info('Statement status: ' + statement_status)

        # logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
        	logging.info(line)

        if 'progress' in statement_response.json():
        	logging.info('Progress: ' + str(statement_response.json()['progress']))
        time.sleep(10)
    final_statement_status = statement_response.json()['output']['status']
    if final_statement_status == 'error':
    	logging.info('Statement exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            logging.info(trace)
        raise ValueError('Final Statement Status: ' + final_statement_status)
    logging.info('Final Statement Status: ' + final_statement_status)


def get_public_ip(cluster_id):
    instances = emr.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'])
    return instances['Instances'][0]['PublicIpAddress']
