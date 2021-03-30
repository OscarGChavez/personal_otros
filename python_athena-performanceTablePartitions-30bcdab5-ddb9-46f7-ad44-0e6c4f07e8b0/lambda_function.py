import boto3
import time

TABLES = ['xxxxxxxxxxxxx', 'xxxxxxxxxxxxx']
ATHENA_CLIENT = boto3.client('athena')
RETRY_COUNT = 100

def query_status(query_execution_id):
    for i in range(1, 1 + RETRY_COUNT):
        query_status = ATHENA_CLIENT.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break
        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)

    else:
        ATHENA_CLIENT.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')
    return None


def execute_proess():
    config = {
        'OutputLocation': 's3://xxxxxxxxxxxxx-xxxxxxxxxxxxx-xxxxxxxxxxxxx/xxxxxxxxxxxxx/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    context = {'Database': 'performancetx'}
    for table_target in TABLES:
        print('xxxxxxxxxxxxx ' + table_target)
        sql = 'xxxxxxxxxxxxx REPAIR TABLE xxxxxxxxxxxxx.' + table_target
        response = ATHENA_CLIENT.start_query_execution(QueryString=sql,
                                                       QueryExecutionContext=context,
                                                       ResultConfiguration=config)
        query_execution_id = response['QueryExecutionId']
        # tiempo aproximado que demora en ejecuar esta query
        time.sleep(100)
        query_status(query_execution_id)
    return None


def lambda_handler(event, context):
    execute_proess()
