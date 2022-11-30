import json
import boto3
import traceback
import os
import time

rds = boto3.client('rds')

# failover 开始事件
FAILOVER_START_EVENT_ID = 'RDS-EVENT-0073'
# failover 结束事件
FAILOVER_END_ENENT_ID = 'RDS-EVENT-0071'
# 数据库实例创建完成事件
FAILOVER_DB_CREATED_EVENT_ID = 'RDS-EVENT-0092'

# 为创建的新实例添加当天日期结尾（一天只能处理一个集群一次failover，理论上failover不会经常发生）
def get_date_flag():
    timestamp = int(time.time())
    t = time.strftime('%Y%m%d',time.localtime(timestamp))
    return t

# 新实例标记    
def get_db_new_flag():
    return 'failover' + get_date_flag()

# 获取集群下的所有实例
def get_db_cluster_members(db_cluster):
    response = rds.describe_db_clusters(
        DBClusterIdentifier=db_cluster
    )
    
    return response['DBClusters'][0]['DBClusterMembers'] 
    
# 检查是否需要创建新实例，如果已经有带有标记的实例就不创建
def check_need_created_writer(db_cluster):
    db_instances = get_db_cluster_members(db_cluster)
    db_flag = get_db_new_flag()
    for db in db_instances:
        if db_flag in db['DBInstanceIdentifier']:
            print('alreay created failover db instance')
            return False
    
    return True

# 根据实例获取集群标志
def get_cluster_identifier(sourceDB):
    rds_response = rds.describe_db_instances(
        DBInstanceIdentifier=sourceDB
        )
        
    if rds_response:
        source_Instance_configs = rds_response['DBInstances'][0]
    
    return source_Instance_configs['DBClusterIdentifier']

# 创建数据库实例
def create_db(sourceDB, dbinstance_Identifier,db_cluster):
    if len(dbinstance_Identifier) > 63:
        dbinstance_Identifier = dbinstance_Identifier[len(dbinstance_Identifier) - 62:]
    print('create db with param ' + sourceDB + ' ' + dbinstance_Identifier + ' ' +  db_cluster)
    #Describe DB Instance
    rds_response = rds.describe_db_instances(
        DBInstanceIdentifier=sourceDB
        )
        
    if rds_response:
        source_Instance_configs = rds_response['DBInstances'][0]
    
    kwargs = {}
    kwargs['DBInstanceIdentifier'] = dbinstance_Identifier
    kwargs['DBClusterIdentifier'] = db_cluster
    if ('DBInstanceClass' in source_Instance_configs): kwargs['DBInstanceClass'] =  source_Instance_configs['DBInstanceClass']
    
    rds_cluster_Response = rds.describe_db_clusters(
        DBClusterIdentifier=db_cluster
        )
    rds_Cluster_Engine = rds_cluster_Response['DBClusters'][0]['Engine']
    kwargs['Engine'] = rds_Cluster_Engine
    #DBSecurityGroups are part of the EC2-Classic
    if ('MultiAZ' in source_Instance_configs):
        if (not source_Instance_configs.get('MultiAZ')):
            kwargs['AvailabilityZone'] = source_Instance_configs['AvailabilityZone']
    if ('DBSubnetGroup' in source_Instance_configs):
        kwargs['DBSubnetGroupName'] = source_Instance_configs.get('DBSubnetGroup').get('DBSubnetGroupName')
    if ('PreferredMaintenanceWindow' in source_Instance_configs): kwargs['PreferredMaintenanceWindow'] = source_Instance_configs.get('PreferredMaintenanceWindow')
    
    if ('DBParameterGroups' in source_Instance_configs): kwargs['DBParameterGroupName'] = source_Instance_configs['DBParameterGroups'][0]['DBParameterGroupName']

    
    
    if ('AutoMinorVersionUpgrade' in source_Instance_configs): kwargs['AutoMinorVersionUpgrade'] = source_Instance_configs.get('AutoMinorVersionUpgrade')
    if ('LicenseModel' in source_Instance_configs): kwargs['LicenseModel'] = source_Instance_configs.get('LicenseModel')
    if ('PubliclyAccessible' in source_Instance_configs): kwargs['PubliclyAccessible'] =  source_Instance_configs.get('PubliclyAccessible')
    if ('TagList' in source_Instance_configs): kwargs['Tags'] = source_Instance_configs.get('TagList')
    if ('MonitoringInterval' in source_Instance_configs):
        kwargs['MonitoringInterval'] = source_Instance_configs.get('MonitoringInterval')
        if ((source_Instance_configs.get('MonitoringInterval') > 0) and ('MonitoringRoleArn' in source_Instance_configs)):
            kwargs['MonitoringRoleArn'] = source_Instance_configs.get('MonitoringRoleArn')
    
    # if ('PromotionTier' in source_Instance_configs): kwargs['PromotionTier'] = source_Instance_configs.get('PromotionTier')
    if ('PerformanceInsightsEnabled' in source_Instance_configs): kwargs['EnablePerformanceInsights'] = source_Instance_configs.get('PerformanceInsightsEnabled')
    if ('PerformanceInsightsKMSKeyId' in source_Instance_configs): kwargs['PerformanceInsightsKMSKeyId'] = source_Instance_configs.get('PerformanceInsightsKMSKeyId')
    if ('PerformanceInsightsRetentionPeriod' in source_Instance_configs): kwargs['PerformanceInsightsRetentionPeriod'] = source_Instance_configs.get('PerformanceInsightsRetentionPeriod')
    
    # give hight level PromotionTier
    kwargs['PromotionTier'] = 0
    print(kwargs)
    #Create the Instance
    new_DBInstance = rds.create_db_instance(**kwargs)
    print("************* Restored Cluster:")
    print(new_DBInstance)
    print('Creating Instance: '+dbinstance_Identifier)

# 复制一个failover前的写实例作为读取器
def create_new_reader_from_failover(db_cluster):
    db_instances = get_db_cluster_members(db_cluster)
    db_flag = get_db_new_flag()
    source_db = None
    for db in db_instances:
        if db['IsClusterWriter'] == True:
            source_db = db['DBInstanceIdentifier']
            break
    
    if source_db is not None:
        create_db(source_db,source_db + '-' + db_flag,db_cluster)

# 检查failover是否成功
def check_failover_success(db_cluster):
    db_instances = get_db_cluster_members(db_cluster)
    db_flag = get_db_new_flag()
    for db in db_instances:
        if db_flag in db['DBInstanceIdentifier'] and db['IsClusterWriter'] == True:
            return True
    
    return False

def lambda_handler(event, context):
    print(event)
    # 1. 通过eventbridge 捕获failover事件
    # 2. 获取源集群信息，源写入实例信息
    # 3. 判断集群中是否已经包含和源写入节点一样配置的节点，没有则创建一个和源写入节点一样的配置的读取器，并且把failover 优先级设置为0
    # 4. 当新读取节点启动完成后，触发failover
    # TODO implement
    if 'EventID' not in event['detail']:
         return {
                'statusCode': 200,
                'body': json.dumps('do nothing!')
            }
        
    eventId = event['detail']['EventID']

    if eventId == FAILOVER_START_EVENT_ID:
        if check_need_created_writer(event['detail']['SourceIdentifier']):
            create_new_reader_from_failover(event['detail']['SourceIdentifier'])
        
        
    if eventId == FAILOVER_END_ENENT_ID:
        if check_failover_success(event['detail']['SourceIdentifier']):
            print('failover successed')
        else:
            print('failover fail')
    
    if eventId == FAILOVER_DB_CREATED_EVENT_ID:
        print('db created successed!!!')
        db_identifer = event['detail']['SourceIdentifier']
        db_flag = get_db_new_flag()
        if db_flag in db_identifer:
            db_cluster = get_cluster_identifier(db_identifer)
            print("************* Failover Cluster:" + db_cluster)
            response = rds.failover_db_cluster(
                DBClusterIdentifier=db_cluster,
                TargetDBInstanceIdentifier=db_identifer
            )
            print(response)
            
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
