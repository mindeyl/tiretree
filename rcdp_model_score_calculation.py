import boto3
import sys
import os
import mysql.connector
import datetime
import logging
import json
import time
import psycopg2
import subprocess
import s3fs
import gzip
import requests
import pytz
import shutil

from math import ceil


global logger


def setup_logger():
	"""
	Sets up console logger with name 'logger'
	"""

	# Get logger and formatter
	logger = logging.getLogger('logger')
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	
	consoleHandler = logging.StreamHandler()
	consoleHandler.setFormatter(formatter)
	logger.addHandler(consoleHandler)
	logger.setLevel(logging.INFO)

	return logger


def getConfig():
	"""
	Receives following config from cmdline arguments:
	1. MySql Endpoint
	2. MySql Database
	3. MySql Username
	4. MySql Password
	5. MySql Port
	6. Segment Id
	7. Company Key
	8. Score Id
	8. Delta Table Name
	"""

	ArgCount = len(sys.argv)
	ExpectedAgCount = 11
	if ArgCount != ExpectedAgCount:
		error = 'Expected Argument Count: ' + str(ExpectedAgCount - 1) + '; Received Argument Count: {}'.format(ArgCount - 1)
		raise Exception(error)

	# Get logger
	logger = logging.getLogger('logger')
	logger.info('Getting MySql Database Credentials, modelId and CompanyKey from CommandLine')

	config = {}
	config['pyDirPath'] = sys.argv[0].rsplit('/', 1)[0] if '/' in sys.argv[0] else os.getcwd()
	config['host'] = sys.argv[1]
	config['database'] = sys.argv[2]
	config['user'] = sys.argv[3]
	config['password'] = sys.argv[4]
	config['port'] = sys.argv[5]
	config['modelId'] = sys.argv[6]
	config['companyKey'] = sys.argv[7]
	config['scoreId'] = sys.argv[8]
	config['mcrsDeltaTableName'] = sys.argv[9]
	config['scoreLocalDirPath'] = sys.argv[10]

	return config


def logConfig(config):

	logger.info('pyDirPath: %s', config['pyDirPath'])
	logger.info('host: %s', config['host'])
	logger.info('database: %s', config['database'])
	logger.info('user: %s', config['user'])
	logger.info('password: %s', '*' * (len(config['password'])-4) + config['password'][-4:])
	logger.info('port: %s', config['port'])
	logger.info('modelId: %s', config['modelId'])
	logger.info('companyKey: %s', config['companyKey'])
	logger.info('scoreId: %s', config['scoreId'])
	logger.info('mcrsDeltaTableName: %s', config['mcrsDeltaTableName'])
	logger.info('scoreLocalDirPath: %s', config['scoreLocalDirPath'])

	
def runMySqlQuery(query, config, returnResults = True):

	ConnCreatedFlag = False
	CursorCreatedFlag = False
	modelInfo = {}

	logger.info('Running MySql query: %s', query)

	try:
		mySqlConfig = {key: value for (key, value) in config.items() if key in ('host', 'database', 'user', 'password', 'port')}
		conn = mysql.connector.connect(**mySqlConfig); ConnCreatedFlag = True
		cursor = conn.cursor(); CursorCreatedFlag = True
		cursor.execute(query)

		if returnResults is True:
			results = []
			for row in cursor: results.append(row)
			return results

	finally:

		if CursorCreatedFlag is True:
			conn.commit()
			cursor.close()
		if ConnCreatedFlag is True: conn.close()
		


def getModelInfo(config):

	modelInfo = {}
	query = """
			SELECT MODEL_ID,
			       MODEL_NAME,
			       MODEL_DESC,
			       MODEL_CONFIG_DEF
			FROM RCDP_MODEL_CONFIG
			WHERE MODEL_TYPE = 'RFME'
			AND   COMPANY_KEY = """ + config['companyKey'] + """
			AND   MODEL_ID = '""" + config['modelId'] + """'
			AND   ACTIVE = 1
		"""

	results = runMySqlQuery(query, config, returnResults = True)
	for row in results:
		modelInfo['modelId'] = row[0]
		modelInfo['modelName'] = row[1]
		modelInfo['modelDesc'] = row[2]
		modelInfo['modelConfigDef'] = json.loads(row[3])

	return modelInfo


def logModelInfo(modelInfo):

	logger.info('modelId: %s', modelInfo['modelId'])
	logger.info('modelName: %s', modelInfo['modelName'])
	logger.info('modelDesc: %s', modelInfo['modelDesc'])
	logger.info('modelConfigDef: %s', modelInfo['modelConfigDef'])


def getAthenaConfig(config, modelInfo):

	athenaConfig = {}

	# Get modelId and scoreId
	modelId = modelInfo['modelId']
	scoreId = config['scoreId']

	# Get and set bucketName, athenaDbName
	query = """
			SELECT PROPERTY_NAME,
			       PROPERTY_VALUE
			FROM APP_CONFIG
			WHERE PROPERTY_NAME IN ('pp.di.rcdp.athena.schema.name','pp.tenant.s3.bucket.readwrite')
			AND   COMPANY_KEY = """ + config['companyKey'] + """
		"""

	results = runMySqlQuery(query, config, returnResults = True)
	resultsMap	= {row[0]:row[1] for row in results}

	bucketName = resultsMap['pp.tenant.s3.bucket.readwrite']
	athenaDbName = resultsMap['pp.di.rcdp.athena.schema.name']

	athenaConfig['bucketName'] = bucketName
	athenaConfig['athenaDbName'] = athenaDbName.lower()

	# Get and set awsRegionName
	query = """
			SELECT DISTINCT EMR_REGION_NAME
			FROM RCDP_EMR_CLUSTER_CONFIG
		"""
	results = runMySqlQuery(query, config, returnResults = True)
	awsRegionName = results[0][0]
	
	athenaConfig['awsRegionName'] = awsRegionName.lower()
	
	# Initialize stagingDir
	stagingDir = 's3://' + athenaConfig['bucketName'] + '/__internal/Athena/DataLake/Results'
	athenaConfig['stagingDir'] = stagingDir

	# Initialize drsCuratedDataPath, drsTempTableName, drsTempWriteDirPath
	drsCuratedDataPath = 's3://' + athenaConfig['bucketName'] + '/__internal/Athena/Data/' + athenaConfig['athenaDbName'] + '/dim_rfme_segment'
	athenaConfig['drsCuratedDataPath'] = drsCuratedDataPath
	drsTempTableName = 'RCDP_TMP_DIM_RFME_SEGMENT_' + str(modelId) + '_' + str(scoreId)
	athenaConfig['drsTempTableName'] = drsTempTableName
	drsTempWriteDirPath = drsCuratedDataPath + '/temp_' + str(modelId) + '_' + str(scoreId)	
	athenaConfig['drsTempWriteDirPath'] = drsTempWriteDirPath

	# Initialize drscCuratedDataPath, drscTempTableName, drscTempWriteDirPath
	drscCuratedDataPath = 's3://' + athenaConfig['bucketName'] + '/__internal/Athena/Data/' + athenaConfig['athenaDbName'] + '/dim_rfme_segment_score'
	athenaConfig['drscCuratedDataPath'] = drscCuratedDataPath
	drscTempTableName = 'RCDP_TMP_DIM_RFME_SEGMENT_SCORE_' + str(modelId) + '_' + str(scoreId)
	athenaConfig['drscTempTableName'] = drscTempTableName
	drscTempWriteDirPath = drscCuratedDataPath + '/temp_' + str(modelId) + '_' + str(scoreId)	
	athenaConfig['drscTempWriteDirPath'] = drscTempWriteDirPath

	# Initialize mcrsCuratedDataPath, mcrsAsCsvTableName, mcrsTempWriteDirPath
	mcrsCuratedDataPath = 's3://' + athenaConfig['bucketName'] + '/__internal/Athena/Data/' + athenaConfig['athenaDbName'] + '/map_cust_rfme_segment'
	athenaConfig['mcrsCuratedDataPath'] = mcrsCuratedDataPath
	mcrsAsCsvTableName = 'RCDP_TMP_MAP_CUST_RFME_SEGMENT_AS_CSV_' + str(modelId) + '_' + str(scoreId)
	athenaConfig['mcrsAsCsvTableName'] = mcrsAsCsvTableName
	mcrsTempWriteDirPath = mcrsCuratedDataPath + '/temp_' + str(modelId) + '_' + str(scoreId)	
	athenaConfig['mcrsTempWriteDirPath'] = mcrsTempWriteDirPath

	# Initialize isolation-local/s3 file paths for dim_rfme_segment, dim_rfme_segment_score
	athenaConfig['isolationDrsDirPath'] = athenaConfig['drsCuratedDataPath'] + '/isolation'
	athenaConfig['isolationDrscDirPath'] = athenaConfig['drscCuratedDataPath'] + '/isolation'
	athenaConfig['isolationDrsS3FilePath'] = athenaConfig['isolationDrsDirPath'] + '/dim_rfme_segment_' + str(modelId) + '_' + str(scoreId)
	athenaConfig['isolationDrscS3FilePath'] = athenaConfig['isolationDrscDirPath'] + '/dim_rfme_segment_score_' + str(modelId) + '_' + str(scoreId)
	athenaConfig['isolationDrsLocalFilePath'] = config['scoreLocalDirPath'] + '/dim_rfme_segment_' + str(modelId) + '_' + str(scoreId)
	athenaConfig['isolationDrscLocalFilePath'] = config['scoreLocalDirPath'] + '/dim_rfme_segment_score_' + str(modelId) + '_' + str(scoreId)

	return athenaConfig


def logAthenaConfig(athenaConfig):

	logger.info('bucketName: %s', athenaConfig['bucketName'])
	logger.info('athenaDbName: %s', athenaConfig['athenaDbName'])
	logger.info('awsRegionName: %s', athenaConfig['awsRegionName'])
	logger.info('stagingDir: %s', athenaConfig['stagingDir'])
	logger.info('drsCuratedDataPath: %s', athenaConfig['drsCuratedDataPath'])
	logger.info('drsTempTableName: %s', athenaConfig['drsTempTableName'])
	logger.info('drsTempWriteDirPath: %s', athenaConfig['drsTempWriteDirPath'])
	logger.info('drscCuratedDataPath: %s', athenaConfig['drscCuratedDataPath'])
	logger.info('drscTempTableName: %s', athenaConfig['drscTempTableName'])
	logger.info('drscTempWriteDirPath: %s', athenaConfig['drscTempWriteDirPath'])
	logger.info('mcrsCuratedDataPath: %s', athenaConfig['mcrsCuratedDataPath'])
	logger.info('mcrsAsCsvTableName: %s', athenaConfig['mcrsAsCsvTableName'])
	logger.info('mcrsTempWriteDirPath: %s', athenaConfig['mcrsTempWriteDirPath'])
	logger.info('isolationDrsDirPath: %s', athenaConfig['isolationDrsDirPath'])
	logger.info('isolationDrscDirPath: %s', athenaConfig['isolationDrscDirPath'])
	logger.info('isolationDrsS3FilePath: %s', athenaConfig['isolationDrsS3FilePath'])
	logger.info('isolationDrscS3FilePath: %s', athenaConfig['isolationDrscS3FilePath'])
	logger.info('isolationDrsLocalFilePath: %s', athenaConfig['isolationDrsLocalFilePath'])
	logger.info('isolationDrscLocalFilePath: %s', athenaConfig['isolationDrscLocalFilePath'])


def getAuroraConfig(config):

	auroraConfig = {}

	# Get host, dbname, user, password, port
	query = """
			SELECT DB_URL,
			       DB_SCHEMA_NAME,
			       DB_USER_ID,
			       DB_PWD
			FROM DIM_DB_SERVER
			WHERE DB_TYPE_NAME = 'aurora'
			AND   COMPANY_KEY = """ + config['companyKey'] + """
		"""

	results = runMySqlQuery(query, config, returnResults = True)

	auroraConfig['host'] = results[0][0].split('//', 1)[1].split(':', 1)[0]
	auroraConfig['dbname'] = results[0][1]
	auroraConfig['user'] = results[0][2]
	auroraConfig['password'] = subprocess.run(['java', '-jar', config['pyDirPath']+'/convert.jar', 'decrypt', str(results[0][3])], capture_output=True).stdout.decode('utf-8').strip('\r\n')
	auroraConfig['port'] = results[0][0].split('//', 1)[1].split(':', 1)[1]
	
	return auroraConfig


def logAuroraConfig(auroraConfig):

	logger.info('host: %s', auroraConfig['host'])
	logger.info('dbname: %s', auroraConfig['dbname'])
	logger.info('user: %s', auroraConfig['user'])
	logger.info('password: %s', '*' * (len(auroraConfig['password'])-4) + auroraConfig['password'][-4:])
	logger.info('port: %s', auroraConfig['port'])


def runAuroraQuery(query, auroraConfig, returnResults = True):

	connection = None
	cursor = None

	logger.info('Running Aurora query: %s', query)

	try:
		psycopg2Config = {key: value for (key, value) in auroraConfig.items() if key in ('host', 'dbname', 'user', 'password', 'port')}
		connection = connection = psycopg2.connect(**psycopg2Config)
		with connection.cursor() as cursor:
			cursor.execute(query)
			if returnResults is True:
				results = cursor.fetchall()
		if returnResults is True:
			return results

	finally:
		if cursor is not None:
			connection.commit()
			cursor.close()
		if connection is not None:
			connection.close()


def getRfmeScoreCodeNameListMap(modelInfo):

	recencybucketCount = modelInfo['modelConfigDef']['parameters']['recency']['bucket_count']
	frequencybucketCount = modelInfo['modelConfigDef']['parameters']['frequency']['bucket_count']
	monetarybucketCount = modelInfo['modelConfigDef']['parameters']['monetary']['bucket_count']
	engagementbucketCount = modelInfo['modelConfigDef']['parameters']['engagement']['bucket_count']

	recencybucketValue = modelInfo['modelConfigDef']['parameters']['recency']['bucket_value']
	frequencybucketValue = modelInfo['modelConfigDef']['parameters']['frequency']['bucket_value']
	monetarybucketValue = modelInfo['modelConfigDef']['parameters']['monetary']['bucket_value']
	engagementbucketValue = modelInfo['modelConfigDef']['parameters']['engagement']['bucket_value']

	recencyScoreCodeNameList = [['R' + str(index + 1), recencybucketValue[index]] for index in range(0, recencybucketCount)]
	frequencyScoreCodeNameList = [['F' + str(index + 1), frequencybucketValue[index]] for index in range(0, frequencybucketCount)]
	monetaryScoreCodeNameList = [['M' + str(index + 1), monetarybucketValue[index]] for index in range(0, monetarybucketCount)]
	engagementScoreCodeNameList = [['E' + str(index + 1), engagementbucketValue[index]] for index in range(0, engagementbucketCount)]

	rfmeScoreCodeNameListMap = {}
	rfmeScoreCodeNameListMap['recency'] = recencyScoreCodeNameList
	rfmeScoreCodeNameListMap['frequency'] = frequencyScoreCodeNameList
	rfmeScoreCodeNameListMap['monetary'] = monetaryScoreCodeNameList
	rfmeScoreCodeNameListMap['engagement'] = engagementScoreCodeNameList

	return rfmeScoreCodeNameListMap


def logRfmeScoreCodeNameListMap(rfmeScoreCodeNameListMap):

	logger.info('recencyScoreCodeNameList: %s', rfmeScoreCodeNameListMap['recency'])
	logger.info('frequencyScoreCodeNameList: %s', rfmeScoreCodeNameListMap['frequency'])
	logger.info('monetaryScoreCodeNameList: %s', rfmeScoreCodeNameListMap['monetary'])
	logger.info('engagementScoreCodeNameList: %s', rfmeScoreCodeNameListMap['engagement'])


def getRfmeBucketQueryMap(modelInfo):

	recencybucketCount = modelInfo['modelConfigDef']['parameters']['recency']['bucket_count']
	frequencybucketCount = modelInfo['modelConfigDef']['parameters']['frequency']['bucket_count']
	monetarybucketCount = modelInfo['modelConfigDef']['parameters']['monetary']['bucket_count']
	engagementbucketCount = modelInfo['modelConfigDef']['parameters']['engagement']['bucket_count']

	recencyBucketQuery = getBucketQuery('BKT_R', recencybucketCount)
	frequencyBucketQuery = getBucketQuery('BKT_F', frequencybucketCount)
	monetaryBucketQuery = getBucketQuery('BKT_M', monetarybucketCount)
	engagementBucketQuery = getBucketQuery('BKT_E', engagementbucketCount)

	rfmeBucketQueryMap = {}
	rfmeBucketQueryMap['recency'] = recencyBucketQuery
	rfmeBucketQueryMap['frequency'] = frequencyBucketQuery
	rfmeBucketQueryMap['monetary'] = monetaryBucketQuery
	rfmeBucketQueryMap['engagement'] = engagementBucketQuery

	return rfmeBucketQueryMap


def getBucketQuery(queryName, bucketCount):

	bucketNoList = [*range(1, bucketCount+1)]
	bucketNoList = ['SELECT ' + str(bucketNo) + ' AS BKT_NO' for bucketNo in bucketNoList]
	BucketQuery = queryName + ' AS (' + ' UNION ALL '.join(bucketNoList) + ')'

	return BucketQuery


def logRfmeBucketQueryMap(rfmeBucketQueryMap):

	logger.info('recencyBucketQuery: %s', rfmeBucketQueryMap['recency'])
	logger.info('frequencyBucketQuery: %s', rfmeBucketQueryMap['frequency'])
	logger.info('monetaryBucketQuery: %s', rfmeBucketQueryMap['monetary'])
	logger.info('engagementBucketQuery: %s', rfmeBucketQueryMap['engagement'])


def getRfmeMinMaxQueryMap(modelInfo):

	recencybucketCount = modelInfo['modelConfigDef']['parameters']['recency']['bucket_count']
	frequencybucketCount = modelInfo['modelConfigDef']['parameters']['frequency']['bucket_count']
	monetarybucketCount = modelInfo['modelConfigDef']['parameters']['monetary']['bucket_count']
	engagementbucketCount = modelInfo['modelConfigDef']['parameters']['engagement']['bucket_count']

	recencyMinMaxQuery = getMinMaxQuery('MM_R', 'INT_RFM_EXTRACT', 'RECENCY_VAL', recencybucketCount)
	frequencyMinMaxQuery = getMinMaxQuery('MM_F', 'INT_RFM_EXTRACT', 'FREQUENCY_VAL', frequencybucketCount)
	monetaryMinMaxQuery = getMinMaxQuery('MM_M', 'INT_RFM_EXTRACT', 'MONETARY_VAL', monetarybucketCount)
	engagementMinMaxQuery = getMinMaxQuery('MM_E', 'INT_RFM_EXTRACT', 'ENGAGEMENT_VAL', engagementbucketCount)

	rfmeMinMaxQueryMap = {}
	rfmeMinMaxQueryMap['recency'] = recencyMinMaxQuery
	rfmeMinMaxQueryMap['frequency'] = frequencyMinMaxQuery
	rfmeMinMaxQueryMap['monetary'] = monetaryMinMaxQuery
	rfmeMinMaxQueryMap['engagement'] = engagementMinMaxQuery

	return rfmeMinMaxQueryMap


def getMinMaxQuery(queryName, baseTableName, baseColumName, bucketCount):

	minBaseColumnNameExpr = 'MIN(' + baseColumName + ')'
	maxBaseColumnNameExpr = 'MAX(' + baseColumName + ')'
	bucketCountExpr = '(' + str(bucketCount) + '* 1.0' + ')'

	minMaxQuery = queryName + ' AS (' + \
					'SELECT ' + minBaseColumnNameExpr + ' AS MIN_VAL, ' + maxBaseColumnNameExpr +' AS MAX_VAL, ' + \
					'(' + maxBaseColumnNameExpr + ' - ' + minBaseColumnNameExpr + ')' + '/' + bucketCountExpr + ' AS DIFF_VAL' + \
					' FROM ' + baseTableName + ')'

	return minMaxQuery


def logRfmeMinMaxQueryMap(rfmeMinMaxQueryMap):

	logger.info('recencyMinMaxQuery: %s', rfmeMinMaxQueryMap['recency'])
	logger.info('frequencyMinMaxQuery: %s', rfmeMinMaxQueryMap['frequency'])
	logger.info('monetaryMinMaxQuery: %s', rfmeMinMaxQueryMap['monetary'])
	logger.info('engagementMinMaxQuery: %s', rfmeMinMaxQueryMap['engagement'])


def getRfmeBucketValQueryMap(modelInfo):

	recencySortOrder = modelInfo['modelConfigDef']['parameters']['recency']['sort_order']
	frequencySortOrder = modelInfo['modelConfigDef']['parameters']['frequency']['sort_order']
	monetarySortOrder = modelInfo['modelConfigDef']['parameters']['monetary']['sort_order']
	engagementSortOrder = modelInfo['modelConfigDef']['parameters']['engagement']['sort_order']

	recencybucketCount = modelInfo['modelConfigDef']['parameters']['recency']['bucket_count']
	frequencybucketCount = modelInfo['modelConfigDef']['parameters']['frequency']['bucket_count']
	monetarybucketCount = modelInfo['modelConfigDef']['parameters']['monetary']['bucket_count']
	engagementbucketCount = modelInfo['modelConfigDef']['parameters']['engagement']['bucket_count']

	recencyBucketValQuery = getBucketValQuery('BV_R', 'BKT_R', 'MM_R', recencySortOrder, recencybucketCount)
	frequencyBucketValQuery = getBucketValQuery('BV_F', 'BKT_F', 'MM_F', frequencySortOrder, frequencybucketCount)
	monetaryBucketValQuery = getBucketValQuery('BV_M', 'BKT_M', 'MM_M', monetarySortOrder, monetarybucketCount)
	engagementBucketValQuery = getBucketValQuery('BV_E', 'BKT_E', 'MM_E', engagementSortOrder, engagementbucketCount)

	rfmeBucketValQueryMap = {}
	rfmeBucketValQueryMap['recency'] = recencyBucketValQuery
	rfmeBucketValQueryMap['frequency'] = frequencyBucketValQuery
	rfmeBucketValQueryMap['monetary'] = monetaryBucketValQuery
	rfmeBucketValQueryMap['engagement'] = engagementBucketValQuery

	return rfmeBucketValQueryMap


def getBucketValQuery(queryName, bucketQueryName, minMaxQueryName, sortOrder, bucketCount):


	startValParamName = 'MIN_VAL' if sortOrder == 'asc' else 'MAX_VAL'
	endValParamName = 'MAX_VAL' if sortOrder == 'asc' else 'MIN_VAL'
	arithmeticOp = '+' if sortOrder == 'asc' else '-'
	startValCompOp = '>' if sortOrder == 'asc' else '<'

	bucketValQuery = 'SELECT BKT_NO, ' + \
						'CASE WHEN BKT_NO = 1 THEN ' + startValParamName + ' ELSE BKT_START_VAL END AS BKT_START_VAL, ' + \
						'CASE WHEN BKT_NO = ' + str(bucketCount) + ' THEN ' + endValParamName + ' ELSE BKT_END_VAL END AS BKT_END_VAL ' + \
					'FROM (SELECT BKT_NO, ' + \
								startValParamName + ', ' + \
								endValParamName + ', ' + \
								'BKT_START_VAL, ' + \
								'BKT_END_VAL, ' + \
								'COALESCE(LAG (BKT_START_VAL,1) OVER (ORDER BY BKT_NO),0) AS PREV_BKT_START_VAL ' + \
							'FROM (SELECT BKT_NO, ' + \
										startValParamName + ', ' + \
										endValParamName + ', ' + \
										'CEIL(' + startValParamName + arithmeticOp + '((BKT_NO - 1)*DIFF_VAL)) AS BKT_START_VAL, ' + \
										'CEIL(' + startValParamName + arithmeticOp + '((BKT_NO - 1)*DIFF_VAL)' + arithmeticOp + 'DIFF_VAL) BKT_END_VAL ' + \
									'FROM ' + bucketQueryName + ' ' + \
									'JOIN ' + minMaxQueryName + ' ON 1=1)) ' + \
					'WHERE (BKT_NO = 1 OR (BKT_START_VAL ' + startValCompOp + ' PREV_BKT_START_VAL))'

	bucketValQuery = queryName + ' AS (' + bucketValQuery + ')'

	return bucketValQuery


def logRfmeBucketValQueryMap(rfmeBucketValQueryMap):

	logger.info('recencyBucketValQuery: %s', rfmeBucketValQueryMap['recency'])
	logger.info('frequencyBucketValQuery: %s', rfmeBucketValQueryMap['frequency'])
	logger.info('monetaryBucketValQuery: %s', rfmeBucketValQueryMap['monetary'])
	logger.info('engagementBucketValQuery: %s', rfmeBucketValQueryMap['engagement'])


def getRfmeBucketValColSeqMap(modelInfo):

	recencySortOrder = modelInfo['modelConfigDef']['parameters']['recency']['sort_order']
	frequencySortOrder = modelInfo['modelConfigDef']['parameters']['frequency']['sort_order']
	monetarySortOrder = modelInfo['modelConfigDef']['parameters']['monetary']['sort_order']
	engagementSortOrder = modelInfo['modelConfigDef']['parameters']['engagement']['sort_order']

	recencyColSeq = getRfmeBucketValColSequence('BV_R', '_R', '_R_TEMP', recencySortOrder)
	frequencyColSeq = getRfmeBucketValColSequence('BV_F', '_F', '_F_TEMP', frequencySortOrder)
	monetaryColSeq = getRfmeBucketValColSequence('BV_M', '_M', '_M_TEMP', monetarySortOrder)
	engagementColSeq = getRfmeBucketValColSequence('BV_E', '_E', '_E_TEMP', engagementSortOrder)

	rfmeBucketValColSeqMap = {}
	rfmeBucketValColSeqMap['recency'] = recencyColSeq
	rfmeBucketValColSeqMap['frequency'] = frequencyColSeq
	rfmeBucketValColSeqMap['monetary'] = monetaryColSeq
	rfmeBucketValColSeqMap['engagement'] = engagementColSeq

	return rfmeBucketValColSeqMap


def getRfmeBucketValColSequence(bucketValQueryName, suffix, tempStartValColSuffix, sortOrder):

	arithmeticOp = '-' if sortOrder == 'asc' else '+'
	RfmeBucketValColSequence = bucketValQueryName + '.BKT_NO AS BKT_NO' + suffix + ', ' + \
								bucketValQueryName + '.BKT_START_VAL AS BKT_START_VAL' + suffix + ', ' + \
								bucketValQueryName + '.BKT_END_VAL AS BKT_END_VAL' + suffix + ', ' + \
								'CASE ' + \
									'WHEN ' + bucketValQueryName + '.BKT_NO = 1 THEN ' + bucketValQueryName + '.BKT_START_VAL ' + arithmeticOp + ' 1 ' + \
									'ELSE ' + bucketValQueryName + '.BKT_START_VAL ' + \
								'END AS BKT_START_VAL' + tempStartValColSuffix

	return RfmeBucketValColSequence


def logRfmeBucketValColSeqMap(rfmeBucketValColSeqMap):

	logger.info('recencyColSeq: %s', rfmeBucketValColSeqMap['recency'])
	logger.info('frequencyColSeq: %s', rfmeBucketValColSeqMap['frequency'])
	logger.info('monetaryColSeq: %s', rfmeBucketValColSeqMap['monetary'])
	logger.info('engagementColSeq: %s', rfmeBucketValColSeqMap['engagement'])


def getRfmeBucketValQuery(rfmeBucketValColSeqMap):

	recencyColSeq = rfmeBucketValColSeqMap['recency']
	frequencyColSeq = rfmeBucketValColSeqMap['frequency']
	monetaryColSeq = rfmeBucketValColSeqMap['monetary']
	engagementColSeq = rfmeBucketValColSeqMap['engagement']
	
	rfmeBucketValQuery = 'BV AS' + \
							'(' + \
								'SELECT ' + \
									recencyColSeq + ', ' + \
									frequencyColSeq + ', ' + \
									monetaryColSeq + ', ' + \
									engagementColSeq + ' ' + \
								'FROM BV_R ' + \
									'JOIN BV_F ON 1 = 1 ' + \
									'JOIN BV_M ON 1 = 1 ' + \
									'JOIN BV_E ON 1 = 1 ' + \
							')'

	return rfmeBucketValQuery

def logRfmeBucketValQuery(rfmeBucketValQuery):

	logger.info('rfmeBucketValQuery: %s', rfmeBucketValQuery)


def getRfmeComparisonClauseMap(modelInfo):

	recencySortOrder = modelInfo['modelConfigDef']['parameters']['recency']['sort_order']
	frequencySortOrder = modelInfo['modelConfigDef']['parameters']['frequency']['sort_order']
	monetarySortOrder = modelInfo['modelConfigDef']['parameters']['monetary']['sort_order']
	engagementSortOrder = modelInfo['modelConfigDef']['parameters']['engagement']['sort_order']

	recencyComparisonClause = getComparisonClause('RECENCY_VAL', '_R', '_R_TEMP', recencySortOrder)
	frequencyComparisonClause = getComparisonClause('FREQUENCY_VAL', '_F', '_F_TEMP', frequencySortOrder)
	monetaryComparisonClause = getComparisonClause('MONETARY_VAL', '_M', '_M_TEMP', monetarySortOrder)
	engagementComparisonClause = getComparisonClause('ENGAGEMENT_VAL', '_E', '_E_TEMP', engagementSortOrder)

	rfmeComparisonClauseMap = {}
	rfmeComparisonClauseMap['recency'] = recencyComparisonClause
	rfmeComparisonClauseMap['frequency'] = frequencyComparisonClause
	rfmeComparisonClauseMap['monetary'] = monetaryComparisonClause
	rfmeComparisonClauseMap['engagement'] = engagementComparisonClause

	return rfmeComparisonClauseMap


def getComparisonClause(columnName, suffix, tempStartValColSuffix, sortOrder):


	startValComparisonOperator = '>' if sortOrder == 'asc' else '<'
	endValComparisonOperator = '<=' if sortOrder == 'asc' else '>='

	comparisonClause = columnName + ' ' + startValComparisonOperator + ' BKT_START_VAL' +  tempStartValColSuffix + ' ' + \
						'AND ' + columnName + ' ' + endValComparisonOperator + ' BKT_END_VAL' + suffix

	return comparisonClause


def logRfmeComparisonClauseMap(rfmeComparisonClauseMap):

	logger.info('recencyComparisonClause: %s', rfmeComparisonClauseMap['recency'])
	logger.info('frequencyComparisonClause: %s', rfmeComparisonClauseMap['frequency'])
	logger.info('monetaryComparisonClause: %s', rfmeComparisonClauseMap['monetary'])
	logger.info('engagementComparisonClause: %s', rfmeComparisonClauseMap['engagement'])	


def getRfmeExtactQuery(modelInfo):

	months = modelInfo['modelConfigDef']['months']
	modelId = int(modelInfo['modelId'])
	if modelId == 1:
		rfmeExtactQuery = 'INT_RFM_EXTRACT AS' + \
							'(' + \
								'SELECT DC.CUSTOMER_CODE, ' + \
									'RECENCY_VAL,' + \
									'COALESCE(FREQUENCY_VAL,0) AS FREQUENCY_VAL, ' + \
									'COALESCE(MONETARY_VAL,0) AS MONETARY_VAL, ' + \
									'COALESCE(ENGAGEMENT_VAL,0) AS ENGAGEMENT_VAL ' + \
								'FROM (SELECT DC.CUSTOMER_CODE, ' + \
											'CASE ' + \
												'WHEN LAST_TRANSACTED_DATE IS NULL THEN 0 ' + \
												'ELSE DATE_DIFF (\'day\',LAST_TRANSACTED_DATE,CURRENT_DATE) ' + \
											'END AS RECENCY_VAL ' + \
										'FROM DIM_CUSTOMER DC ' + \
											'JOIN SNAP_CUSTOMER SC ON SC.CUSTOMER_CODE = DC.CUSTOMER_CODE ' + \
										'WHERE CUSTOMER_TYPE = \'Known\') DC ' + \
									'JOIN (SELECT CUSTOMER_CODE, ' + \
												'COUNT(DISTINCT CAST(CUSTOMER_CODE AS VARCHAR) || \'-\' || STORE_CODE || \'-\' || CAST(SALE_TRANS_DATE AS VARCHAR)) AS FREQUENCY_VAL, ' + \
												'SUM(SALE_TRANS_NET_VAL) AS MONETARY_VAL, ' + \
												'CASE ' + \
													'WHEN COUNT(DISTINCT CAST(CUSTOMER_CODE AS VARCHAR) || \'-\' || STORE_CODE || \'-\' || CAST(SALE_TRANS_DATE AS VARCHAR)) = 1 THEN 0 ' + \
													'ELSE (DATE_DIFF (\'day\',MIN(SALE_TRANS_DATE),MAX(SALE_TRANS_DATE)) + 1) /(COUNT(DISTINCT CAST(CUSTOMER_CODE AS VARCHAR) || \'-\' || STORE_CODE || \'-\' || CAST(SALE_TRANS_DATE AS VARCHAR)) - 1) ' + \
												'END AS ENGAGEMENT_VAL ' + \
											'FROM FCT_SALE_TRANSACTION ' + \
											'WHERE DATE_DIFF(\'month\',SALE_TRANS_DATE,CURRENT_DATE) <= ' + str(months) + ' ' + \
											'GROUP BY CUSTOMER_CODE) FCT ON FCT.CUSTOMER_CODE = DC.CUSTOMER_CODE ' + \
							')'
	elif modelId == 2:
		rfmeExtactQuery = """
			INT_RFM_EXTRACT AS
			(
			  SELECT FS.CUSTOMER_CODE,
			         COALESCE(RECENCY_VAL,0) AS RECENCY_VAL,
			         COALESCE(FREQUENCY_VAL,0) AS FREQUENCY_VAL,
			         COALESCE(MONETARY_VAL,0) AS MONETARY_VAL,
			         COALESCE(ENGAGEMENT_VAL,0) AS ENGAGEMENT_VAL
			  FROM (SELECT CUSTOMER_CODE,
			               DATE_DIFF('day',MAX(EVENT_DATE),CURRENT_DATE) AS RECENCY_VAL,
			               COUNT(DISTINCT CAST(CUSTOMER_CODE AS VARCHAR) || '-' || CAST(EVENT_DATE AS VARCHAR)) AS FREQUENCY_VAL,
			               CASE
			                 WHEN COUNT(DISTINCT CAST(CUSTOMER_CODE AS VARCHAR) || '-' || CAST(EVENT_DATE AS VARCHAR)) = 1 THEN 0
			                 ELSE (DATE_DIFF ('day',MIN(EVENT_DATE),MAX(EVENT_DATE)) + 1) /(COUNT(DISTINCT CAST(CUSTOMER_CODE AS VARCHAR) || '-' || CAST(EVENT_DATE AS VARCHAR)) -1)
			               END AS ENGAGEMENT_VAL
			        FROM FCT_CS_SESSION
			        WHERE DATE_DIFF('month',EVENT_DATE,CURRENT_DATE) <= """ + str(months) + """
			        GROUP BY CUSTOMER_CODE) FS
			    LEFT JOIN (SELECT CUSTOMER_CODE,
			                      SUM(TOTAL_AMT) AS MONETARY_VAL
			               FROM FCT_CS_EVENT_TRANS_COMPLETE
			               WHERE DATE_DIFF('month',EVENT_DATE,CURRENT_DATE) <= """ + str(months) + """
			               GROUP BY CUSTOMER_CODE) FETC ON FETC.CUSTOMER_CODE = FS.CUSTOMER_CODE
			  WHERE FREQUENCY_VAL > 1
			)
		"""

	return rfmeExtactQuery


def logRfmeExtactQuery(rfmeExtactQuery):

	logger.info('rfmeExtactQuery: %s', rfmeExtactQuery)


def getRfmeQueryMap(rfmeScoreCodeNameListMap, rfmeBucketQueryMap, rfmeMinMaxQueryMap, rfmeBucketValQueryMap, rfmeBucketValQuery, rfmeComparisonClauseMap, rfmeExtactQuery):

	rfmeQueryMap = {}
	rfmeQueryMap['rfmeScoreCodeNameListMap'] = rfmeScoreCodeNameListMap
	rfmeQueryMap['rfmeBucketQueryMap'] = rfmeBucketQueryMap
	rfmeQueryMap['rfmeMinMaxQueryMap'] = rfmeMinMaxQueryMap
	rfmeQueryMap['rfmeBucketValQueryMap'] = rfmeBucketValQueryMap
	rfmeQueryMap['rfmeBucketValQuery'] = rfmeBucketValQuery
	rfmeQueryMap['rfmeComparisonClauseMap'] = rfmeComparisonClauseMap
	rfmeQueryMap['rfmeExtactQuery'] = rfmeExtactQuery

	return rfmeQueryMap


def getMapCustRfmeSegmentQuery(modelInfo, rfmeQueryMap, startTime, scoreId):

	modelId = modelInfo['modelId']
	modelName = modelInfo['modelName']
	modelDesc = modelInfo['modelDesc']

	rfmeBucketQueryMap = rfmeQueryMap['rfmeBucketQueryMap']
	rfmeMinMaxQueryMap = rfmeQueryMap['rfmeMinMaxQueryMap']
	rfmeBucketValQueryMap = rfmeQueryMap['rfmeBucketValQueryMap']
	rfmeBucketValQuery = rfmeQueryMap['rfmeBucketValQuery']
	rfmeComparisonClauseMap = rfmeQueryMap['rfmeComparisonClauseMap']
	rfmeExtactQuery = rfmeQueryMap['rfmeExtactQuery']

	recencyComparisonClause = rfmeComparisonClauseMap['recency']
	frequencyComparisonClause = rfmeComparisonClauseMap['frequency']
	monetaryComparisonClause = rfmeComparisonClauseMap['monetary']
	engagementComparisonClause = rfmeComparisonClauseMap['engagement']

	cteQueryList = [rfmeExtactQuery] + \
					list(rfmeBucketQueryMap.values()) + \
					list(rfmeMinMaxQueryMap.values()) + \
					list(rfmeBucketValQueryMap.values()) + \
					[rfmeBucketValQuery]

	cteQuery = 'WITH ' + ', '.join(cteQueryList)
	mapCustRfmeSegmentQuery = 'INSERT INTO MAP_CUST_RFME_SEGMENT' + \
								'(RFME_SEGMENT_ID, RFME_SEGMENT_SCORE_CODE, CUSTOMER_CODE, RECENCY_VAL, FREQUENCY_VAL, MONETARY_VAL, ENGAGEMENT_VAL, CREATED_DATE, RFME_SEGMENT_SCORE_ID) ' + \
								cteQuery + \
								'SELECT CAST(\'' + str(modelId) + '\' AS BIGINT) AS RFME_SEGMENT_ID, ' + \
									'CAST(\'R\' || CAST(BKT_NO_R AS VARCHAR) || \'F\' || CAST(BKT_NO_F AS VARCHAR) || \'M\' || CAST(BKT_NO_M AS VARCHAR) || \'E\' || CAST(BKT_NO_E AS VARCHAR) AS VARCHAR(20)) AS RFME_SEGMENT_SCORE_CODE, ' + \
									'CUSTOMER_CODE, ' + \
									'CAST(RECENCY_VAL AS INT) AS RECENCY_VAL, ' + \
									'CAST(FREQUENCY_VAL AS INT) AS FREQUENCY_VAL, ' + \
									'CAST(FREQUENCY_VAL AS DECIMAL(22,4)) AS MONETARY_VAL, ' + \
									'CAST(ENGAGEMENT_VAL AS DECIMAL(22,4)) AS ENGAGEMENT_VAL, ' + \
									'CAST(\'' + str(startTime) + '\' AS TIMESTAMP) AS CREATED_DATE, ' + \
									'CAST(\'' + str(scoreId) + '\' AS BIGINT) AS RFME_SEGMENT_SCORE_ID ' + \
								'FROM BV ' + \
									'JOIN INT_RFM_EXTRACT ' + \
									'ON ' + recencyComparisonClause + ' ' + \
									'AND ' + frequencyComparisonClause + ' ' + \
									'AND ' + monetaryComparisonClause + ' ' + \
									'AND ' + engagementComparisonClause

	return mapCustRfmeSegmentQuery


def getDimRfmeSegmentQuery(modelInfo, athenaConfig):

	modelId = modelInfo['modelId']
	modelName = modelInfo['modelName']
	modelDesc = modelInfo['modelDesc']
	bucketType = modelInfo['modelConfigDef']['bucket_type']
	
	recencybucketCount = modelInfo['modelConfigDef']['parameters']['recency']['bucket_count']
	frequencybucketCount = modelInfo['modelConfigDef']['parameters']['frequency']['bucket_count']
	monetaryCount = modelInfo['modelConfigDef']['parameters']['monetary']['bucket_count']
	engagementbucketCount = modelInfo['modelConfigDef']['parameters']['engagement']['bucket_count']
	bucketCount = ceil((int(recencybucketCount) + int(frequencybucketCount) + int(monetaryCount) + int(engagementbucketCount))/4)
	
	drsTempWriteDirPath = athenaConfig['drsTempWriteDirPath']
	drsTempTableName = athenaConfig['drsTempTableName']

	query = """
			CREATE TABLE """ + drsTempTableName + """ WITH 
			(
			  external_location   = '""" + drsTempWriteDirPath + """',
			  format              = 'parquet',
			  bucket_count        = 1,
			  bucketed_by         = ARRAY['RFME_SEGMENT_ID']
			)
			AS
			SELECT RFME_SEGMENT_ID,
			       RFME_SEGMENT_NAME,
			       RFME_SEGMENT_DESC,
			       RFME_BUCKET_TYPE,
			       RFME_BUCKET_COUNT,
			       ACTIVE,
			       CREATED_DATE,
			       UPDATED_DATE
			FROM DIM_RFME_SEGMENT
			WHERE RFME_SEGMENT_ID != """ + str(modelId) + """
			UNION ALL
			SELECT RFME_SEGMENT_ID,
			       RFME_SEGMENT_NAME,
			       RFME_SEGMENT_DESC,
			       RFME_BUCKET_TYPE,
			       RFME_BUCKET_COUNT,
			       ACTIVE,
			       COALESCE(DRS.CREATED_DATE,DRS_NEW.CREATED_DATE) AS CREATED_DATE,
			       UPDATED_DATE
			FROM (SELECT CAST('""" + str(modelId) + """' AS BIGINT) AS RFME_SEGMENT_ID,
			             CAST('""" + str(modelName) + """' AS VARCHAR(100)) AS RFME_SEGMENT_NAME,
			             CAST('""" + str(modelDesc) + """' AS VARCHAR(500)) AS RFME_SEGMENT_DESC,
			             CAST('""" + str(bucketType) + """' AS VARCHAR(20)) AS RFME_BUCKET_TYPE,
			             CAST('""" + str(bucketCount) + """' AS SMALLINT) AS RFME_BUCKET_COUNT,
			             CAST(1 AS SMALLINT) AS ACTIVE,
			             CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS CREATED_DATE,
			             CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS UPDATED_DATE) DRS_NEW
			  LEFT JOIN (SELECT CREATED_DATE
			             FROM DIM_RFME_SEGMENT
			             WHERE RFME_SEGMENT_ID = """ + str(modelId) + """) DRS ON 1 = 1
		"""

	return query


def getDropDimRfmeSegmentQuery(athenaConfig):

	drsTempTableName = athenaConfig['drsTempTableName']
	dropDimRfmeSegmentQuery = 'DROP TABLE IF EXISTS ' + drsTempTableName

	return dropDimRfmeSegmentQuery


def getDimRfmeSegmentScoreQuery(modelInfo, rfmeQueryMap, startTime, scoreId, athenaConfig):

	modelId = modelInfo['modelId']
	bucketName = athenaConfig['bucketName']
	athenaDbName = athenaConfig['athenaDbName']
	drscTempWriteDirPath = athenaConfig['drscTempWriteDirPath']
	drscTempTableName = athenaConfig['drscTempTableName']
	
	recencyScoreCodeNameList = rfmeQueryMap['rfmeScoreCodeNameListMap']['recency']
	frequencyScoreCodeNameList = rfmeQueryMap['rfmeScoreCodeNameListMap']['frequency']
	monetaryScoreCodeNameList = rfmeQueryMap['rfmeScoreCodeNameListMap']['monetary']
	engagementScoreCodeNameList = rfmeQueryMap['rfmeScoreCodeNameListMap']['engagement']

	scoreCodeNameList = [rsnl + fsnl + msnl + esnl
							for rsnl in recencyScoreCodeNameList 
							for fsnl in frequencyScoreCodeNameList 
							for msnl in monetaryScoreCodeNameList
							for esnl in engagementScoreCodeNameList
						]

	segmentScoreQueryList = []
	for index in range(len(scoreCodeNameList)):
		rfmeScoreCodeStmnt = '\'' + scoreCodeNameList[index][0] + scoreCodeNameList[index][2] + scoreCodeNameList[index][4] + scoreCodeNameList[index][6] + '\' AS RFME_SEGMENT_SCORE_CODE' 
		recencyScoreCodeStmnt = '\'' + scoreCodeNameList[index][0] + '\' AS RFME_RECENCY_SCORE_CODE' 
		recencyScoreNameStmnt = '\'' + scoreCodeNameList[index][1] + '\' AS RFME_RECENCY_SCORE_NAME'
		frequencyScoreCodeStmnt = '\'' + scoreCodeNameList[index][2] + '\' AS RFME_FREQUENCY_SCORE_CODE' 
		frequencyScoreNameStmnt = '\'' + scoreCodeNameList[index][3] + '\' AS RFME_FREQUENCY_SCORE_NAME'
		monetaryScoreCodeStmnt = '\'' + scoreCodeNameList[index][4] + '\' AS RFME_MONETARY_SCORE_CODE' 
		monetaryScoreNameStmnt = '\'' + scoreCodeNameList[index][5] + '\' AS RFME_MONETARY_SCORE_NAME'
		engagementScoreCodeStmnt = '\'' + scoreCodeNameList[index][6] + '\' AS RFME_ENGAGEMENT_SCORE_CODE' 
		engagementScoreNameStmnt = '\'' + scoreCodeNameList[index][7] + '\' AS RFME_ENGAGEMENT_SCORE_NAME'

		segmentScoreQueryList.append('SELECT ' + ', '.join([
										rfmeScoreCodeStmnt,
										recencyScoreCodeStmnt,
										recencyScoreNameStmnt,
										frequencyScoreCodeStmnt,
										frequencyScoreNameStmnt,
										monetaryScoreCodeStmnt,
										monetaryScoreNameStmnt,
										engagementScoreCodeStmnt,
										engagementScoreNameStmnt
									]))

	segmentScoreQuery = ' UNION ALL '.join(segmentScoreQueryList)

	dimRfmeSegmentScoreQuery = 'CREATE TABLE ' + drscTempTableName + ' ' + \
								'WITH ( ' + \
									'external_location = \'' + drscTempWriteDirPath + '\', ' + \
									'format = \'parquet\', ' + \
									'bucket_count = 1, ' + \
									'bucketed_by = ARRAY[\'RFME_SEGMENT_SCORE_ID\'] ' + \
								') AS ' + \
								'SELECT CAST(\'' + str(modelId) + '\' AS BIGINT) AS RFME_SEGMENT_ID, ' + \
									'CAST(\'' + str(scoreId) + '\' AS BIGINT) AS RFME_SEGMENT_SCORE_ID, ' + \
									'CAST(\'Score - ' + str(scoreId) + '\' AS VARCHAR(100)) AS RFME_SEGMENT_SCORE_NAME, ' + \
									'CAST(MCRS.RFME_SEGMENT_SCORE_CODE AS VARCHAR(20)) AS RFME_SEGMENT_SCORE_CODE, ' + \
									'CAST(RFME_RECENCY_SCORE_CODE AS VARCHAR(20)) AS RFME_RECENCY_SCORE_CODE, ' + \
									'CAST(RFME_RECENCY_SCORE_NAME AS VARCHAR(100)) AS RFME_RECENCY_SCORE_NAME, ' + \
									'CAST(RFME_FREQUENCY_SCORE_CODE AS VARCHAR(20)) AS RFME_FREQUENCY_SCORE_CODE, ' + \
									'CAST(RFME_FREQUENCY_SCORE_NAME AS VARCHAR(100)) AS RFME_FREQUENCY_SCORE_NAME, ' + \
									'CAST(RFME_MONETARY_SCORE_CODE AS VARCHAR(20)) AS RFME_MONETARY_SCORE_CODE, ' + \
									'CAST(RFME_MONETARY_SCORE_NAME AS VARCHAR(100)) AS RFME_MONETARY_SCORE_NAME, ' + \
									'CAST(RFME_ENGAGEMENT_SCORE_CODE AS VARCHAR(20)) AS RFME_ENGAGEMENT_SCORE_CODE, ' + \
									'CAST(RFME_ENGAGEMENT_SCORE_NAME AS VARCHAR(100)) AS RFME_ENGAGEMENT_SCORE_NAME, ' + \
									'CAST(1 AS SMALLINT) AS RFME_SEGMENT_SCORE_LATEST_FLG, ' + \
									'CAST(CUSTOMER_COUNT AS INT) AS CUSTOMER_COUNT, ' + \
									'CAST(\'' + str(startTime) + '\' AS TIMESTAMP) AS CREATED_DATE ' + \
								'FROM ' + \
									'(SELECT RFME_SEGMENT_SCORE_ID, ' + \
										'RFME_SEGMENT_SCORE_CODE, ' + \
										'COUNT(*) AS CUSTOMER_COUNT ' + \
									'FROM MAP_CUST_RFME_SEGMENT ' + \
									'WHERE RFME_SEGMENT_SCORE_ID = CAST(\'' + str(scoreId) + '\' AS BIGINT) ' + \
									'GROUP BY RFME_SEGMENT_SCORE_ID, ' + \
										'RFME_SEGMENT_SCORE_CODE) MCRS ' + \
									'JOIN (' + segmentScoreQuery + ') DIM ON MCRS.RFME_SEGMENT_SCORE_CODE = DIM.RFME_SEGMENT_SCORE_CODE ' + \
								'UNION ALL ' + \
								'SELECT RFME_SEGMENT_ID,' + \
									'RFME_SEGMENT_SCORE_ID, ' + \
									'RFME_SEGMENT_SCORE_NAME, ' + \
									'RFME_SEGMENT_SCORE_CODE, ' + \
									'RFME_RECENCY_SCORE_CODE, ' + \
									'RFME_RECENCY_SCORE_NAME, ' + \
									'RFME_FREQUENCY_SCORE_CODE, ' + \
									'RFME_FREQUENCY_SCORE_NAME, ' + \
									'RFME_MONETARY_SCORE_CODE, ' + \
									'RFME_MONETARY_SCORE_NAME, ' + \
									'RFME_ENGAGEMENT_SCORE_CODE, ' + \
									'RFME_ENGAGEMENT_SCORE_NAME, ' + \
									'CAST(0 AS SMALLINT) AS RFME_SEGMENT_SCORE_LATEST_FLG, ' + \
									'CUSTOMER_COUNT, ' + \
									'CREATED_DATE ' + \
								'FROM DIM_RFME_SEGMENT_SCORE ' + \
								'WHERE RFME_SEGMENT_ID = ' + str(modelId) + ' ' + \
								'UNION ALL ' + \
								'SELECT RFME_SEGMENT_ID,' + \
									'RFME_SEGMENT_SCORE_ID, ' + \
									'RFME_SEGMENT_SCORE_NAME, ' + \
									'RFME_SEGMENT_SCORE_CODE, ' + \
									'RFME_RECENCY_SCORE_CODE, ' + \
									'RFME_RECENCY_SCORE_NAME, ' + \
									'RFME_FREQUENCY_SCORE_CODE, ' + \
									'RFME_FREQUENCY_SCORE_NAME, ' + \
									'RFME_MONETARY_SCORE_CODE, ' + \
									'RFME_MONETARY_SCORE_NAME, ' + \
									'RFME_ENGAGEMENT_SCORE_CODE, ' + \
									'RFME_ENGAGEMENT_SCORE_NAME, ' + \
									'RFME_SEGMENT_SCORE_LATEST_FLG, ' + \
									'CUSTOMER_COUNT, ' + \
									'CREATED_DATE ' + \
								'FROM DIM_RFME_SEGMENT_SCORE ' + \
								'WHERE RFME_SEGMENT_ID != ' + str(modelId) + ' '


	return dimRfmeSegmentScoreQuery


def getDropDimRfmeSegmentScoreQuery(modelInfo, scoreId, athenaConfig):

	drscTempTableName = athenaConfig['drscTempTableName']
	dropDimRfmeSegmentScoreQuery = 'DROP TABLE IF EXISTS ' + drscTempTableName

	return dropDimRfmeSegmentScoreQuery


def getMapCustRfmeSegmentAsCsvQuery(modelInfo, scoreId, athenaConfig):

	modelId = modelInfo['modelId']
	mcrsAsCsvTableName = athenaConfig['mcrsAsCsvTableName']
	mcrsTempWriteDirPath = athenaConfig['mcrsTempWriteDirPath']

	mapCustRfmeSegmentAsCsvQuery = """
		CREATE TABLE """ + mcrsAsCsvTableName + """ WITH 
		(
		  external_location   = '""" + mcrsTempWriteDirPath + """',
		  format              = 'textfile',
		  field_delimiter     = '~',
		  bucket_count        = 1,
		  bucketed_by         = ARRAY['RFME_SEGMENT_SCORE_CODE']
		)
		AS
		SELECT CUSTOMER_CODE,
		       MCRS.RFME_SEGMENT_SCORE_CODE,
		       RFME_RECENCY_SCORE_NAME,
		       RFME_FREQUENCY_SCORE_NAME,
		       RFME_MONETARY_SCORE_NAME,
		       RFME_ENGAGEMENT_SCORE_NAME
		FROM MAP_CUST_RFME_SEGMENT MCRS
		  JOIN DIM_RFME_SEGMENT_SCORE DRSC ON DRSC.RFME_SEGMENT_SCORE_CODE = MCRS.RFME_SEGMENT_SCORE_CODE
		WHERE MCRS.RFME_SEGMENT_SCORE_ID = CAST('""" + str(scoreId) + """' AS BIGINT)
		AND   DRSC.RFME_SEGMENT_SCORE_ID = CAST('""" + str(scoreId) + """' AS BIGINT)
	"""

	return mapCustRfmeSegmentAsCsvQuery


def getDropMapCustRfmeSegmentAsCsvQuery(scoreId, athenaConfig):

	mcrsAsCsvTableName = athenaConfig['mcrsAsCsvTableName']
	dropMapCustRfmeSegmentAsCsvQuery = 'DROP TABLE IF EXISTS ' + mcrsAsCsvTableName

	return dropMapCustRfmeSegmentAsCsvQuery


def getCreateMcrsDeltaTableQuery(modelInfo, mcrsDeltaTableName):

	modelId = modelInfo['modelId']

	if modelId == 1:
		createMcrsDeltaTableQuery = """
			CREATE TABLE """ + mcrsDeltaTableName + """ 
			(
			  CUSTOMER_CODE                BIGINT,
			  RFME_SEGMENT_SCORE_CODE      VARCHAR(20),
			  RFME_RECENCY_SCORE_NAME      VARCHAR(100),
			  RFME_FREQUENCY_SCORE_NAME    VARCHAR(100),
			  RFME_MONETARY_SCORE_NAME     VARCHAR(100),
			  RFME_ENGAGEMENT_SCORE_NAME   VARCHAR(100)
			)
		"""
	elif modelId == 2:
		createMcrsDeltaTableQuery = """
			CREATE TABLE """ + mcrsDeltaTableName + """ 
			(
			  CUSTOMER_CODE                      BIGINT,
			  RFME_EVENT_SEGMENT_SCORE_CODE      VARCHAR(20),
			  RFME_EVENT_RECENCY_SCORE_NAME      VARCHAR(100),
			  RFME_EVENT_FREQUENCY_SCORE_NAME    VARCHAR(100),
			  RFME_EVENT_MONETARY_SCORE_NAME     VARCHAR(100),
			  RFME_EVENT_ENGAGEMENT_SCORE_NAME   VARCHAR(100)
			)
		"""

	return createMcrsDeltaTableQuery


def getDropMcrsDeltaTableQuery(mcrsDeltaTableName):

	dropMapCustRfmeSegmentAsCsvQuery = 'DROP TABLE IF EXISTS ' + mcrsDeltaTableName

	return dropMapCustRfmeSegmentAsCsvQuery


def getInsertMcrsDeltaTableQuery(modelInfo, mcrsDeltaTableName):

	modelId = modelInfo['modelId']

	if modelId == 1:
		insertMcrsDeltaTableQuery = """
			INSERT INTO """ + mcrsDeltaTableName + """
			(
			  CUSTOMER_CODE,
			  RFME_SEGMENT_SCORE_CODE,
			  RFME_RECENCY_SCORE_NAME,
			  RFME_FREQUENCY_SCORE_NAME,
			  RFME_MONETARY_SCORE_NAME,
			  RFME_ENGAGEMENT_SCORE_NAME
			)
			SELECT SC.CUSTOMER_CODE,
			       NULL AS RFME_SEGMENT_SCORE_CODE,
			       NULL AS RFME_RECENCY_SCORE_NAME,
			       NULL AS RFME_FREQUENCY_SCORE_NAME,
			       NULL AS RFME_MONETARY_SCORE_NAME,
			       NULL AS RFME_ENGAGEMENT_SCORE_NAME
			FROM SNAP_CUSTOMER SC
			  LEFT JOIN """ + mcrsDeltaTableName + """ TEMP ON SC.CUSTOMER_CODE = TEMP.CUSTOMER_CODE
			WHERE TEMP.CUSTOMER_CODE IS NULL
			AND   SC.RFME_SEGMENT_SCORE_CODE IS NOT NULL
			"""

	elif modelId == 2:
		insertMcrsDeltaTableQuery = """
			INSERT INTO """ + mcrsDeltaTableName + """
			(
			  CUSTOMER_CODE,
			  RFME_EVENT_SEGMENT_SCORE_CODE,
			  RFME_EVENT_RECENCY_SCORE_NAME,
			  RFME_EVENT_FREQUENCY_SCORE_NAME,
			  RFME_EVENT_MONETARY_SCORE_NAME,
			  RFME_EVENT_ENGAGEMENT_SCORE_NAME
			)
			SELECT SC.CUSTOMER_CODE,
			       NULL AS RFME_EVENT_SEGMENT_SCORE_CODE,
			       NULL AS RFME_EVENT_RECENCY_SCORE_NAME,
			       NULL AS RFME_EVENT_FREQUENCY_SCORE_NAME,
			       NULL AS RFME_EVENT_MONETARY_SCORE_NAME,
			       NULL AS RFME_EVENT_ENGAGEMENT_SCORE_NAME
			FROM SNAP_CUSTOMER SC
			  LEFT JOIN """ + mcrsDeltaTableName + """ TEMP ON SC.CUSTOMER_CODE = TEMP.CUSTOMER_CODE
			WHERE TEMP.CUSTOMER_CODE IS NULL
			AND   SC.RFME_SEGMENT_SCORE_CODE IS NOT NULL
			"""

	return insertMcrsDeltaTableQuery


def getDeleteFromMcrsDeltaTableQuery(modelInfo, mcrsDeltaTableName):

	modelId = modelInfo['modelId']

	if modelId == 1:
		deleteFromMcrsDeltaTableQuery = """
			DELETE
			FROM """ + mcrsDeltaTableName + """ TMP USING SNAP_CUSTOMER SNAP
			WHERE SNAP.CUSTOMER_CODE = TMP.CUSTOMER_CODE
			AND   SNAP.RFME_SEGMENT_SCORE_CODE = TMP.RFME_SEGMENT_SCORE_CODE
			AND   SNAP.RFME_RECENCY_SCORE_NAME = TMP.RFME_RECENCY_SCORE_NAME
			AND   SNAP.RFME_FREQUENCY_SCORE_NAME = TMP.RFME_FREQUENCY_SCORE_NAME
			AND   SNAP.RFME_MONETARY_SCORE_NAME = TMP.RFME_MONETARY_SCORE_NAME
			AND   SNAP.RFME_ENGAGEMENT_SCORE_NAME = TMP.RFME_ENGAGEMENT_SCORE_NAME
		"""

	elif modelId == 2:
		deleteFromMcrsDeltaTableQuery = """
			DELETE
			FROM """ + mcrsDeltaTableName + """ TMP USING SNAP_CUSTOMER SNAP
			WHERE SNAP.CUSTOMER_CODE = TMP.CUSTOMER_CODE
			AND   SNAP.RFME_EVENT_SEGMENT_SCORE_CODE = TMP.RFME_EVENT_SEGMENT_SCORE_CODE
			AND   SNAP.RFME_EVENT_RECENCY_SCORE_NAME = TMP.RFME_EVENT_RECENCY_SCORE_NAME
			AND   SNAP.RFME_EVENT_FREQUENCY_SCORE_NAME = TMP.RFME_EVENT_FREQUENCY_SCORE_NAME
			AND   SNAP.RFME_EVENT_MONETARY_SCORE_NAME = TMP.RFME_EVENT_MONETARY_SCORE_NAME
			AND   SNAP.RFME_EVENT_ENGAGEMENT_SCORE_NAME = TMP.RFME_EVENT_ENGAGEMENT_SCORE_NAME
		"""

	return deleteFromMcrsDeltaTableQuery


def generateRfmeScoreQueryMap(modelInfo, startTime, scoreId, mcrsDeltaTableName, athenaConfig):

	rfmeScoreCodeNameListMap = getRfmeScoreCodeNameListMap(modelInfo); logRfmeScoreCodeNameListMap(rfmeScoreCodeNameListMap)
	rfmeBucketQueryMap = getRfmeBucketQueryMap(modelInfo); logRfmeBucketQueryMap(rfmeBucketQueryMap)
	rfmeMinMaxQueryMap = getRfmeMinMaxQueryMap(modelInfo); logRfmeMinMaxQueryMap(rfmeMinMaxQueryMap)
	rfmeBucketValQueryMap = getRfmeBucketValQueryMap(modelInfo); logRfmeBucketValQueryMap(rfmeBucketValQueryMap)
	rfmeBucketValColSeqMap = getRfmeBucketValColSeqMap(modelInfo); logRfmeBucketValColSeqMap(rfmeBucketValColSeqMap)
	rfmeBucketValQuery = getRfmeBucketValQuery(rfmeBucketValColSeqMap); logRfmeBucketValQuery(rfmeBucketValQuery)
	rfmeComparisonClauseMap = getRfmeComparisonClauseMap(modelInfo); logRfmeComparisonClauseMap(rfmeComparisonClauseMap)
	rfmeExtactQuery = getRfmeExtactQuery(modelInfo); logRfmeExtactQuery(rfmeExtactQuery)
	rfmeQueryMap = getRfmeQueryMap(rfmeScoreCodeNameListMap, rfmeBucketQueryMap, rfmeMinMaxQueryMap, rfmeBucketValQueryMap, rfmeBucketValQuery, rfmeComparisonClauseMap, rfmeExtactQuery)
	mapCustRfmeSegmentQuery = getMapCustRfmeSegmentQuery(modelInfo, rfmeQueryMap, startTime, scoreId)
	dimRfmeSegmentQuery =  getDimRfmeSegmentQuery(modelInfo, athenaConfig)
	dropDimRfmeSegmentQuery = getDropDimRfmeSegmentQuery(athenaConfig)
	dimRfmeSegmentScoreQuery = getDimRfmeSegmentScoreQuery(modelInfo, rfmeQueryMap, startTime, scoreId, athenaConfig)
	dropDimRfmeSegmentScoreQuery = getDropDimRfmeSegmentScoreQuery(modelInfo, scoreId, athenaConfig)
	mapCustRfmeSegmentAsCsvQuery = getMapCustRfmeSegmentAsCsvQuery(modelInfo, scoreId, athenaConfig)
	dropMapCustRfmeSegmentAsCsvQuery = getDropMapCustRfmeSegmentAsCsvQuery(scoreId, athenaConfig)
	createMcrsDeltaTableQuery = getCreateMcrsDeltaTableQuery(modelInfo, mcrsDeltaTableName)
	dropMcrsDeltaTableQuery = getDropMcrsDeltaTableQuery(mcrsDeltaTableName)
	insertMcrsDeltaTableQuery = getInsertMcrsDeltaTableQuery(modelInfo, mcrsDeltaTableName)
	deleteFromMcrsDeltaTableQuery = getDeleteFromMcrsDeltaTableQuery(modelInfo, mcrsDeltaTableName)

	rfmeScoreQueryMap = {}
	rfmeScoreQueryMap['mapCustRfmeSegmentQuery'] = mapCustRfmeSegmentQuery
	rfmeScoreQueryMap['dimRfmeSegmentQuery'] = dimRfmeSegmentQuery
	rfmeScoreQueryMap['dropDimRfmeSegmentQuery'] = dropDimRfmeSegmentQuery
	rfmeScoreQueryMap['dimRfmeSegmentScoreQuery'] = dimRfmeSegmentScoreQuery
	rfmeScoreQueryMap['dropDimRfmeSegmentScoreQuery'] = dropDimRfmeSegmentScoreQuery
	rfmeScoreQueryMap['mapCustRfmeSegmentAsCsvQuery'] = mapCustRfmeSegmentAsCsvQuery
	rfmeScoreQueryMap['dropMapCustRfmeSegmentAsCsvQuery'] = dropMapCustRfmeSegmentAsCsvQuery
	rfmeScoreQueryMap['createMcrsDeltaTableQuery'] = createMcrsDeltaTableQuery
	rfmeScoreQueryMap['dropMcrsDeltaTableQuery'] = dropMcrsDeltaTableQuery
	rfmeScoreQueryMap['insertMcrsDeltaTableQuery'] = insertMcrsDeltaTableQuery
	rfmeScoreQueryMap['deleteFromMcrsDeltaTableQuery'] = deleteFromMcrsDeltaTableQuery

	return rfmeScoreQueryMap


def runAthenaQuery(query, athenaConfig):

	bucketName = athenaConfig['bucketName']
	athenaDbName = athenaConfig['athenaDbName']
	awsRegionName = athenaConfig['awsRegionName']
	stagingDir = athenaConfig['stagingDir']

	logger.info('Running Athena query: %s', query)

	client = boto3.client('athena', region_name = awsRegionName)
	ResultConfiguration = {'OutputLocation': stagingDir, 'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}}

	response = client.start_query_execution(QueryString = query, 
												QueryExecutionContext = {'Database': athenaDbName}, 
												ResultConfiguration = ResultConfiguration
											)
	prevExecutionState = ''
	QueryExecutionId = response['QueryExecutionId']
	logger.info('QueryExecutionId: %s', QueryExecutionId)

	while True:
		executionInfo = client.get_query_execution(QueryExecutionId = QueryExecutionId)
		executionState = executionInfo['QueryExecution']['Status']['State']
		
		if executionState in ('QUEUED','RUNNING'):
			if prevExecutionState != executionState:
				prevExecutionState = executionState
				logger.info('Execution State: [%s]', executionState)
			time.sleep(1)
		else:
			submissionDateTime = executionInfo['QueryExecution']['Status']['SubmissionDateTime']
			completionDateTime = executionInfo['QueryExecution']['Status']['CompletionDateTime']
			if executionState == 'SUCCEEDED':
				executionTime = str(completionDateTime-submissionDateTime)
				logger.info('Execution State: [%s] Execution Time: [%s]', executionState, executionTime)
				break
			elif executionState in ('FAILED', 'CANCELLED'):
				stateChangeReason = executionInfo['QueryExecution']['Status']['StateChangeReason'] \
										if 'StateChangeReason' in executionInfo['QueryExecution']['Status'] \
										else ''
				logger.error('Execution Status: [%s] StateChangeReason: [%s]', executionState, stateChangeReason)
				raise Exception('AthenaQueryExecutionFailed')


def hideFiles(bucketName, dirPath, awsRegionName):

	logger.info('Hiding files in %s', dirPath)

	client = boto3.client('s3', region_name = awsRegionName)
	s3 = boto3.resource('s3', region_name = awsRegionName)

	prefix = dirPath.split(bucketName, 1)[1].strip('/')
	ExtraArgs = {'ServerSideEncryption': 'AES256'}
	paginator = client.get_paginator('list_objects_v2')
	page_iterator = paginator.paginate(Bucket = bucketName, Prefix = prefix + '/', Delimiter = '/')
	for page in page_iterator:
		if 'Contents' in page:
			objectsToBeDeleted = []
			for content in page.get('Contents'):
				srcFileKey = content.get('Key')
				srcFileName = srcFileKey.split(prefix, 1)[1].lstrip('/')
				logger.info('srcFileName: %s', srcFileName)
				if srcFileName.startswith('_') is False and srcFileName.startswith('.') is False:
					objectsToBeDeleted.append({'Key': srcFileKey})
					tgtFileName = '_' + srcFileName
					tgtFileKey = prefix + '/' + '_' + tgtFileName
					logger.info('FileHide: srcFileKey: %s, tgtFileKey: %s', srcFileKey, tgtFileKey)
					s3.meta.client.copy({'Bucket' : bucketName, 'Key' : srcFileKey}, bucketName, tgtFileKey, ExtraArgs)
			logger.info('objectsToBeDeleted: %s', objectsToBeDeleted)
			if objectsToBeDeleted != []:
				client.delete_objects(Bucket = bucketName, Delete = {'Objects' : objectsToBeDeleted})


def copyFiles(bucketName, srcDirPath, tgtDirPath, awsRegionName):


	logger.info('Copying files from %s to %s', srcDirPath, tgtDirPath)

	client = boto3.client('s3', region_name = awsRegionName)
	s3 = boto3.resource('s3', region_name = awsRegionName)

	srcPrefix = srcDirPath.split(bucketName, 1)[1].strip('/')
	tgtPrefix = tgtDirPath.split(bucketName, 1)[1].strip('/')
	ExtraArgs = {'ServerSideEncryption': 'AES256'}
	paginator = client.get_paginator('list_objects_v2')
	page_iterator = paginator.paginate(Bucket = bucketName, Prefix = srcPrefix)
	for page in page_iterator:
		if 'Contents' in page:
			for content in page.get('Contents'):
				srcFileKey = content.get('Key')
				srcFileKeyWithoutSrcPrefix = srcFileKey.split(srcPrefix, 1)[1].lstrip('/')
				tgtFileKey = tgtPrefix + '/' + srcFileKeyWithoutSrcPrefix
				logger.info('FileCopy: srcFileKey: %s, tgtFileKey: %s', srcFileKey, tgtFileKey)
				s3.meta.client.copy({'Bucket' : bucketName, 'Key' : srcFileKey}, bucketName, tgtFileKey, ExtraArgs)


def createLocalDirectory(dirPath):

	logger.info('Creating directory: %s', dirPath)

	if os.path.isdir(dirPath) is False:
		os.makedirs(dirPath)


def writeFile(filePathLocal, fileContentList, writeMode = 'w'):

	logger.info('Writing local file: %s', filePathLocal)

	fileOpenFlag = False
	try:
		fileObj = open(filePathLocal, writeMode)
		fileOpenFlag = True
		for fileContent in fileContentList:
			fileObj.write(str(fileContent) + '\n')
		fileObj.close()
	finally:
		if fileOpenFlag is True: 
			fileObj.close()


def deleteFile(bucketName, filePath, awsRegionName):

	logger.info('Deleting s3 file: %s', filePath)

	client = boto3.client('s3', region_name = awsRegionName)

	objectsToBeDeleted = []
	fileKey = filePath.split(bucketName, 1)[1].strip('/')
	objectsToBeDeleted.append({'Key': fileKey})

	client.delete_objects(Bucket = bucketName, Delete = {'Objects' : objectsToBeDeleted})


def deleteLocalDirectory(dirPath):

	logger.info('Deleting local directory: %s', dirPath)

	if os.path.isdir(dirPath):
		shutil.rmtree(dirPath)
			

def deleteDirectory(bucketName, dirPath, awsRegionName, deleteOnlyHiddenFiles = False):

	logger.info('Deleting directory (deleteOnlyHiddenFiles: %s): %s', deleteOnlyHiddenFiles, dirPath)

	client = boto3.client('s3', region_name = awsRegionName)

	filesExistFlag = True
	prefix = dirPath.split(bucketName, 1)[1].strip('/')
	paginator = client.get_paginator('list_objects_v2')
	
	while filesExistFlag is True:
		filesExistFlag = False
		objectsToBeDeleted = []
		page_iterator = paginator.paginate(Bucket = bucketName, Prefix = prefix)
		for page in page_iterator:	
			if 'Contents' in page:
				for content in page.get('Contents'):
					fileKey = content.get('Key')
					fileName = fileKey.rsplit('/', 1)[-1]
					if deleteOnlyHiddenFiles is True and (fileName.startswith('.') is True or fileName.startswith('_') is True):
						objectsToBeDeleted.append({'Key': fileKey})
					elif deleteOnlyHiddenFiles is False:
						objectsToBeDeleted.append({'Key': fileKey})
				logger.info('objectsToBeDeleted: %s', objectsToBeDeleted)
				if objectsToBeDeleted != []:
					filesExistFlag = True
					client.delete_objects(Bucket = bucketName, Delete = {'Objects' : objectsToBeDeleted})
					objectsToBeDeleted = []
				else:
					filesExistFlag = False


def uploadFileToS3(filePathLocal, filePathS3, awsRegionName):

	logger.info('Uploading local file: ' + str(filePathLocal) + ' to ' + str(filePathS3))

	s3 = boto3.resource('s3', region_name = awsRegionName)
	
	bucket = filePathS3.split('/')[2]
	fileKey = '/'.join(filePathS3.split('/')[3:])
	extraArgs = {'ServerSideEncryption': 'AES256'}

	s3.meta.client.upload_file(filePathLocal, bucket, fileKey, extraArgs)


def getUpdateTableGreenSignal(filePath, bucketName, awsRegionName):

	dirPath, uniqueFileName = filePath.rsplit('/', 1)
	scoreId = uniqueFileName.rsplit('_', 1)[-1]
	prefix = dirPath.split(bucketName, 1)[1].strip('/')

	logger.info('Looking for concurrent Athena table update requests in %s', dirPath)

	client = boto3.client('s3', region_name = awsRegionName)
	paginator = client.get_paginator('list_objects_v2')
		
	while True:
		fileNameInfoList = []
		page_iterator = paginator.paginate(Bucket = bucketName, Prefix = prefix)
		for page in page_iterator:
			if 'Contents' in page:
				for content in page.get('Contents'):
					fileKey = content.get('Key')
					lastModifiedTime = content.get('LastModified').astimezone(pytz.utc)
					fileName = fileKey.rsplit('/', 1)[-1]
					fileScoreId = fileName.rsplit('_', 1)[-1]
					fileNameInfoList.append((fileScoreId, lastModifiedTime))

		sorted(fileNameInfoList, key = lambda x: (x[1], x[0]))
		logger.info('fileNameInfoList: %s', fileNameInfoList)
		if fileNameInfoList[0][0] == scoreId or (datetime.datetime.now().astimezone(pytz.utc) - fileNameInfoList[0][1]).total_seconds() > 3600:
			break

		time.sleep(5)


def performIsolationUpdate(isolationS3FilePath, createTempTableQuery, tempDirPath, curatedDataPath, athenaConfig):

	bucketName = athenaConfig['bucketName']
	awsRegionName = athenaConfig['awsRegionName']

	getUpdateTableGreenSignal(isolationS3FilePath, bucketName, awsRegionName)
	runAthenaQuery(createTempTableQuery, athenaConfig)
	hideFiles(bucketName, curatedDataPath, awsRegionName)
	copyFiles(bucketName, tempDirPath, curatedDataPath, awsRegionName)
	deleteDirectory(bucketName, tempDirPath, awsRegionName)
	deleteDirectory(bucketName, curatedDataPath, awsRegionName, deleteOnlyHiddenFiles = True)
	deleteFile(bucketName, isolationS3FilePath, awsRegionName)


def loadS3DataIntoAurora(dirPath, mcrsDeltaTableName, auroraConfig):

	delimiter = '~'
	psycopg2Config = {key: value for (key, value) in auroraConfig.items() if key in ('host', 'dbname', 'user', 'password', 'port')}

	logger.info('Loading data from %s to Aurora table %s', dirPath, mcrsDeltaTableName)

	s3 = s3fs.S3FileSystem(anon=False)
	s3FilePathList = s3.ls(dirPath)

	logger.info('s3FilePathList: %s', s3FilePathList)

	connection = psycopg2.connect(**psycopg2Config)
	cursor = connection.cursor()

	try:
		for s3FilePath in s3FilePathList:	
			with s3.open(s3FilePath, mode='rb') as fileObj:
				copyCommand = "COPY " + mcrsDeltaTableName + " FROM STDIN NULL AS '""' DELIMITER '" + delimiter + "'"
				logger.info('Copy Command: %s', copyCommand.replace('STDIN', s3FilePath))
				gzipFileObj = gzip.GzipFile(fileobj = fileObj)
				cursor.copy_expert(copyCommand, gzipFileObj)
				connection.commit()
	finally:
		cursor.close()
		connection.close()


def makeRestCallToApp(apiCsServerBaseUrl, appKey, appId, mcrsDeltaTableName):

	payload={}
	headers = {}
	url = apiCsServerBaseUrl + '/dataIntegration/event?' + \
			'appKey=' + appKey + \
			'&appId=' + appId + \
			'&srcname=RFME_DATA_SYNC' + \
			'&TempTable=' + mcrsDeltaTableName

	logger.info('Making GET call to URL: %s', url)
	response = requests.request("GET", url, headers=headers, data=payload)
	logger.info('Response Text: %s', response.text)


def main():

	global logger

	startTime = datetime.datetime.now()
	startTime = startTime.strftime('%Y-%m-%d %H:%M:%S')

	logger = setup_logger()

	logger.info('Sys Path: %s', sys.path)

	# util = AirflowUtils()
	# churnUtil = ChurnUtils()

	logger.info('Starting RFME Score Calculation')
	logger.info('startTime: %s', startTime)

	try:
		config = getConfig(); logConfig(config)
		modelInfo = getModelInfo(config); logModelInfo(modelInfo)
		athenaConfig = getAthenaConfig(config, modelInfo); logAthenaConfig(athenaConfig)
		auroraConfig = getAuroraConfig(config); logAuroraConfig(auroraConfig)
		rfmeScoreQueryMap = generateRfmeScoreQueryMap(modelInfo, startTime, config['scoreId'], config['mcrsDeltaTableName'], athenaConfig)
		runAthenaQuery(rfmeScoreQueryMap['mapCustRfmeSegmentQuery'], athenaConfig)

		createLocalDirectory(config['scoreLocalDirPath'])
		writeFile(athenaConfig['isolationDrsLocalFilePath'], [])
		writeFile(athenaConfig['isolationDrscLocalFilePath'], [])
		uploadFileToS3(athenaConfig['isolationDrsLocalFilePath'], athenaConfig['isolationDrsS3FilePath'], athenaConfig['awsRegionName'])
		uploadFileToS3(athenaConfig['isolationDrscLocalFilePath'], athenaConfig['isolationDrscS3FilePath'], athenaConfig['awsRegionName'])
		time.sleep(5)

		performIsolationUpdate(athenaConfig['isolationDrsS3FilePath'], rfmeScoreQueryMap['dimRfmeSegmentQuery'], athenaConfig['drsTempWriteDirPath'], athenaConfig['drsCuratedDataPath'], athenaConfig)
		performIsolationUpdate(athenaConfig['isolationDrscS3FilePath'], rfmeScoreQueryMap['dimRfmeSegmentScoreQuery'], athenaConfig['drscTempWriteDirPath'], athenaConfig['drscCuratedDataPath'], athenaConfig)

		runAthenaQuery(rfmeScoreQueryMap['mapCustRfmeSegmentAsCsvQuery'], athenaConfig)
		runAthenaQuery(rfmeScoreQueryMap['dropMapCustRfmeSegmentAsCsvQuery'], athenaConfig)
		runAuroraQuery(rfmeScoreQueryMap['createMcrsDeltaTableQuery'], auroraConfig, returnResults = False)
		loadS3DataIntoAurora(athenaConfig['mcrsTempWriteDirPath'], config['mcrsDeltaTableName'], auroraConfig)
		deleteDirectory(athenaConfig['bucketName'], athenaConfig['mcrsTempWriteDirPath'], athenaConfig['awsRegionName'])
		runAuroraQuery(rfmeScoreQueryMap['insertMcrsDeltaTableQuery'], auroraConfig, returnResults = False)
		runAuroraQuery(rfmeScoreQueryMap['deleteFromMcrsDeltaTableQuery'], auroraConfig, returnResults = False)
		# makeRestCallToApp(auroraConfig['apiCsServerBaseUrl'], auroraConfig['appKey'], auroraConfig['appId'], config['mcrsDeltaTableName'])
		
	finally:

		endTime = datetime.datetime.now()
		logger.info('endTime: %s', str(endTime))
		logger.info('Total Time: %s', str(endTime-datetime.datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S')))


		try: deleteDirectory(athenaConfig['bucketName'], athenaConfig['drsTempWriteDirPath'], athenaConfig['awsRegionName'])
		except Exception as e: logger.exception('message')
		try: deleteDirectory(athenaConfig['bucketName'], athenaConfig['drscTempWriteDirPath'], athenaConfig['awsRegionName'])
		except Exception as e: logger.exception('message')
		try: deleteDirectory(athenaConfig['bucketName'], athenaConfig['mcrsTempWriteDirPath'], athenaConfig['awsRegionName'])
		except Exception as e: logger.exception('message')


		try: deleteFile(athenaConfig['bucketName'], athenaConfig['isolationDrsS3FilePath'], athenaConfig['awsRegionName'])
		except Exception as e: logger.exception('message')
		try: deleteFile(athenaConfig['bucketName'], athenaConfig['isolationDrscS3FilePath'], athenaConfig['awsRegionName'])
		except Exception as e: logger.exception('message')

		try: deleteLocalDirectory(config['scoreLocalDirPath'])
		except Exception as e: logger.exception('message')

		try: runAthenaQuery(rfmeScoreQueryMap['dropDimRfmeSegmentQuery'], athenaConfig)
		except Exception as e: logger.exception('message')
		try: runAthenaQuery(rfmeScoreQueryMap['dropDimRfmeSegmentScoreQuery'], athenaConfig)
		except Exception as e: logger.exception('message')
		try: runAthenaQuery(rfmeScoreQueryMap['dropMapCustRfmeSegmentAsCsvQuery'], athenaConfig)
		except Exception as e: logger.exception('message')


if __name__ == '__main__':
	main()