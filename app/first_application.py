import os
import argparse
import traceback
import logging

from concurrent.futures import ThreadPoolExecutor
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from modules.commons import s3_uri, conf
from modules.services import FirstService

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

def execute_service(service):
    try:
        service.execute()
    except Exception:
        logging.error('{} failed'.format(service.name))
        traceback.print_exc()

def execute(context, sqlContext, args):
    services = []
    logging.info('start application')

    with ThreadPoolExecutor(max_workers=args.jobs) as executor:

        for i in range(args.run_times):
            input_uri = s3_uri("emr-deploy-template/dataset/airbnb_tokyo_demo.csv", conf.get("s3_bucket").get("analysis_bucket"))
            output_uri = s3_uri("emr-deploy-template/analysis/{}".format(i), conf.get("s3_bucket").get("analysis_bucket"))

            services.append(FirstService(
                context=context,
                sqlContext=sqlContext,
                name='read data airbnb tokyo',
                input_path=input_uri,
                output_path=output_uri,
                ))

        executor.map(execute_service, services)

    errors = list(map(lambda p: p.name, filter(lambda p: not p.successful, services)))

    if errors:
        raise Exception('following jobs failed:\n  ' + '\n  '.join(errors))

    logging.info('end application')


# ------------------------------------------------------------------------------
def readArgs():
    # 引数の解析
    parser = argparse.ArgumentParser(description='Read AirBnb Data split by date')
    parser.add_argument('--run-times', metavar='run_times', type=int, default=10, help='Number of times application to be executed in parallel mode (default 10)')
    parser.add_argument('--jobs', metavar='jobs', type=int, default=2, help='number of jobs to be executed simultaneously (default 2)')
    args = parser.parse_args()
    return args

# 実行
args = readArgs()

sparkConf = SparkConf().setAppName('First Application')
# conf = SparkConf().setMaster('yarn-cluster').setAppName('First Application')
context = SparkContext(conf=sparkConf).getOrCreate()
sqlContext = SQLContext(context)

execute(context, sqlContext, args)
