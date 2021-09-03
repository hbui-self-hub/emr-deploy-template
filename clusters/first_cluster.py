import argparse
from emr_cluster import EMRCluster
from datetime import datetime, timedelta

class FirstCluster(EMRCluster):
    def __init__(self):
        super().__init__()
        self.args = read_args()
        self.script_paths = {
            '../app/first_application.py': 'first_application.py',
        }
        self.tags = {
            'billing-to': 'demo-emr-deploy',
            'batch': 'first_cluster',
            'exec-date': '{:%Y%m%d}'.format(datetime.now().date()),
        }

    def upload_temp_files(self, s3):
        super().upload_temp_files(s3)
        for path, filename in self.script_paths.items():
            s3.Object(self.s3_bucket_temp_files, self.job_name + '/{}'.format(filename)).put(Body=open(path, 'rb'), ContentType='text/x-sh')

    def generate_job_name(self):
        self.job_name = "First_cluster.{}".format(datetime.now().strftime("%Y%m%d.%H%M%S.%f"))

    def step_spark_submit(self, c, arguments):
        if not self.args.run_times:
            return False
        
        emr_args = [
            '/usr/lib/spark/bin/spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'cluster',
            '--driver-cores', '16',
            '--driver-memory', '30g',
            '--executor-cores', '5',
            '--executor-memory', '30g',
            '--conf', 'spark.yarn.maxAppAttempts=1',
            '--conf', 'spark.pyspark.python=/usr/bin/python3',
            '--py-files',
            's3://{}/{}/modules.zip'.format(self.s3_bucket_temp_files, self.job_name),
            's3://{}/{}/first_application.py'.format(self.s3_bucket_temp_files, self.job_name),
            '--run-times', '{}'.format(self.args.run_times),
            '--jobs', '{}'.format(self.args.jobs),
        ]

        c.add_job_flow_steps(
            JobFlowId= self.job_flow_id,
            Steps=[{
                'Name': 'First Step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Args': emr_args,
                    'Jar': 'command-runner.jar'
                }
            }]
        )

        return True


def read_args():
    # 引数の解析
    parser = argparse.ArgumentParser(description='First Cluster')
    parser.add_argument('--run-times', metavar='run_times', type=int, default=10, help='Number of times application to be executed in parallel mode (default 10)')
    parser.add_argument('--jobs', metavar='jobs', type=int, default=2, help='number of jobs to be executed simultaneously (default 2)')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    FirstCluster().run()