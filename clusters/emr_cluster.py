# encoding: utf-8

import logging
import time
import boto3
import botocore
import subprocess
import random
from config import conf
from datetime import datetime


def setup_logging(default_level=logging.WARNING):
    logging.basicConfig(level=default_level)
    return logging.getLogger('EMRCLuster')


def terminate(error_message=None):
    if error_message:
        logger.error(error_message)
    logger.critical('The script is now terminating')
    exit()


class EMRCluster:

    def __init__(self):
        self.conf = conf

        self.app_name = "BaseCluster"

        self.emr_release_label = conf.get("emr_release")

        self.ec2_key_name = conf.get("ec2_key_name")
        self.aws_profile_name = conf.get("aws_profile")

        self.subnets = conf.get("subnets")
        self.subnet_id = random.choice(self.subnets)

        # Filled by generate_job_name()
        self.job_name = None

        # Returned by AWS in start_spark_cluster()
        self.job_flow_id = None

        # Path of Spark script to be deployed on AWS Cluster
        self.path_script = "../app/"

        # S3 Bucket which store analysis data, EMR logs and temporary files
        # Filled by generate s3_bucket()
        self.s3_bucket_analysis =conf.get("s3_bucket").get("analysis_bucket")
        self.s3_bucket_logs = conf.get("s3_bucket").get("logs_bucket")
        self.s3_bucket_temp_files = conf.get("s3_bucket").get("temp_files_bucket")

        # EMR Tags
        self.tags = {}

    def run(self):
        session = boto3.session.Session(profile_name=self.aws_profile_name)

        s3 = session.resource('s3')
        self.generate_job_name()

        # Check if S3 bucket to store temporary files in exists
        self.temp_bucket_exists(s3)

        # Tar the Python Spark script
        self.zip_python_script()

        # Move the Spark files to a S3 bucket for temporary files
        self.upload_temp_files(s3)

        # Open EMR connection
        c = session.client('emr')

        # Start Spark EMR cluster
        self.start_spark_cluster(c)

        # Add step 'spark-submit'
        self.step_spark_submit(c, '')

        # Set EMR tags
        self.add_cluster_tags(c)

        # Describe cluster status until terminated
        # self.describe_status_until_terminated(c)
        # self.remove_temp_files(s3)

    def generate_job_name(self):
        self.job_name = "{}.{}".format(self.app_name, datetime.now().strftime("%Y%m%d.%H%M%S.%f"))

    def temp_bucket_exists(self, s3):
        try:
            s3.meta.client.head_bucket(Bucket=self.s3_bucket_temp_files)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            print(error_code)
            if error_code == 404:
                terminate("Bucket for temporary files does not exist")
            terminate("Error while connecting to Bucket")
        logger.info("S3 bucket for temporary files exists")

    def zip_python_script(self):
        args = ["pushd ../app/; zip -r modules.zip modules -x '*.pyc' -x '*__pycache__*'; popd"]
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        proc.communicate()

    def upload_temp_files(self, s3):
        s3.Object(self.s3_bucket_temp_files, self.job_name + '/setup.sh')\
          .put(Body=open('./init/setup.sh', 'rb'), ContentType='text/x-sh')

        s3.Object(self.s3_bucket_temp_files, self.job_name + '/requirements.txt')\
          .put(Body=open('./init/requirements.txt', 'rb'), ContentType='text/x-sh')

        s3.Object(self.s3_bucket_temp_files, self.job_name + '/modules.zip')\
          .put(Body=open('../app/modules.zip', 'rb'), ContentType='application/zip')

        logger.info("Uploaded files to folder '{}' in bucket '{}'".format(
            self.job_name, self.s3_bucket_temp_files))

        return True

    def remove_temp_files(self, s3):
        bucket = s3.Bucket(self.s3_bucket_temp_files)
        for key in bucket.objects.all():
            if key.key.startswith(self.job_name) is True:
                key.delete()
                logger.info("Removed '{}' from bucket for temporary files".format(key.key))

    def start_spark_cluster(self, c):
        response = c.run_job_flow(
            Name=self.job_name,
            LogUri="s3://{}/".format(self.s3_bucket_logs),
            ReleaseLabel=self.emr_release_label,
            Instances={
                'InstanceFleets': [
                    {
                        "InstanceFleetType": "MASTER",
                        "TargetOnDemandCapacity": 1,
                        "InstanceTypeConfigs": [
                            {
                                "InstanceType": "m5.xlarge"
                            }
                        ]
                    },
                    {
                        "InstanceFleetType": "CORE",
                        "TargetSpotCapacity": 64,
                        "LaunchSpecifications": {
                            "SpotSpecification": {
                                "TimeoutDurationMinutes": 5,
                                "TimeoutAction": "SWITCH_TO_ON_DEMAND"
                            }
                        },
                        "InstanceTypeConfigs": [
                            {
                                "InstanceType": "r5.4xlarge",
                                "WeightedCapacity": 16,
                                'EbsConfiguration': {
                                    'EbsBlockDeviceConfigs': [
                                        {
                                            'VolumeSpecification': {
                                                'VolumeType': 'gp2',
                                                'SizeInGB': 50
                                            },
                                        }
                                    ]
                                }
                            },
                            {
                                "InstanceType": "r5.8xlarge",
                                "WeightedCapacity": 32,
                                'EbsConfiguration': {
                                    'EbsBlockDeviceConfigs': [
                                        {
                                            'VolumeSpecification': {
                                                'VolumeType': 'gp2',
                                                'SizeInGB': 50
                                            },
                                        }
                                    ]
                                }
                            },
                        ]
                    }
                ],
                'Ec2KeyName': self.ec2_key_name,
                'KeepJobFlowAliveWhenNoSteps': False, 
                'Ec2SubnetId': self.subnet_id,
            },
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'},
                {'Name': 'Ganglia'}
            ],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            BootstrapActions=[
                {
                    'Name': 'setup',
                    'ScriptBootstrapAction': {
                        'Path': 's3://{}/{}/setup.sh'.format(self.s3_bucket_temp_files, self.job_name),
                        'Args': [
                            's3://{}/{}/requirements.txt'.format(
                                self.s3_bucket_temp_files,
                                self.job_name),
                        ]
                    }
                }
            ],
            Configurations=[
                {
                    "Classification": "hadoop-env", 
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Configurations": [],
                            "Properties": {
                                "JAVA_HOME": "/etc/alternatives/jre"
                            }
                        }
                    ],
                    "Properties": {}
                },
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Configurations": [],
                            "Properties": {
                                "JAVA_HOME": "/etc/alternatives/jre"
                            }
                        }
                    ], 
                    "Properties": {}
                },
                {
                    "Classification": "yarn-site",
                    "Properties": {
                        "yarn.nodemanager.vmem-check-enabled": "false"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.default.parallelism": "180",
                        "spark.driver.memoryOverhead": "4000M",
                        "spark.driver.maxResultSize": "4g",
                        "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:OnOutOfMemoryError='kill -9 %p'",
                        "spark.dynamicAllocation.enabled": "false",
                        "spark.executor.heartbeatInterval": "4500s",
                        "spark.executor.memoryOverhead": "4000M",
                        "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:OnOutOfMemoryError='kill -9 %p'",
                        "spark.memory.fraction": "0.80",
                        "spark.memory.storageFraction": "0.30",
                        "spark.network.timeout": "8000s",
                        "spark.rdd.compress": "true",
                        "spark.shuffle.compress": "true",
                        "spark.shuffle.spill.compress": "true",
                        "spark.storage.level": "MEMORY_AND_DISK_SER",
                        "spark.yarn.scheduler.reporterThread.maxFailures": "5",
                    }
                }
            ],
        )

        # Process response to determine if Spark cluster was started, and if so, the JobFlowId of the cluster
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.job_flow_id = response['JobFlowId']
        else:
            terminate(
                "Could not create EMR cluster (status code {})".format(response_code))

        logger.info(
            "Created Spark {} cluster with JobFlowId {}".format(self.emr_release_label, self.job_flow_id))

    def describe_status_until_terminated(self, c):
        stop = False
        while stop is False:
            description = c.describe_cluster(ClusterId=self.job_flow_id)
            state = description['Cluster']['Status']['State']
            if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
                stop = True
            logger.info(state)
            # Prevent ThrottlingException by limiting number of requests
            time.sleep(30)

    def step_spark_submit(self, c, arguments):
        return True

    def add_cluster_tags(self, c):
        if len(self.tags) == 0:
            return True
        cluster_tags = [{'Key': str(key), 'Value': str(value)} for key, value in self.tags.items()]
        c.add_tags(
            ResourceId=self.job_flow_id,
            Tags=cluster_tags
        )


logger = setup_logging(logging.INFO)
