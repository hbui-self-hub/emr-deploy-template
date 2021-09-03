# emr-deploy-template
template for deploying PySpark application to EMR


## Software Require

1. python3
2. Python packages which is described in `clusters/init/requirements.txt`
## Project Structure

|Folder|Explain|
|---|---|
|app|Pyspark Application|
|cluster|EMR setting|
|config|Config for this project|
|scripts|shell file to deploy application to emr or run pyspark application in local|


## Environmental variables

### .env 

|Variables|Explain|
|---|---|
|EMR_ENVIRONMENT|enviroment for deploy, choose 1 of following for corresponding environment: development, staging, product|
|EMR_APP_PATH|Path to `app` folder|
|EMR_CONFIG_PATH|Path to `config` folder|

### config file

|File|Variables|Explain| |
|---|---|---|---|
|app|app_name|app name|current not use|
|   |company|company name|current not use|
|   |analysis_data|path to analysis data folder (assuming to save in hdfs or s3) |current not use|
|   |result_data|path to analysed data folder(assuming to save in hdfs or s3) |current not use|
|aws|emr_release|emr release version||
|   |ec2_key_name|ec2 key, assigned to emr's instance|
|   |aws_profile|profile which is setup by awscli|
|   |s3_bucket.analysis_bucket|s3 bucket which keep source data and analysed data|
|   |s3_bucket.logs_bucket|s3 bucket which keep emr logs|
|   |s3_bucket.temp_files_bucket|s3 bucket which keep emr setting and source code|

## How to deploy

1. setup aws account using awscli
1. insert all info to .env file and config file
1. access folder `scripts`
1. run `run_application_emr.sh` 