import subprocess
import boto3
import os

def file_exists(uri):
    code = run_cmd(['hadoop', 'fs', '-test', '-e', uri])
    return code == 0


def s3_delete_uri(uri):
    code = run_cmd(['hadoop', 'dfs', '-rm', '-rf', uri])
    return code == 0


def s3_file_count(dir, regex):
    code, count = run_cmd_result(['hadoop', 'fs', '-ls', '-C', dir, '| grep -c -E', '"{}"'.format(regex)])
    return code == 0, count


def s3_uri(prefix, bucket):
    return 's3://{}/{}'.format(bucket, prefix)


def s3_list_files(prefix, bucket, include_path=False):
    s3 = boto3.client('s3')
    objs = s3.list_objects(Bucket=bucket, Prefix=prefix)
    files = []
    if 'Contents' not in objs:
        return files
    for obj in objs['Contents']:
        if include_path:
            filepath = obj['Key'].replace(prefix, '')
            files.append(filepath)
        else:
            filename = os.path.split(obj['Key'])[1]
            if filename is None or filename == '':
                continue
            files.append(filename)
    return files


def s3_file_exists(s3, prefix, bucket):
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    for obj in response.get('Contents', []):
        if obj['Key'] == prefix:
            return True
    return False


def hdfs_uri(hdfs_path, prefix):
    return 'hdfs:///{}/{}'.format(hdfs_path, prefix)


def run_cmd(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.communicate()
    return proc.returncode


def run_cmd_result(args):
    cmd = ' '.join(args)
    proc = subprocess.Popen((cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = proc.communicate()
    return proc.returncode, result[0].decode('utf-8').strip()