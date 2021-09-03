import datetime

DATE_FORMAT = '%Y-%m-%d'
DATE_FOLDER_FORMAT = '%Y%m%d'

def date_format(date, f='%Y-%m-%d'):
    return date.strftime(f)

def date_folder_format(date):
    return date.strftime(DATE_FOLDER_FORMAT)