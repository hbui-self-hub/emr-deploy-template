import logging
import time

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

class BaseService:
    def __init__(self, context, sqlContext, name):
        self.context = context
        self.sqlContext = sqlContext
        self.name = name
        self.successful = False

    def execute(self):
        start_time = time.time()
        logging.info("start {}".format(self.name))
        
        self._run()

        elapsed_time = time.time() - start_time
        logging.info("{} completed. {:.2f} seconds elapsed.".format(self.name, elapsed_time))
        self.successful = True
    
    def _run(self):
        pass
    
    def _readParquet(self, input_uri):
        return self.sqlContext.read.parquet(input_uri)
    def _readCsv(self, input_uri, sep=',', header=True):
        return self.sqlContext.read.csv(input_uri, sep=sep, header=header)
    
    def _exportParquet(self, df, output_uri, mode="overwrite", coalesce=500):
        df.coalesce(coalesce).write.mode(mode).parquet(output_uri, compression='snappy')
    def _exportCsv(self, df, output_uri, mode="overwrite", coalesce=500, sep="\t"):
        df.coalesce(coalesce).write.mode(mode).csv(output_uri, compression='bzip2', sep=sep)
    def _exportCsvFile(self, df, output_filepath):
        df.toPandas().reset_index().drop_duplicates(subset='index', keep='first').set_index('index').to_parquet(output_filepath, index=False)