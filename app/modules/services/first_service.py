import gc
from .base_service import BaseService


class FirstService(BaseService):
    def __init__(self, context, sqlContext, name, input_path, output_path):
        super().__init__(context, sqlContext, name)
        self.input_path = input_path
        self.output_path = output_path

    def _run(self):
        df = self._readCsv(self.input_path)
        df.show(10)
        self._exportParquet(df, self.output_path)

        del [[df]]
        gc.collect()
