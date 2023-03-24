from enum import Enum
from apache_beam.pipeline import PipelineOptions


class Variables(Enum):
    BEAM_OPTIONS = PipelineOptions(runner='DirectRunner',)

    GCS_URI = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'

    INPUT_DATE_STRING_FORMAT = r"%Y-%m-%d %H:%M:%S UTC"

    EXCLUDE_BEFORE_DATE = '2010'

    OUTPUT_DATE_STRING_FORMAT = r"%Y-%m-%d"
