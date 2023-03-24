import apache_beam as beam
from utils.variables import Variables
import datetime
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def transaction_amount(file):
    file[3] = float(file[3])
    return file[3] > 20


def date_filter(file):
    file[0] = datetime.datetime.strptime(file[0], Variables.INPUT_DATE_STRING_FORMAT.value)
    return file[0] > datetime.datetime.strptime(Variables.EXCLUDE_BEFORE_DATE.value, '%Y')


def date_to_str(file):
    file[0] = datetime.datetime.strftime(file[0], Variables.OUTPUT_DATE_STRING_FORMAT.value)
    return file[0]


def test_count_words():
    with TestPipeline() as p:

        test_data = ('2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
                     '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
                     '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                     '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                     '2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08',
                     '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12')

        test_pipeline_input = p | beam.Create(test_data)

        composite_transform = (
         test_pipeline_input
         | 'split_csv_file' >> beam.Map(lambda line: line.split(","))
         | 'filter_by_transaction_amount' >> beam.Filter(transaction_amount)
         | 'filter_by_date' >> beam.Filter(date_filter)
         | 'convert_date_to_str' >> beam.Filter(date_to_str)
         | 'remove_unwanted_values' >> beam.Map(lambda line: line[0::3])
         | 'sum_values_per_date' >> beam.CombinePerKey(sum)
         | 'format_csv_file' >> beam.Map(lambda row: ', '.join([""+ str(column) +"" for column in row]))
        )

        assert_that(
            composite_transform,
            equal_to(['2017-03-18, 2102.22', '2017-08-31, 13700000023.08', '2018-02-27, 129.12']))


test_count_words()

