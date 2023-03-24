import apache_beam as beam
from utils.variables import Variables
import datetime

p = beam.Pipeline(options=Variables.BEAM_OPTIONS.value)


def transaction_amount(file):
    file[3] = float(file[3])
    return file[3] > 20


def date_filter(file):
    file[0] = datetime.datetime.strptime(file[0], Variables.INPUT_DATE_STRING_FORMAT.value)
    return file[0] > datetime.datetime.strptime(Variables.EXCLUDE_BEFORE_DATE.value, '%Y')


def date_to_str(file):
    file[0] = datetime.datetime.strftime(file[0], Variables.OUTPUT_DATE_STRING_FORMAT.value)
    return file[0]


(
 p
 | 'read_file_content' >> beam.io.ReadFromText(Variables.GCS_URI.value, skip_header_lines=1)
 | 'split_csv_file' >> beam.Map(lambda line: line.split(","))
 | 'filter_by_transaction_amount' >> beam.Filter(transaction_amount)
 | 'filter_by_date' >> beam.Filter(date_filter)
 | 'convert_date_to_str' >> beam.Filter(date_to_str)
 | 'remove_unwanted_values' >> beam.Map(lambda line: line[0::3])
 | 'sum_values_per_date' >> beam.CombinePerKey(sum)
 | 'format_csv_file' >> beam.Map(lambda row: ', '.join([""+ str(column) +"" for column in row]))
 | 'write_to_csv' >> beam.io.WriteToText('output/results.jsonl.gz', header='date, total_amount',
                                         shard_name_template='')
)

p.run().wait_until_finish()
