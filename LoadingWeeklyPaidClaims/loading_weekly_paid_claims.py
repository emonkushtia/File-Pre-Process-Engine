from datetime import datetime

from Babel import rds_provider, EntityType, redshift_provider, print_message
from pre_process_script_interface import IPreProcessScript, PreProcessOrder, FileLoadException

date_formats = [
    '%Y%m%d',
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%m%d%Y',
    '%m-%d-%Y',
    '%m/%d/%Y',
    '%d%m%Y',
    '%d-%m-%Y',
    '%d/%m/%Y'
]


def parse_date(date):
    if date == '':
        return None
    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date, date_format)
            return parsed_date
        except:
            pass
    return None


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16695, https://babelhealth.atlassian.net/browse/BBL-17363
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.max_paid_date = None
        self.paid_date_index = None
        self.any_records_found = False

    @property
    def order(self):
        return PreProcessOrder.LoadingWeeklyPaidClaims.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableLoadingWeeklyPaidClaims')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableLoadingWeeklyPaidClaims']) is False:
            return False

        paid_date_field = self.map_columns.where(lambda x: x['redshift_name'] == 'paid_date_medical_claims_header').first_or_default()
        if paid_date_field:
            self.paid_date_index = paid_date_field['index']
            return True
        return False

    def prepare_data(self):
        data = redshift_provider.fetch_all('''SELECT paid_date_medical_claims_header FROM medical_claims_header WHERE paid_date_medical_claims_header NOT SIMILAR TO  '[0-9]{8}' LIMIT 1;''')
        if len(data) > 0:
            raise FileLoadException('Different formats paid date has been found in claim database while executing loading weekly paid claims pre process.')

        data = redshift_provider.fetch_all('SELECT MAX(paid_date_medical_claims_header) FROM medical_claims_header;')
        self.max_paid_date = parse_date(data[0][0])
        print_message('Max paid date for weekly paid claims is: {}'.format(self.max_paid_date))

    def process_rows(self, rows):
        if self.max_paid_date is None:
            self.any_records_found = True
            return rows

        new_rows = []
        for row in rows:
            paid_date = parse_date(row[self.paid_date_index])
            if paid_date and paid_date > self.max_paid_date:
                self.any_records_found = True
                new_rows.append(row)
        return new_rows

    def post_processing_tasks(self):
        if self.any_records_found:
            return
        raise FileLoadException('Weekly paid claims pre process does not satisfy any records.')


