from Babel import rds_provider, EntityType
from pre_process_script_interface import IPreProcessScript, PreProcessOrder


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-17002
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.provider_type_code_index = None
        self.place_of_service_code_index = None
        self.type_of_bill_code_index = None

    @property
    def order(self):
        return PreProcessOrder.ClaimTobPosPopulation.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableChartPOSTOBPopulation')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableChartPOSTOBPopulation']) is False:
            return False
        is_chart_review = rds_provider.fetch_one('SELECT IsChartReview FROM ControlFiles WHERE Id={}'.format(self.control_file_id))[0]
        if is_chart_review is False:
            return False

        provider_type_field = self.map_columns.where(lambda x: x['redshift_name'] == 'medical_claim_type_code_medical_claims_header').first_or_default()
        if provider_type_field is None:
            return False
        self.provider_type_code_index = provider_type_field['index']

        place_of_service_code_field = self.map_columns.where(lambda x: x['redshift_name'] == 'place_of_service_code_medical_claims_header').first_or_default()
        if place_of_service_code_field is not None:
            self.place_of_service_code_index = place_of_service_code_field['index']

        type_of_bill_code_field = self.map_columns.where(lambda x: x['redshift_name'] == 'type_of_bill_code_medical_claims_header').first_or_default()
        if type_of_bill_code_field is not None:
            self.type_of_bill_code_index = type_of_bill_code_field['index']

        if place_of_service_code_field is None and type_of_bill_code_field is None:
            return False

        return True

    def prepare_data(self):
        pass

    def process_rows(self, rows):
        for row in rows:
            provider_type_code = row[self.provider_type_code_index].strip()
            if self.place_of_service_code_index and row[self.place_of_service_code_index].strip() == '':
                if provider_type_code in ['01', '02', '10']:
                    row[self.place_of_service_code_index] = ''
                elif provider_type_code in ['20']:
                    row[self.place_of_service_code_index] = '11'

            if self.type_of_bill_code_index:
                if provider_type_code in ['20']:
                    row[self.type_of_bill_code_index] = ''
                elif provider_type_code in ['01', '02']:
                    row[self.type_of_bill_code_index] = '111'
                elif provider_type_code in ['10']:
                    row[self.type_of_bill_code_index] = '131'

        return rows
