from Babel import rds_provider, EntityType
from pre_process_script_interface import IPreProcessScript, PreProcessOrder


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16731
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.diagnosis_pointers_map = []
        self.diagnosis_codes_map = []
        self.claim_type_code_index = None

    @property
    def order(self):
        return PreProcessOrder.PopulatingDiagnosisPointer.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnablePopulatingDiagnosisPointer')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnablePopulatingDiagnosisPointer']) is False:
            return False

        is_chart_review = self.file_name.startswith('ChartReview_Merged_')
        if is_chart_review:
            return False
        claim_type_code_field = self.map_columns.where(lambda x: x['redshift_name'] == 'medical_claim_type_code_medical_claims_header').first_or_default()
        if claim_type_code_field is None:
            return False
        self.claim_type_code_index = claim_type_code_field['index']

        return True

    def prepare_data(self):
        diagnosis_code_pointer_fields = ['professional_service_diagnosis_code_pointer_{}_medical_claims_detail'.format(x) for x in range(1, 5)]
        self.diagnosis_pointers_map = self.map_columns.where(lambda x: x['redshift_name'] in diagnosis_code_pointer_fields).to_list()
        diagnosis_code_fields = ['diagnosis_code_primary_medical_claims_header']
        diagnosis_code_fields.extend(['diagnosis_code_{}_medical_claims_header'.format(x) for x in range(1, 12)])
        for diagnosis_code_position, diagnosis_code_field in enumerate(diagnosis_code_fields):
            diagnosis_code_position += 1
            diagnosis_code_map = self.map_columns.where(lambda x: x['redshift_name'] == diagnosis_code_field).first_or_default()
            if diagnosis_code_map:
                self.diagnosis_codes_map.append((diagnosis_code_position, diagnosis_code_map['index'], diagnosis_code_map['redshift_name']))

    def process_rows(self, rows):
        for row in rows:
            if row[self.claim_type_code_index] == 'I':
                continue

            for diagnosis_pointer_map in self.diagnosis_pointers_map:
                diagnosis_pointer_value = row[diagnosis_pointer_map['index']].strip()
                if diagnosis_pointer_value == '':
                    continue
                for diagnosis_code_position, diagnosis_code_field_index, redshift_name in self.diagnosis_codes_map:
                    if row[diagnosis_code_field_index].strip() == diagnosis_pointer_value:
                        row[diagnosis_pointer_map['index']] = diagnosis_code_position
                        break
        return rows
