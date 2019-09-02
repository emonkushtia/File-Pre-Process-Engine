from Babel import rds_provider
from pre_process_script_interface import IPreProcessScript, PreProcessOrder


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BAPCD-690 , BAPCD-695
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.procedure_code_index = None

    @property
    def order(self):
        return PreProcessOrder.DentalOrMedicalClaimExtractor.value

    def will_execute(self):
        if self.entity_type not in ['Claim', 'DentalClaim']:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableDentalAndMedicalClaimExtractor')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableDentalAndMedicalClaimExtractor']) is False:
            return False

        if self.entity_type == 'DentalClaim':
            procedure_code = self.map_columns.where(lambda x: x['redshift_name'] == 'procedure_code_dental_claim_detail').first_or_default()
        else:
            procedure_code = self.map_columns.where(lambda x: x['redshift_name'] == 'procedure_code_medical_claims_detail').first_or_default()
        if procedure_code is None:
            return False

        self.procedure_code_index = procedure_code['index']
        return True

    def prepare_data(self):
        pass

    def process_rows(self, rows):
        new_rows = []
        for row in rows:
            procedure_code_value = str(row[self.procedure_code_index])
            if self.entity_type == 'DentalClaim' and procedure_code_value.startswith('D'):
                new_rows.append(row)
            elif self.entity_type == 'Claim' and not procedure_code_value.startswith('D'):
                new_rows.append(row)
        return new_rows
