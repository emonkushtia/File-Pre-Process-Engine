import itertools
import operator

from Babel import rds_provider, EntityType
from pre_process_script_interface import IPreProcessScript, PreProcessOrder


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16918
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.diagnosis_codes_map = []
        self.claim_type_code_index = None
        self.claim_id_index = None
        self.claim_line_number_index = None

    @property
    def order(self):
        return PreProcessOrder.ClaimDiagnosisFlattening.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableClaimDiagnosisFlattening')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)

        if IPreProcessScript.to_boolean(configurations['EnableClaimDiagnosisFlattening']) is False:
            return False

        self.claim_id_index = self.map_columns.where(lambda x: x['redshift_name'] == 'claim_id_medical_claim_header').first()['index']
        claim_type_code_field = self.map_columns.where(lambda x: x['redshift_name'] == 'medical_claim_type_code_medical_claims_header').first_or_default()
        if claim_type_code_field is None:
            return False
        self.claim_type_code_index = claim_type_code_field['index']

        claim_line_number_field = self.map_columns.where(lambda x: x['redshift_name'] == 'claim_line_number_medical_claims_detail').first_or_default()
        if claim_line_number_field is None:
            return False
        self.claim_line_number_index = claim_line_number_field['index']

        return True

    def prepare_data(self):
        diagnosis_code_fields = ['diagnosis_code_primary_medical_claims_header']
        diagnosis_code_fields.extend(['diagnosis_code_{}_medical_claims_header'.format(x) for x in range(1, 25)])
        for diagnosis_code_field in diagnosis_code_fields:
            diagnosis_code_map = self.map_columns.where(lambda x: x['redshift_name'] == diagnosis_code_field).first_or_default()
            if diagnosis_code_map:
                self.diagnosis_codes_map.append((diagnosis_code_map['index'], diagnosis_code_map['redshift_name']))

    def process_rows(self, rows):
        new_rows = []
        for claim_id, claims_lines in itertools.groupby(rows, key=lambda x: x[self.claim_id_index]):
            new_claims_lines = []
            for claim_line in sorted(claims_lines, key=operator.itemgetter(self.claim_line_number_index)):
                if claim_line[self.claim_type_code_index] == 'I':
                    new_rows.append(claim_line)
                    continue
                new_claims_lines.append(claim_line)
            if len(new_claims_lines) == 0:
                continue

            primary_diagnosis_code_value, other_diagnosis_code_values = self.get_distinct_sorted_diagnosis_codes(new_claims_lines)
            diagnosis_codes_map_values = self.get_diagnosis_codes_map_values(primary_diagnosis_code_value, other_diagnosis_code_values)
            for claim_line in new_claims_lines:
                for diagnosis_code_index, diagnosis_code_value in diagnosis_codes_map_values:
                    claim_line[diagnosis_code_index] = diagnosis_code_value
                new_rows.append(claim_line)
        return new_rows

    def get_diagnosis_codes_map_values(self, primary_diagnosis_code_value, other_diagnosis_code_values):
        diagnosis_codes_map_values = []
        other_diagnosis_value_index = 0
        for diagnosis_code_field_index, redshift_name in self.diagnosis_codes_map:
            if redshift_name == 'diagnosis_code_primary_medical_claims_header':
                diagnosis_codes_map_values.append((diagnosis_code_field_index, primary_diagnosis_code_value))
            else:
                diagnosis_codes_map_values.append((diagnosis_code_field_index, other_diagnosis_code_values[other_diagnosis_value_index]))
                other_diagnosis_value_index += 1
        return diagnosis_codes_map_values

    def get_distinct_sorted_diagnosis_codes(self, claims_lines):
        primary_diagnosis_code_value = ''
        other_diagnosis_code_values = set()
        for claim in claims_lines:
            for diagnosis_code_field_index, redshift_name in self.diagnosis_codes_map:
                code_value = claim[diagnosis_code_field_index].strip()
                if code_value == '':
                    continue

                if redshift_name == 'diagnosis_code_primary_medical_claims_header' and primary_diagnosis_code_value == '':
                    primary_diagnosis_code_value = code_value
                else:
                    other_diagnosis_code_values.add(code_value)

        other_diagnosis_code_values.discard(primary_diagnosis_code_value)  # Remove the primary_diagnosis_code.
        other_diagnosis_code_values = list(other_diagnosis_code_values)
        other_diagnosis_code_values.sort()

        other_diagnosis_code_values.extend(itertools.repeat('', len(self.diagnosis_codes_map) - len(other_diagnosis_code_values)))  # Padding '' to ensure the the array size = diagnosis_codes_map size

        return primary_diagnosis_code_value, other_diagnosis_code_values
