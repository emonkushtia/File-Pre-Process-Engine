import re
from Babel import rds_provider, EntityType, print_message
from pre_process_script_interface import IPreProcessScript, PreProcessOrder


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16970
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.frequency_code_index = None
        self.type_of_bill_code_index = None
        self.claim_id_index = None
        self.link_claim_id_index = None
        self.linked_claim_replacement_dic = {}

    @property
    def order(self):
        return PreProcessOrder.LinkedClaimPopulationFromVersionedClaimId.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableLinkedClaimFromVersionedClaimId')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableLinkedClaimFromVersionedClaimId']) is False:
            return False

        return self.has_required_column()

    def has_required_column(self):
        frequency_code_field = self.map_columns.where(lambda x: x['redshift_name'] == 'frequency_code_medical_claims_header').first_or_default()
        type_of_bill_code_field = self.map_columns.where(lambda x: x['redshift_name'] == 'type_of_bill_code_medical_claims_header').first_or_default()
        claim_id_field = self.map_columns.where(lambda x: x['redshift_name'] == 'claim_id_medical_claim_header').first_or_default()
        link_claim_id_field = self.map_columns.where(lambda x: x['redshift_name'] == 'linked_claimid_medical_claims_header').first_or_default()

        if frequency_code_field is None:
            print_message('LinkedClaimIdPopulationFromVersioned pre-process service stopped because mandatory column "frequency_code_medical_claims_header" is missing in the map.')
            return False

        if type_of_bill_code_field is None:
            print_message('LinkedClaimIdPopulationFromVersioned pre-process service stopped because mandatory column "type_of_bill_code_medical_claims_header" is missing in the map.')
            return False

        if claim_id_field is None:
            print_message('LinkedClaimIdPopulationFromVersioned pre-process service stopped because mandatory column "claim_id_medical_claim_header" is missing in the map.')
            return False

        if link_claim_id_field is None:
            print_message('LinkedClaimReplacement pre-process service stopped because mandatory column "linked_claimid_medical_claims_header" is missing in the map.')
            return False

        self.frequency_code_index = frequency_code_field['index']
        self.type_of_bill_code_index = type_of_bill_code_field['index']
        self.claim_id_index = claim_id_field['index']
        self.link_claim_id_index = link_claim_id_field['index']

        return True

    def read_row_file_rows(self, rows):
        for row in rows:
            claim_id = row[self.claim_id_index].strip()
            if claim_id in self.linked_claim_replacement_dic or not re.match('^.{11}A[\d]+$', claim_id):
                continue

            new_link_claim_id = None
            new_type_of_bill_code = None
            if claim_id[11] == 'A':
                if claim_id[12:] == '1':
                    new_link_claim_id = claim_id[:11]
                else:
                    claim_id_13_character = int(claim_id[12:])
                    new_link_claim_id = claim_id[:-1] + str(claim_id_13_character - 1)

            type_of_bill_code = row[self.type_of_bill_code_index].strip()
            if len(type_of_bill_code) > 2:
                new_type_of_bill_code = type_of_bill_code[:2] + '1' + type_of_bill_code[3:]
            else:
                new_type_of_bill_code = type_of_bill_code

            new_frequency_code = '1'
            self.linked_claim_replacement_dic[claim_id] = (new_link_claim_id, new_type_of_bill_code, new_frequency_code)

    def prepare_data(self):
        pass

    def process_rows(self, rows):
        if len(self.linked_claim_replacement_dic) == 0:
            return rows

        for row in rows:
            claim_id = row[self.claim_id_index]
            if claim_id in self.linked_claim_replacement_dic:
                new_link_claim_id, new_type_of_bill_code, new_frequency_code = self.linked_claim_replacement_dic[claim_id]
                row[self.link_claim_id_index] = new_link_claim_id
                row[self.type_of_bill_code_index] = new_type_of_bill_code
                row[self.frequency_code_index] = new_frequency_code
        return rows
