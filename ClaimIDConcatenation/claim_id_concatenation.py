from Babel import rds_provider, EntityType
from pre_process_script_interface import IPreProcessScript, PreProcessOrder


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-17001
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.claim_source_index = None
        self.claim_id_index = None

    @property
    def order(self):
        return PreProcessOrder.ClaimIDConcatenation.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableClaimIDConcatenation')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableClaimIDConcatenation']) is False:
            return False

        claim_id_field = self.map_columns.where(lambda x: x['redshift_name'] == 'claim_id_medical_claim_header').first_or_default()
        claim_source_field = self.map_columns.where(lambda x: x['redshift_name'] == 'claim_source_medical_claims_header').first_or_default()
        if claim_source_field is None:
            return False

        self.claim_id_index = claim_id_field['index']
        self.claim_source_index = claim_source_field['index']

        return True

    def prepare_data(self):
        pass

    def process_rows(self, rows):
        for row in rows:
            claim_source = row[self.claim_source_index].strip()
            claim_id = row[self.claim_id_index].strip()
            if claim_id.startswith(claim_source):
                continue
            row[self.claim_id_index] = claim_source + claim_id
        return rows
