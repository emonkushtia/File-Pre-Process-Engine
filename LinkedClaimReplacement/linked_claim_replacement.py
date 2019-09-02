import os
from Babel import CsvWriter, rds_provider, redshift_provider, file_utility, s3_provider, EntityType, print_message, CsvReader
from pre_process_script_interface import IPreProcessScript, PreProcessOrder

temp_table_columns = ['date_of_service_from_medical_claims_header', 'billing_national_provider_id_medical_claims_header', 'claim_id_medical_claim_header',
                      'contract_id_product_reference', 'member_id_member_demographic', 'procedure_code_medical_claims_detail']


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16111
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.frequency_code_index = None
        self.linked_claim_temp_table_definitions = None
        self.claim_id_index = None
        self.link_claim_id_index = None
        self.linked_claim_replacement_dic = {}
        self.linked_claims = []
        self.temp_table_name = 'Temp_Linked_claim_Replacement_{0}_{1}'.format(self.entity_type, self.map_id).replace('-', '')
        pre_process_folder = os.path.split(self.merge_root_path)[-1]
        self.temp_folder = '{0}/Input/LinkedClaimReplacement'.format(self.merge_root_path)
        self.temp_s3_folder = 'PythonPreProcess/{0}/Input/LinkedClaimReplacement'.format(pre_process_folder)

    @property
    def order(self):
        return PreProcessOrder.LinkedClaimReplacement.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableLinkedClaimReplacement')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableLinkedClaimReplacement']) is False:
            return False
        return self._has_required_column()

    def _has_required_column(self):
        frequency_code_field = self.map_columns.where(lambda x: x['redshift_name'] == 'frequency_code_medical_claims_header').first_or_default()
        link_claim_id_field = self.map_columns.where(lambda x: x['redshift_name'] == 'linked_claimid_medical_claims_header').first_or_default()

        if frequency_code_field is None:
            print_message('LinkedClaimReplacement pre-process service stopped because mandatory column "frequency_code_medical_claims_header" is missing in the map.')
            return False
        if link_claim_id_field is None:
            print_message('LinkedClaimReplacement pre-process service stopped because mandatory column "linked_claimid_medical_claims_header" is missing in the map.')
            return False

        for link_claim_field in temp_table_columns:
            field = self.map_columns.where(lambda x: x['redshift_name'] == link_claim_field).first_or_default()
            if field is None:
                print_message('LinkedClaimReplacement pre-process service stopped because mandatory column "{}" is missing in the map.'.format(link_claim_id_field))
                return False

        self.frequency_code_index = frequency_code_field['index']
        self.link_claim_id_index = link_claim_id_field['index']
        self.claim_id_index = self.map_columns.where(lambda x: x['redshift_name'] == 'claim_id_medical_claim_header').first()['index']
        self.linked_claim_temp_table_definitions = self.map_columns.where(lambda x: x['redshift_name'] in temp_table_columns).to_list()
        return True

    def read_row_file_rows(self, rows):
        for row in rows:
            frequency_code = row[self.frequency_code_index].strip()
            if frequency_code not in ['7', '8']:
                continue

            linked_claim = []
            for field in self.linked_claim_temp_table_definitions:
                linked_claim.append(row[field['index']])
            self.linked_claims.append(linked_claim)

    def prepare_data(self):
        if len(self.linked_claims) == 0:
            return

        file_utility.create_folder(self.temp_folder)
        self._unload_linked_claim_replacement_data()
        self._load_linked_claim_replacement_data()

    def _unload_linked_claim_replacement_data(self):
        with CsvWriter('{}/linked_claims.txt'.format(self.temp_folder), self.delimiter) as writer:
            writer.writerows(self.linked_claims)

        s3_provider.upload_file('{}/linked_claims.txt'.format(self.temp_folder), '{}/linked_claims.txt'.format(self.temp_s3_folder))
        columns = [x['redshift_name'] for x in self.linked_claim_temp_table_definitions]
        create_columns_statement = ['{0} VARCHAR(500)'.format(x) for x in columns]
        create_columns_statement = ', '.join(create_columns_statement)
        create_table_sql = 'DROP TABLE IF EXISTS {0}; CREATE TABLE {0}({1});'.format(self.temp_table_name, create_columns_statement)
        redshift_provider.copy_data_from_s3({
            'data_preparation_query': create_table_sql,
            'table_name': self.temp_table_name,
            'columns': ', '.join(columns),
            's3_location': '{}/linked_claims.txt'.format(self.temp_s3_folder),
            'delimiter': self.delimiter
        })

        unload_query = '''
                    select DISTINCT t.claim_id_medical_claim_header,mh.claim_id_medical_claim_header AS linked_claimid_medical_claims_header from medical_claims_header mh
        INNER JOIN medical_claims_detail md ON mh.linenumber = md.linenumber_header
        INNER JOIN {temp_table} t ON t.contract_id_product_reference = mh.contract_id_product_reference
        AND COALESCE(t.member_id_member_demographic, ''\'') = COALESCE(mh.member_id_member_demographic,''\'')
        AND COALESCE(t.date_of_service_from_medical_claims_header, ''\'') = COALESCE(mh.date_of_service_from_medical_claims_header,''\'')
        AND COALESCE(t.billing_national_provider_id_medical_claims_header, ''\'') = COALESCE(mh.billing_national_provider_id_medical_claims_header, ''\'')
        AND COALESCE(t.procedure_code_medical_claims_detail, ''\'') = COALESCE(md.procedure_code_medical_claims_detail, ''\'')
        WHERE  t.claim_id_medical_claim_header != mh.claim_id_medical_claim_header
        AND COALESCE(mh.frequency_code_medical_claims_header, ''\'') NOT IN (''7'', ''8'');
        '''
        redshift_provider.unload_data({
            'unload_query': unload_query.format(temp_table=self.temp_table_name),
            's3_location': '{}/LinkedClaims'.format(self.temp_s3_folder),
            'destination_folder_location': '{}/LinkedClaims'.format(self.temp_folder)
        })
        redshift_provider.execute_query('DROP TABLE IF EXISTS {};'.format(self.temp_table_name))

    def _load_linked_claim_replacement_data(self):
        for file in os.listdir('{}/LinkedClaims'.format(self.temp_folder)):
            with CsvReader('{0}/LinkedClaims/{1}'.format(self.temp_folder, file), '|', has_header_columns=False) as csv_reader:
                for claim_id, linked_claim_id in csv_reader.read_records():
                    self.linked_claim_replacement_dic[claim_id] = linked_claim_id

    def process_rows(self, rows):
        if len(self.linked_claims) == 0:
            return rows

        for row in rows:
            claim_id = row[self.claim_id_index]
            if claim_id in self.linked_claim_replacement_dic:
                row[self.link_claim_id_index] = self.linked_claim_replacement_dic[claim_id]
        return rows
