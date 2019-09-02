import os

from Babel import file_utility, redshift_provider, s3_provider, CsvWriter, CsvReader, EntityType, rds_provider
from pre_process_script_interface import IPreProcessScript, PreProcessOrder

dir_path = os.path.dirname(os.path.realpath(__file__))


class PreProcessScript(IPreProcessScript):
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.claim_member_id_index = None
        self.claim_hicn_index = None
        self.claim_payment_date_index = None
        self.claim_contract_id_index = None
        self.mbi_transition_date = None
        self.unload_columns = ['member_id_member', 'contract_id_product_reference', 'hicn_member', 'mbi_member']
        self.unload_columns_index = dict((k[1], k[0]) for k in enumerate(self.unload_columns))
        self.member_contract_set = set()
        self.member_dict = {}

        self.temp_table_name = 'Temp_ClaimHICNReplacement_{0}_{1}'.format(self.entity_type, self.map_id).replace('-', '')
        pre_process_folder = os.path.split(self.merge_root_path)[-1]
        self.temp_folder = '{0}/Input/ClaimHICNReplacement'.format(self.merge_root_path)
        self.temp_s3_folder = 'PythonPreProcess/{0}/Input/ClaimHICNReplacement'.format(pre_process_folder)

    @property
    def order(self):
        return PreProcessOrder.ClaimHICNReplacement.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableClaimHICNReplacement', 'MBITransitionDate')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableClaimHICNReplacement']) is False:
            return False
        self.mbi_transition_date = configurations['MBITransitionDate']

        return self._has_required_column()

    def _has_required_column(self):
        member_id_field = self.map_columns.where(lambda x: x['redshift_name'] == 'member_id_member_demographic').first_or_default()
        hicn_field = self.map_columns.where(lambda x: x['redshift_name'] == 'hicn_medical_claims_header').first_or_default()
        adjudication_or_payment_date_paid_field = self.map_columns.where(lambda x: x['redshift_name'] == 'adjudication_or_payment_date_paid_date_medical_claims_detail').first_or_default()
        contract_id_field = self.map_columns.where(lambda x: x['redshift_name'] == 'contract_id_product_reference').first_or_default()

        if member_id_field is None:
            return False
        if hicn_field is None:
            return False
        if adjudication_or_payment_date_paid_field is None:
            return False
        if contract_id_field is None:
            return False

        self.claim_member_id_index = member_id_field['index']
        self.claim_hicn_index = hicn_field['index']
        self.claim_payment_date_index = adjudication_or_payment_date_paid_field['index']
        self.claim_contract_id_index = contract_id_field['index']
        return True

    def read_row_file_rows(self, rows):
        for row in rows:
            member_and_contract = '{}{}{}'.format(row[self.claim_member_id_index], self.delimiter, row[self.claim_contract_id_index])
            self.member_contract_set.add(member_and_contract)

    def prepare_data(self):
        file_utility.create_folder(self.temp_folder)
        self._unload_data()
        self._load_data()

    def _unload_data(self):
        with CsvWriter('{0}/member_contract_list.txt'.format(self.temp_folder), self.delimiter) as writer:
            for member_and_contract in self.member_contract_set:
                writer.writerow(member_and_contract.split(self.delimiter))

        s3_provider.upload_file('{0}/member_contract_list.txt'.format(self.temp_folder), '{}/member_contract_list.txt'.format(self.temp_s3_folder))
        create_table_sql = 'DROP TABLE IF EXISTS {0}; CREATE TABLE {0}(member_id VARCHAR(500), contract_id VARCHAR(500));'.format(self.temp_table_name)
        redshift_provider.copy_data_from_s3({
            'data_preparation_query': create_table_sql,
            'table_name': self.temp_table_name,
            'columns': 'member_id,contract_id',
            's3_location': '{}/member_contract_list.txt'.format(self.temp_s3_folder),
            'delimiter': self.delimiter
        })

        unload_query = "select DISTINCT {} from member m INNER JOIN {} t ON m.member_id_member = t.member_id AND m.contract_id_product_reference = t.contract_id".format(str.join(',', self.unload_columns), self.temp_table_name)
        redshift_provider.unload_data({
            'unload_query': unload_query,
            's3_location': '{}/MemberData'.format(self.temp_s3_folder),
            'destination_folder_location': '{}/MemberData'.format(self.temp_folder)
        })
        redshift_provider.execute_query('DROP TABLE IF EXISTS {};'.format(self.temp_table_name))

    def _load_data(self):
        for file in os.listdir('{}/MemberData'.format(self.temp_folder)):
            with CsvReader('{}/MemberData/{}'.format(self.temp_folder, file), '|', has_header_columns=False) as csv_reader:
                for row in csv_reader.read_records():
                    self.member_dict[row[self.unload_columns_index['member_id_member']]] = row

    def process_rows(self, rows):
        if len(self.member_dict) == 0:
            return rows

        for row in rows:
            member_id = row[self.claim_member_id_index].strip()
            if member_id not in self.member_dict:
                continue

            member = self.member_dict[member_id]
            claim_paid_date = row[self.claim_payment_date_index].strip()
            if claim_paid_date < self.mbi_transition_date:
                row[self.claim_hicn_index] = member[self.unload_columns_index['hicn_member']]
            else:
                row[self.claim_hicn_index] = member[self.unload_columns_index['mbi_member']]

        return rows
