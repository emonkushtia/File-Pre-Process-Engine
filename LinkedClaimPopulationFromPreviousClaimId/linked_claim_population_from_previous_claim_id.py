import os

from Babel import CsvWriter, rds_provider, redshift_provider, file_utility, s3_provider, EntityType, print_message, CsvReader
from pre_process_script_interface import IPreProcessScript, PreProcessOrder


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16723
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.linked_claim_id_column_index = None
        self.cross_reference_number_index = None
        self.claim_process_status_code_index = None
        self.system_assigned_number_index = None
        self.linked_claim_id_file_column_name = None
        self.linked_claim_id_by_previous_claim_number = {}
        self.previous_claim_ids = set()
        self.temp_table_name = 'Temp_PreviousClaimIds_{0}_{1}'.format(self.entity_type, self.map_id).replace('-', '')
        pre_process_folder = os.path.split(self.merge_root_path)[-1]
        self.temp_folder = '{0}/Input/LinkedClaimPopulationFromPreviousClaimId'.format(self.merge_root_path)
        self.temp_s3_folder = 'PythonPreProcess/{0}/Input/LinkedClaimPopulationFromPreviousClaimId'.format(pre_process_folder)

    @property
    def order(self):
        return PreProcessOrder.LinkedClaimIdPopulationFromPreviousClaimIdOrder.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableLinkedClaimIdPopulationFromPreviousClaimId')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableLinkedClaimIdPopulationFromPreviousClaimId']) is False:
            return False

        return self.has_required_column()

    def has_required_column(self):
        map_column_linked_claim_id = self.map_columns.where(lambda x: x['redshift_name'] == 'linked_claimid_medical_claims_header').first_or_default()
        map_column_cross_reference_number = self.map_columns.where(lambda x: x['redshift_name'] == 'reference_previous_claim_id_medical_claims_header').first_or_default()
        map_column_claim_process_status_code = self.map_columns.where(lambda x: x['redshift_name'] == 'record_status_medical_claims_header').first_or_default()
        map_column_system_assigned_number = self.map_columns.where(lambda x: x['redshift_name'] == 'previous_claim_number_medical_claims_header').first_or_default()

        if map_column_linked_claim_id is None:
            print_message('LinkedClaimIdPopulation pre-process service stopped because mandatory column "Linked Claim ID - From" is missing in the map. Please map the column and try again.')
            return False
        if map_column_cross_reference_number is None:
            print_message('LinkedClaimIdPopulation pre-process service stopped because mandatory column "Reference Previous Claim Id" is missing in the map. Please map the column and try again.')
            return False
        if map_column_claim_process_status_code is None:
            print_message('LinkedClaimIdPopulation pre-process service stopped because mandatory column "Claim Status Code" is missing in the map. Please map the column and try again.')
            return False
        if map_column_system_assigned_number is None:
            print_message('LinkedClaimIdPopulation pre-process service stopped because mandatory column "Previous Claim ID" is missing in the map. Please map the column and try again.')
            return False

        self.linked_claim_id_column_index = map_column_linked_claim_id['index']
        self.linked_claim_id_file_column_name = map_column_linked_claim_id['column_name']
        self.cross_reference_number_index = map_column_cross_reference_number['index']
        self.claim_process_status_code_index = map_column_claim_process_status_code['index']
        self.system_assigned_number_index = map_column_system_assigned_number['index']
        return True

    def read_row_file_rows(self, rows):
        for row in rows:
            previous_claim_id = self.find_previous_claim_id(row)
            if previous_claim_id:
                self.previous_claim_ids.add(previous_claim_id)

    def prepare_data(self):
        self.previous_claim_ids.discard('')
        if len(self.previous_claim_ids) == 0:
            return

        file_utility.create_folder(self.temp_folder)
        self._unload_linked_claim_replacement_data()
        self._load_linked_claim_replacement_data()

    def _unload_linked_claim_replacement_data(self):
        with CsvWriter('{}/linked_claim_previous_claim_ids.txt'.format(self.temp_folder), self.delimiter) as writer:
            for previous_claim_id in self.previous_claim_ids:
                writer.writerow([previous_claim_id])

        s3_provider.upload_file('{}/linked_claim_previous_claim_ids.txt'.format(self.temp_folder), '{}/linked_claim_previous_claim_ids.txt'.format(self.temp_s3_folder))
        create_table_sql = 'DROP TABLE IF EXISTS {0}; CREATE TABLE {0}(previous_claim_id VARCHAR(500)) DISTKEY(previous_claim_id);'.format(self.temp_table_name)
        redshift_provider.copy_data_from_s3({
            'data_preparation_query': create_table_sql,
            'table_name': self.temp_table_name,
            'columns': 'previous_claim_id',
            's3_location': '{}/linked_claim_previous_claim_ids.txt'.format(self.temp_s3_folder),
            'delimiter': self.delimiter
        })

        unload_query = '''SELECT DISTINCT m.previous_claim_number_medical_claims_header, m.claim_id_medical_claim_header
         FROM medical_claims_header m INNER JOIN {} t ON m.previous_claim_number_medical_claims_header = t.previous_claim_id;'''.format(self.temp_table_name)
        redshift_provider.unload_data({
            'unload_query': unload_query,
            's3_location': '{}/LinkedClaimIds'.format(self.temp_s3_folder),
            'destination_folder_location': '{}/LinkedClaimIds'.format(self.temp_folder)
        })
        redshift_provider.execute_query('DROP TABLE IF EXISTS {};'.format(self.temp_table_name))

    def _load_linked_claim_replacement_data(self):
        for file in os.listdir('{}/LinkedClaimIds'.format(self.temp_folder)):
            with CsvReader('{0}/LinkedClaimIds/{1}'.format(self.temp_folder, file), '|', has_header_columns=False) as csv_reader:
                for line in csv_reader.read_records():
                    self.linked_claim_id_by_previous_claim_number[line[0]] = line[1]

    def process_rows(self, rows):
        for row in rows:
            row[self.linked_claim_id_column_index] = ''
        if len(self.linked_claim_id_by_previous_claim_number) == 0:
            return rows

        for row in rows:
            previous_claim_id = self.find_previous_claim_id(row)
            if previous_claim_id and previous_claim_id in self.linked_claim_id_by_previous_claim_number:
                row[self.linked_claim_id_column_index] = self.linked_claim_id_by_previous_claim_number[previous_claim_id]
        return rows

    def find_previous_claim_id(self, row):
        cross_reference_number = row[self.cross_reference_number_index]
        status_code = row[self.claim_process_status_code_index]
        system_assigned_number = row[self.system_assigned_number_index]
        previous_claim_id = ''

        if cross_reference_number:
            previous_claim_id = cross_reference_number
        elif system_assigned_number and status_code.upper() == 'ADJUSTMENT':
            previous_claim_id = system_assigned_number
        return previous_claim_id
