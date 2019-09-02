import json
import os

from Babel import file_utility, redshift_provider, s3_provider, CsvWriter, CsvReader
from pre_process_script_interface import IPreProcessScript, PreProcessOrder

dir_path = os.path.dirname(os.path.realpath(__file__))


class PreProcessScript(IPreProcessScript):
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.npi_ids = set()
        self.npi_taxonomy_code_map = []
        self.npi_ncpdp_map = []
        self.npi_field_index = []
        self.npi_lookup_dict = {}
        self.npi_header_columns = {}

        self.temp_table_name = 'Temp_Taxonomies_Npi_Ids_{0}_{1}'.format(self.entity_type, self.map_id).replace('-', '')
        pre_process_folder = os.path.split(self.merge_root_path)[-1]
        self.temp_folder = '{0}/Input/TaxonomyCodePopulation'.format(self.merge_root_path)
        self.temp_s3_folder = 'PythonPreProcess/{0}/Input/TaxonomyCodePopulation'.format(pre_process_folder)

    @property
    def order(self):
        return PreProcessOrder.TaxonomyCodePopulation.value

    def will_execute(self):
        if self.entity_type not in ['Claim', 'PharmacyClaim', 'DentalClaim']:
            return False

        configurations = json.loads(open('{}/Configurations.json'.format(dir_path)).read())[self.entity_type]

        for key, value in configurations['npi_taxonomy_map_fields'].items():
            taxonomy_code = self.map_columns.where(lambda x: x['redshift_name'] == key).first_or_default()
            npi_field = self.map_columns.where(lambda x: x['redshift_name'] == value).first_or_default()
            if npi_field is None or taxonomy_code is None:
                continue

            self.npi_field_index.append(npi_field['index'])
            self.npi_taxonomy_code_map.append((npi_field['index'], taxonomy_code['index']))

        if self.entity_type == 'PharmacyClaim':
            for key, value in configurations['npi_ncpdp_map_fields'].items():
                ncpdp_field = self.map_columns.where(lambda x: x['redshift_name'] == key).first_or_default()
                npi_field = self.map_columns.where(lambda x: x['redshift_name'] == value).first_or_default()
                if npi_field is None or taxonomy_code is None:
                    continue

                self.npi_field_index.append(npi_field['index'])
                self.npi_ncpdp_map.append((npi_field['index'], ncpdp_field['index']))

        if len(self.npi_taxonomy_code_map) == 0 and len(self.npi_ncpdp_map_fields) == 0:
            return False

        return True

    def read_row_file_rows(self, rows):
        for row in rows:
            for index in self.npi_field_index:
                self.npi_ids.add(row[index])

    def prepare_data(self):
        self.npi_ids.discard('')
        if len(self.npi_ids) == 0:
            return

        file_utility.create_folder(self.temp_folder)
        self._unload_npi_data()
        self._load_npi_data()

    def _unload_npi_data(self):
        with CsvWriter('{0}/npi_ids.txt'.format(self.temp_folder), self.delimiter) as writer:
            for npi_id in self.npi_ids:
                writer.writerow([npi_id])

        s3_provider.upload_file('{0}/npi_ids.txt'.format(self.temp_folder), '{}/npi_ids.txt'.format(self.temp_s3_folder))
        create_table_sql = 'DROP TABLE IF EXISTS {0}; CREATE TABLE {0}(npi_id VARCHAR(500)) DISTKEY(npi_id);'.format(self.temp_table_name)
        redshift_provider.copy_data_from_s3({
            'data_preparation_query': create_table_sql,
            'table_name': self.temp_table_name,
            'columns': 'npi_id',
            's3_location': '{}/npi_ids.txt'.format(self.temp_s3_folder),
            'delimiter': self.delimiter
        })

        source_fields = ['npi']
        for i in range(1, 16):
            source_fields.append('"healthcare provider primary taxonomy switch_{0}"'.format(i))
            source_fields.append('"healthcare provider taxonomy code_{0}"'.format(i))
        if len(self.npi_ncpdp_map_fields) > 0:
            for i in range(1, 51):
                source_fields.append('"other provider identifier state_{0}"'.format(i))
                source_fields.append('"other provider identifier issuer_{0}"'.format(i))
                source_fields.append('"other provider identifier_{0}"'.format(i))

        for index, field in enumerate(source_fields):
            self.npi_header_columns[field] = index
        unload_query = "SELECT DISTINCT {0} FROM npilookupvalues INNER JOIN {1} ON  npilookupvalues.npi = {1}.npi_id;".format(str.join(',', source_fields), self.temp_table_name)
        redshift_provider.unload_data({
            'unload_query': unload_query,
            's3_location': '{}/NpiValues'.format(self.temp_s3_folder),
            'destination_folder_location': '{}/NpiValues'.format(self.temp_folder)
        })
        redshift_provider.execute_query('DROP TABLE IF EXISTS {};'.format(self.temp_table_name))

    def _load_npi_data(self):
        for npi_file in os.listdir('{}/NpiValues'.format(self.temp_folder)):
            with CsvReader('{}/NpiValues/{}'.format(self.temp_folder, npi_file), '|', has_header_columns=False) as csv_reader:
                for row in csv_reader.read_records():
                    self.npi_lookup_dict[row[self.npi_header_columns['npi']]] = self._get_npi_value(row)

    def _get_npi_value(self, npi_row):
        npi_header_columns = self.npi_header_columns
        taxonomy_code, ncpdp_value = None, None

        for i in range(1, 16):
            if npi_row[npi_header_columns['"healthcare provider primary taxonomy switch_{0}"'.format(i)]].strip().lower() in ['y', 'true']:
                taxonomy_code = npi_row[npi_header_columns['"healthcare provider taxonomy code_{0}"'.format(i)]]
                break

        if taxonomy_code is None:
            for i in range(1, 16):
                if npi_row[npi_header_columns['"healthcare provider taxonomy code_{0}"'.format(i)]]:
                    taxonomy_code = npi_row[npi_header_columns['"healthcare provider taxonomy code_{0}"'.format(i)]]
                    break

        if len(self.npi_ncpdp_map_fields) > 0:
            for i in range(1, 51):
                if not npi_row[npi_header_columns['"other provider identifier state_{0}"'.format(i)]] \
                        and npi_row[npi_header_columns['"other provider identifier issuer_{0}"'.format(i)]].strip().upper() == 'OTHER ID NUMBER-COMMERCIAL NUMBER':
                    ncpdp_value = npi_row[npi_header_columns['"other provider identifier_{0}"'.format(i)]]
                    break
        return {'taxonomy_code': taxonomy_code, 'ncpdp_value': ncpdp_value}

    def process_rows(self, rows):
        if len(self.npi_lookup_dict) == 0:
            return rows

        for row in rows:
            for npi_index, taxonomy_index in self.npi_taxonomy_code_map:
                npi_field_value, taxonomy_code_value = row[npi_index], row[taxonomy_index]
                if not taxonomy_code_value and npi_field_value in self.npi_lookup_dict:
                    npi_obj = self.npi_lookup_dict[npi_field_value]
                    row[taxonomy_index] = npi_obj['taxonomy_code']

            for npi_index, ncpdp_index in self.npi_ncpdp_map:
                npi_field_value, ncpdp_value = row[npi_index], row[ncpdp_index]
                if not ncpdp_value and npi_field_value in self.npi_lookup_dict:
                    npi_obj = self.npi_lookup_dict[npi_field_value]
                    row[ncpdp_index] = npi_obj['ncpdp_value']

        return rows
