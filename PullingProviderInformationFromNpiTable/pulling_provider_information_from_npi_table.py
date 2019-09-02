import json
import os

from Babel import rds_provider, file_utility, redshift_provider, s3_provider, EntityType, CsvWriter, CsvReader
from pre_process_script_interface import IPreProcessScript, PreProcessOrder

dir_path = os.path.dirname(os.path.realpath(__file__))


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16850
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.npi_ids = set()
        self.npi_field_maps = []
        self.npi_field_dic = {}
        self.configurations_map = {}
        self.npi_lookup_dict = {}
        self.temp_table_name = 'Temp_NpiIds_{0}_{1}'.format(self.entity_type, self.map_id).replace('-', '')
        pre_process_folder = os.path.split(self.merge_root_path)[-1]
        self.temp_folder = '{0}/Input/PullingProviderInformationFromNpiTable'.format(self.merge_root_path)
        self.temp_s3_folder = 'PythonPreProcess/{0}/Input/PullingProviderInformationFromNpiTable'.format(pre_process_folder)

    @property
    def order(self):
        return PreProcessOrder.PullingProviderInformationFromNpiTable.value

    def will_execute(self):
        if self.entity_type != EntityType.Claim.name:
            return False

        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnablePullingProviderInformationFromNPPESForChartReview', 'EnablePullingProviderInformationFromNPPESForMedicalClaim')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        enable_chart_review = IPreProcessScript.to_boolean(configurations['EnablePullingProviderInformationFromNPPESForChartReview'])
        enable_claim = IPreProcessScript.to_boolean(configurations['EnablePullingProviderInformationFromNPPESForMedicalClaim'])

        if enable_chart_review is False and enable_claim is False:
            return False

        self._load_npi_configurations()
        if enable_chart_review and enable_claim:
            return True

        is_chart_review = self.file_name.startswith('ChartReview_Merged_')
        if enable_chart_review and is_chart_review:
            return True
        elif enable_claim and is_chart_review is False:
            return True

        return False

    def read_row_file_rows(self, rows):
        for row in rows:
            for source_type, type_config in self.configurations_map.items():
                self.npi_ids.add(row[type_config['key_field']])

    def prepare_data(self):
        self.npi_ids.discard('')
        if len(self.npi_ids) == 0:
            return

        file_utility.create_folder(self.temp_folder)
        self._unload_npi_data()

        self._load_npi_lookup_data()

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

        fields = ['"{}"'.format(x['field']) for x in self.npi_field_maps]
        unload_query = 'SELECT {} FROM npilookupvalues n INNER JOIN {} t ON n.npi = t.npi_id'.format(', '.join(fields), self.temp_table_name)
        redshift_provider.unload_data({
            'unload_query': unload_query,
            's3_location': '{}/NpiValues'.format(self.temp_s3_folder),
            'destination_folder_location': '{}/NpiValues'.format(self.temp_folder)
        })
        redshift_provider.execute_query('DROP TABLE IF EXISTS {};'.format(self.temp_table_name))

    def _load_npi_configurations(self):
        configurations = json.loads(open('{}/Configurations.json'.format(dir_path)).read())['Claim']
        fields = configurations['default_pulling_fields'].split(',')
        for source_type, type_config in list(configurations['types'].items()):
            fields.extend(k for k, v in type_config['mapping'].items())
        fields = sorted(set(fields))
        for index, field in enumerate(fields):
            self.npi_field_maps.append({'field': field.strip(), 'index': index})
        self.npi_field_dic = {x['field']: x['index'] for x in self.npi_field_maps}

        for source_type, type_config in list(configurations['types'].items()):
            key_field = self.map_columns.where(lambda x: x['redshift_name'] == type_config['key_field_for_claim']).first_or_default()
            if key_field:
                map_type = {'key_field': key_field['index'], 'mapping': {}, 'conditional_mapping': {}}
                for npi_field, redshift_name in type_config['mapping'].items():
                    field = self.map_columns.where(lambda x: x['redshift_name'] == redshift_name).first_or_default()
                    if field:
                        map_type['mapping'][npi_field] = field['index']

                for npi_field, redshift_name in type_config['conditional_mapping'].items():
                    field = self.map_columns.where(lambda x: x['redshift_name'] == redshift_name).first_or_default()
                    if field:
                        map_type['conditional_mapping'][npi_field] = field['index']
                self.configurations_map[source_type] = map_type

    def _load_npi_lookup_data(self):
        npi_index = [x['index'] for x in self.npi_field_maps if x['field'] == 'npi'][0]
        for file in os.listdir('{}/NpiValues'.format(self.temp_folder)):
            with CsvReader('{0}/NpiValues/{1}'.format(self.temp_folder, file), '|', has_header_columns=False) as csv_reader:
                for line in csv_reader.read_records():
                    self.npi_lookup_dict[line[npi_index]] = line

    def process_rows(self, rows):
        if len(self.npi_lookup_dict) == 0:
            return rows

        new_rows = []
        for row in rows:
            for source_type, type_config in self.configurations_map.items():
                npi_id = row[type_config['key_field']]
                if npi_id not in self.npi_lookup_dict:
                    continue

                npi_data = self.npi_lookup_dict[npi_id]
                for npi_field, source_column_index in type_config['mapping'].items():
                    if row[source_column_index] == '':
                        npi_field_index = self.npi_field_dic[npi_field]
                        row[source_column_index] = npi_data[npi_field_index]
                self.process_conditional_mapping(npi_data, row, type_config['conditional_mapping'])
            new_rows.append(row)
        return new_rows

    def process_conditional_mapping(self, npi_data, row, conditional_mapping):
        if 'last_name' not in conditional_mapping:
            return
        last_name_index = conditional_mapping['last_name']
        if row[last_name_index].strip() != '':
            return

        npi_field_index = self.npi_field_dic['provider organization name (legal business name)']
        if 'first_name' in conditional_mapping and row[conditional_mapping['first_name']] != '':
            npi_field_index = self.npi_field_dic['provider last name (legal name)']
        row[last_name_index] = npi_data[npi_field_index]
