import csv
import json
import os
import re
import sys

from requests_futures.sessions import FuturesSession

from Babel import file_utility, redshift_provider, s3_provider, CsvWriter, CsvReader, print_message
from pre_process_script_interface import IPreProcessScript, PreProcessOrder

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)  # ::TODO:: We need a batter way to import top directory python package.
import NpiDatabaseHelper


class PreProcessScript(IPreProcessScript):
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.npi_ids = set()
        self.npi_field_index = []

        self.temp_table_name = 'Temp_Update_Npi_Ids_{0}_{1}'.format(self.entity_type, self.map_id).replace('-', '')
        pre_process_folder = os.path.split(self.merge_root_path)[-1]
        self.temp_folder = '{0}/Input/UpdateNpiDatabase'.format(self.merge_root_path)
        self.temp_s3_folder = 'PythonPreProcess/{0}/Input/UpdateNpiDatabase'.format(pre_process_folder)

    @property
    def order(self):
        return PreProcessOrder.UpdateNpiDatabase.value

    def will_execute(self):
        if self.entity_type not in ['Claim', 'PharmacyClaim', 'DentalClaim', 'Provider']:
            return False

        configurations = json.loads(open('{}/Configurations.json'.format(dir_path)).read())[self.entity_type]
        npi_field_maps = self.map_columns.where(lambda x: x['redshift_name'] in configurations['npi_fields']).to_list()
        if len(npi_field_maps) == 0:
            return False
        self.npi_field_index = [x['index'] for x in npi_field_maps]
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
        self._download_npi_data()

        if len(os.listdir('{}/NPIJsonFiles'.format(self.temp_folder))) == 0:
            return

        self._create_csv_file()
        self._update_npi_database()

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

        unload_query = "SELECT DISTINCT npi_id FROM {0} WHERE  npi_id NOT IN (SELECT  COALESCE(npi,'''') FROM npilookupvalues);".format(self.temp_table_name)
        redshift_provider.unload_data({
            'unload_query': unload_query,
            's3_location': '{}/NpiValues'.format(self.temp_s3_folder),
            'destination_folder_location': '{}/NpiValues'.format(self.temp_folder)
        })
        redshift_provider.execute_query('DROP TABLE IF EXISTS {};'.format(self.temp_table_name))

    def _download_npi_data(self):
        file_utility.create_folder('{}/NPIJsonFiles'.format(self.temp_folder))
        for npi_file in os.listdir('{}/NpiValues'.format(self.temp_folder)):
            with CsvReader('{}/NpiValues/{}'.format(self.temp_folder, npi_file), '|', has_header_columns=False) as csv_reader:
                with FuturesSession(max_workers=10) as session:
                    for number in csv_reader.read_records():
                        parameters = {"number": number[0], "version": "2.1", "pretty": "on", "address_purpose": ""}
                        session.get("https://npiregistry.cms.hhs.gov/api", params=parameters, background_callback=self._create_npi_json_file)

    def _create_npi_json_file(self, sess, response):
        number = 0
        data = ''
        try:
            number = re.findall('.*number=([\d\w]*).*', response.url)[0]
            data = response.json()
            if 'results' in data and len(data['results']) > 0:
                with open('{}/NPIJsonFiles/Npi_{}.json'.format(self.temp_folder, number), 'w', encoding="utf-8") as json_file:
                    json.dump(data['results'][0], json_file, indent=2)
            else:
                print_message('NPI data not found for Id: {0}'.format(number))
        except:
            print_message('Exception for NPI Number: {0}\n Data Object: {1} \n Following Exception: {2}'.format(number, data, sys.exc_info()))

    def _create_csv_file(self):
        files = os.listdir('{}/NPIJsonFiles'.format(self.temp_folder))
        with open("{}/Export.csv".format(self.temp_folder), "w", newline='', encoding="utf-8") as csv_writer:
            writer = csv.DictWriter(csv_writer, fieldnames=list(NpiDatabaseHelper.npi_data_dictionary.keys()), delimiter=',', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for f in files:
                with open('{}/NPIJsonFiles/{}'.format(self.temp_folder, f)) as file:
                    try:
                        data_store = json.load(file)
                        for key in NpiDatabaseHelper.npi_data_dictionary:
                            NpiDatabaseHelper.npi_data_dictionary[key] = ''
                        NpiDatabaseHelper.populate_npi_dictionary(data_store)
                        writer.writerow(NpiDatabaseHelper.npi_data_dictionary)
                    except:
                        print('Data Can not processed for the file: '.format(file))
                        print(sys.exc_info())

    def _update_npi_database(self):
        with CsvReader('{0}/Export.csv'.format(self.temp_folder), delimiter=',') as file:
            columns = file.header_columns
        s3_provider.upload_file('{0}/Export.csv'.format(self.temp_folder), '{}/Export.csv'.format(self.temp_s3_folder))
        redshift_provider.copy_data_from_s3({
            'table_name': 'npilookupvalues',
            'ignore_header': True,
            'columns': '"{}"'.format('","'.join(columns)),
            's3_location': '{}/Export.csv'.format(self.temp_s3_folder),
            'delimiter': ',',
            'csv_quote': '"'
        })

    def process_rows(self, rows):
        return rows
