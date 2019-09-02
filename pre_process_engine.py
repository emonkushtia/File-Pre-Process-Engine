import argparse
import asyncio
import importlib.util
import logging
import multiprocessing
import os
import sys

from py_linq import Enumerable

dir_path = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(__name__)
sys.path.append(os.path.split(dir_path)[0])  # ::TODO:: We need a batter way to import top directory python package.
from Babel import delimiters, rds_provider, file_utility, time_it, EntityType, print_message, CsvReader, CsvWriter, ArgumentMap
from pre_process_script_interface import PreProcessOrder, FileLoadException

data_loading_filter_policies = [
    PreProcessOrder.UpdateNpiDatabase.value,
    PreProcessOrder.DentalOrMedicalClaimExtractor.value,
    PreProcessOrder.ClaimIDConcatenation.value,
    PreProcessOrder.FileLoadFilterOrder.value,
    PreProcessOrder.LoadingWeeklyPaidClaims.value,
    PreProcessOrder.ClaimTobPosPopulation.value
]

data_transformation_policies = [
    PreProcessOrder.TaxonomyCodePopulation.value,
    PreProcessOrder.ClaimHICNReplacement.value,
    PreProcessOrder.LinkedClaimReplacement.value,
    PreProcessOrder.LinkedClaimIdPopulationFromPreviousClaimIdOrder.value,
    PreProcessOrder.PullingProviderInformationFromNpiTable.value,
    PreProcessOrder.PopulatingDiagnosisPointer.value,
    PreProcessOrder.ClaimDiagnosisFlattening.value,
    PreProcessOrder.LinkedClaimPopulationFromVersionedClaimId.value
]

parallel_process_count = multiprocessing.cpu_count()
buffer_size = 4096
record_batch_size = 1000


class PreProcessEngine(object):
    def __init__(self, args):
        print_message('Arguments are: {}'.format(args))
        self.pre_process_type = args.pre_process_type
        self.entity_type = args.entity_type
        self.delimiter = delimiters[args.delimiter]
        self.control_file_id = args.control_file_id
        self.map_id = args.map_id
        self.file_location = args.file_location
        self.file_name = os.path.basename(args.file_location)
        self.is_debug_mode = args.d
        self.merge_root_path = '{}/PreProcess_{}_{}_{}_{}'.format(dir_path, self.entity_type, self.pre_process_type, self.control_file_id, self.map_id).replace('-', '')
        self.pre_processes = eval('{}_policies'.format(args.pre_process_type))
        self._initialized()

    def _initialized(self):
        file_utility.create_folder('{0}/Input'.format(self.merge_root_path))
        file_utility.create_folder('{0}/Output'.format(self.merge_root_path))
        file_utility.copy_file(self.file_location, os.path.join(self.merge_root_path, 'Input', self.file_name))

    def execute_process(self):
        try:
            context = self._get_context_arguments()
            pre_process_scripts = self._get_pre_process_scripts(context)
            if len(pre_process_scripts) == 0:
                return

            pre_process_name = [PreProcessOrder(x.order).name for x in pre_process_scripts]
            print_message('Executing pre-process names: {}'.format(pre_process_name))
            self._prepare_base_data(context, pre_process_scripts)
            self._start_pre_process(context, pre_process_scripts)
            self._post_processing_tasks(pre_process_scripts)
            self._upload_merge_file()

        except FileLoadException as ex:
            logger.exception(ex.error_message)
        except Exception as ex:
            logger.exception(ex)
        finally:
            file_utility.delete_folder(self.merge_root_path)

    def _prepare_base_data(self, context, pre_process_scripts):
        need_row_file_precess_scripts = [x for x in pre_process_scripts if 'read_row_file_rows' in dir(x)]
        if len(need_row_file_precess_scripts) > 0:
            batch_filter_func = self._batch_filter_func(context)
            with CsvReader('{0}/Input/{1}'.format(self.merge_root_path, self.file_name), self.delimiter, buffer_size) as reader:
                for batch_records in reader.batch_with_filter_records(batch_filter_func):
                    for pre_process_script in need_row_file_precess_scripts:
                        pre_process_script.read_row_file_rows(batch_records)
        for pre_process_script in pre_process_scripts:
            pre_process_script.prepare_data()

    @time_it('Executing file pre-process')
    def _start_pre_process(self, context, pre_process_scripts):
        loop = asyncio.get_event_loop()
        reader_queue = asyncio.Queue(loop=loop, maxsize=parallel_process_count)
        reader_task = self._reader_task(reader_queue, context)
        transformation_queue = asyncio.Queue(loop=loop, maxsize=parallel_process_count)
        transformation_task = self._transformation_task(reader_queue, transformation_queue, pre_process_scripts)
        writer_task = self._writer_task(transformation_queue, context)
        loop.run_until_complete(asyncio.gather(reader_task, transformation_task, writer_task))

    def _batch_filter_func(self, context):
        def add_record_in_batch(records, current_record):
            if len(records) < record_batch_size:
                return True
            return False

        if self.entity_type == EntityType.Claim.name and self.pre_process_type == 'data_transformation':
            claim_id_index = context.map_columns.where(lambda x: x['redshift_name'] == 'claim_id_medical_claim_header').first()['index']

            def add_record_in_batch(records, current_record):
                claim_id = current_record[claim_id_index]
                if any(x[claim_id_index] == claim_id for x in records):
                    return True
                if len(records) < record_batch_size:
                    return True
                return False
        return add_record_in_batch

    async def _reader_task(self, queue, context):
        batch_filter_func = self._batch_filter_func(context)
        with CsvReader('{0}/Input/{1}'.format(self.merge_root_path, self.file_name), self.delimiter, buffer_size) as reader:
            for batch_records in reader.batch_with_filter_records(batch_filter_func):
                await queue.put(batch_records)
            await queue.put(None)  # poison pill to signal all the work is done

    @staticmethod
    async def _transformation_task(reader_queue, transformation_queue, pre_process_scripts):
        while True:
            records = await reader_queue.get()  # coroutine will be blocked if queue is empty
            if records is None:  # if poison pill is detected, exit the loop
                await transformation_queue.put(None)
                break

            for pre_process_item in pre_process_scripts:
                records = pre_process_item.process_rows(records)
                if len(records) == 0:
                    break
            if len(records) > 0:
                await transformation_queue.put(records)
            reader_queue.task_done()

    async def _writer_task(self, transformation_queue, context):
        with CsvWriter('{0}/Output/{1}'.format(self.merge_root_path, self.file_name), self.delimiter, buffer_size) as writer:
            writer.writerow(context.header_columns)
            while True:
                records = await transformation_queue.get()  # coroutine will be blocked if queue is empty
                if records is None:  # if poison pill is detected, exit the loop
                    break
                writer.writerows(records)
                transformation_queue.task_done()

    def _get_context_arguments(self):
        with CsvReader('{0}/Input/{1}'.format(self.merge_root_path, self.file_name), self.delimiter) as file:
            header_columns = file.header_columns
        rds_provider.create_map_columns_file('{}/map_file.txt'.format(self.merge_root_path), self.map_id, self.delimiter)
        map_columns = self._get_map_columns(header_columns)
        return ArgumentMap({
            'entity_type': self.entity_type,
            'delimiter': self.delimiter,
            'map_id': self.map_id,
            'control_file_id': self.control_file_id,
            'file_location': '{0}/Input/{1}'.format(self.merge_root_path, self.file_name),
            'file_name': self.file_name,
            'merge_root_path': self.merge_root_path,
            'is_debug_mode': self.is_debug_mode,
            'map_columns': map_columns,
            'header_columns': header_columns
        })

    def _get_map_columns(self, header_columns):
        map_list = []
        map_file_reader = file_utility.get_reader('{}/map_file.txt'.format(self.merge_root_path), self.delimiter)
        next(map_file_reader)
        map_columns = {map_col[0].lower(): map_col[1].lower() for map_col in map_file_reader}
        for index, header_column in enumerate(header_columns):
            header_column = header_column.lower()
            if header_column in map_columns:
                map_list.append({'column_name': header_column, 'redshift_name': map_columns[header_column], 'index': index})
            else:
                map_list.append({'column_name': header_column, 'redshift_name': '', 'index': index})
        return Enumerable(map_list)

    def _get_pre_process_scripts(self, context):
        scripts = []
        for root, dirs, files in os.walk('{}/PreProcessScripts'.format(dir_path), topdown=False):
            for file in files:
                if file.split('.')[-1] != 'py':
                    continue
                file_path = os.path.join(root, file)
                spec = importlib.util.spec_from_file_location("PreProcessScript", file_path)
                pre_process_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(pre_process_module)
                if 'PreProcessScript' not in dir(pre_process_module):
                    continue
                pre_process_item = pre_process_module.PreProcessScript(context)
                if pre_process_item.order in self.pre_processes and pre_process_item.will_execute():
                    scripts.append(pre_process_item)
        return sorted(scripts, key=lambda x: x.order)

    @staticmethod
    def _post_processing_tasks(pre_process_scripts):
        for pre_process_script in pre_process_scripts:
            pre_process_script.post_processing_tasks()

    def _upload_merge_file(self):
        print_message('Copy file to upload folder.')
        destination_folder = os.path.split(self.file_location)[0]
        merge_file_location = '{0}\Output\{1}'.format(self.merge_root_path, self.file_name)
        file_utility.copy_file(merge_file_location, os.path.join(destination_folder, '{0}'.format(self.file_name)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("pre_process_type", help="Pre process type. For example: file_load_filter, file_transformation  etc.... ", type=str)
    parser.add_argument("entity_type", help="Entity type. For example: Claim, Member, PharmacyClaim or DentalClaim etc.... ", type=str)
    parser.add_argument("delimiter", help="Delimiter of the file.", type=str)
    parser.add_argument("control_file_id", help="The control file id.", type=str)
    parser.add_argument("map_id", help="The map id.", type=str)
    parser.add_argument("file_location", help="File location.", type=str)
    parser.add_argument("-d", action="store_true", help="debug mode")
    PreProcessEngine(parser.parse_args()).execute_process()


if __name__ == '__main__':
    main()
