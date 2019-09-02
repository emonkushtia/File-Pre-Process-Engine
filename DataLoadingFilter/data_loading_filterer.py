import json

from Babel import rds_provider
from pre_process_script_interface import IPreProcessScript, PreProcessOrder, FileLoadException


class PreProcessScript(IPreProcessScript):  # https://babelhealth.atlassian.net/browse/BBL-16694 , BBL-16724
    def __init__(self, context):
        IPreProcessScript.__init__(self, context)
        self.data_loading_filter_criteria = None
        self.any_records_found = False

    @property
    def order(self):
        return PreProcessOrder.FileLoadFilterOrder.value

    def will_execute(self):
        query = "SELECT [Key],Value FROM  Configurations WHERE [Key] IN ('EnableDataLoadingFilterCriteria', 'DataLoadingFilterCriteria')"
        data = rds_provider.fetch_all(query)
        configurations = dict((k[0], k[1]) for k in data)
        if IPreProcessScript.to_boolean(configurations['EnableDataLoadingFilterCriteria']) is False or configurations['DataLoadingFilterCriteria'].strip() == '':
            return False
        self.data_loading_filter_criteria = FilterExpressionParser(configurations['DataLoadingFilterCriteria'].lower(), self.header_columns).to_filter_expression()
        if self.data_loading_filter_criteria:
            return True
        return False

    def prepare_data(self):
        pass

    def process_rows(self, rows):
        new_rows = []
        for row in rows:
            if eval(self.data_loading_filter_criteria):
                self.any_records_found = True
                new_rows.append(row)
        return new_rows

    def post_processing_tasks(self):
        if self.any_records_found:
            return
        raise FileLoadException('Data loading filter pre process does not satisfy any records.')


class DataLoadingFilter:
    def __init__(self, dict_obj):
        vars(self).update(dict_obj)
        if 'condition_operator' not in dict_obj:
            self.condition_operator = 'or'


class FilterExpressionParser:
    def __init__(self, filter_expression, source_columns):
        self.filter_expression = filter_expression
        self.source_columns = [x.lower() for x in source_columns]

    def to_filter_expression(self):
        filter_json = json.dumps(eval(self.filter_expression))
        data_loading_filter = json.loads(filter_json, object_hook=DataLoadingFilter)
        self._filter_conditions(data_loading_filter)
        filter_expression = self._filter_to_query(data_loading_filter)
        return filter_expression.lower()

    @staticmethod
    def _get_value_query(operator, value, value2=None):
        if operator == 'in' or operator == 'not in':
            value = value.split(',')
            return "('{}')".format("','".join(value))
        if operator == '==' or operator == '!=':
            return "'{}'".format(value)

    def _get_index(self, column):
        try:
            return self.source_columns.index(column)
        except:
            return -1

    def _filter_conditions(self, filter_obj):
        group = []
        for data_filter in filter_obj.conditions:
            if hasattr(data_filter, 'conditions'):
                child_grp = self._filter_conditions(data_filter)
                if len(child_grp.conditions) > 0:
                    group.append(data_filter)
            else:
                column_index = self._get_index(data_filter[0])
                if column_index >= 0:
                    group.append(data_filter)
        filter_obj.conditions = group
        return filter_obj

    def _filter_to_query(self, filter_obj):
        filter_query = ''
        for index, data_filter in enumerate(filter_obj.conditions):
            if index > 0:
                filter_query += ' {} '.format(filter_obj.condition_operator)
            if hasattr(data_filter, 'conditions'):
                filter_query += self._filter_to_query(data_filter)
            else:
                column_index = self._get_index(data_filter[0])
                filter_query += 'row[{}].strip().lower() {} {}'.format(column_index, data_filter[1], self._get_value_query(data_filter[1], data_filter[2]))

        if len(filter_obj.conditions) > 0:
            return '({})'.format(filter_query)
        return filter_query

# filter_ex = '{"conditions": [{"condition_operator": "and", "conditions": [("clmstat", "in", "pa,pd"), ("svcstat", "in", "pa,pd")]}, ("prvtyp", "not in", "yy")]}'
# filter_ex = '{"conditions": [{"condition_operator": "and", "conditions": [("CLM_PROCESS_STUS_CD", "in", "A"), ("CLM_LN_PROCESS_STUS_CD", "in", "A")]}, {"condition_operator": "and", "conditions": [("CLM_PROCESS_STUS_CD", "in", "J"), ("CLM_LN_PROCESS_STUS_CD", "in", "J,H")]},{"conditions": [("CLM_PROCESS_STUS_CD", "in", "V")]}]}'
