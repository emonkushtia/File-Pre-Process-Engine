import abc
from enum import Enum


class FileLoadException(Exception):
    def __init__(self, error_message):
        self.error_message = error_message


class PreProcessOrder(Enum):
    ClaimIDConcatenation = 5
    DentalOrMedicalClaimExtractor = 9
    FileLoadFilterOrder = 10
    LoadingWeeklyPaidClaims = 20
    ClaimTobPosPopulation = 21
    UpdateNpiDatabase = 25

    TaxonomyCodePopulation = 29
    LinkedClaimIdPopulationFromPreviousClaimIdOrder = 30
    ClaimHICNReplacement = 31
    LinkedClaimReplacement = 32
    PullingProviderInformationFromNpiTable = 40
    ClaimDiagnosisFlattening = 50
    PopulatingDiagnosisPointer = 60
    LinkedClaimPopulationFromVersionedClaimId = 70


class IPreProcessScript(abc.ABC):
    def __init__(self, context):
        self.entity_type = context['entity_type']
        self.delimiter = context['delimiter']
        self.control_file_id = context['control_file_id']
        self.map_id = context['map_id']
        self.file_location = context['file_location']
        self.file_name = context['file_name']
        self.merge_root_path = context['merge_root_path']
        self.header_columns = context.header_columns
        self.map_columns = context.map_columns
        self.is_debug_mode = context['is_debug_mode']

    @property
    @abc.abstractmethod
    def order(self):
        pass

    @abc.abstractmethod
    def will_execute(self):
        pass

    @abc.abstractmethod
    def prepare_data(self):
        pass

    @abc.abstractmethod
    def process_rows(self, rows):
        pass

    def post_processing_tasks(self):
        pass

    @staticmethod
    def to_boolean(bool_string):
        if bool_string is None:
            return False
        bool_string = bool_string.lower().strip()
        if bool_string == '':
            return False
        if bool_string == 'true' or bool_string == 't' or bool_string == '1':
            return True
        return False
