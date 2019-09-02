import os
from datetime import datetime

npi_data_dictionary = {'npi': '',
                       'entity type code': '',
                       'replacement npi': '',
                       'employer identification number (ein)': '',
                       'provider organization name (legal business name)': '',
                       'provider last name (legal name)': '',
                       'provider first name': '',
                       'provider middle name': '',
                       'provider name prefix text': '',
                       'provider name suffix text': '',
                       'provider credential text': '',
                       'provider other organization name': '',
                       'provider other organization name type code': '',
                       'provider other last name': '',
                       'provider other first name': '',
                       'provider other middle name': '',
                       'provider other name prefix text': '',
                       'provider other name suffix text': '',
                       'provider other credential text': '',
                       'provider other last name type code': '',
                       'provider first line business mailing address': '',
                       'provider second line business mailing address': '',
                       'provider business mailing address city name': '',
                       'provider business mailing address state name': '',
                       'provider business mailing address postal code': '',
                       'provider business mailing address country code (if outside u s )': '',
                       'provider business mailing address telephone number': '',
                       'provider business mailing address fax number': '',
                       'provider first line business practice location address': '',
                       'provider second line business practice location address': '',
                       'provider business practice location address city name': '',
                       'provider business practice location address state name': '',
                       'provider business practice location address postal code': '',
                       'provider business practice location address country code (if outside u s )': '',
                       'provider business practice location address telephone number': '',
                       'provider business practice location address fax number': '',
                       'provider enumeration date': '',
                       'last update date': '',
                       'npi deactivation reason code': '',
                       'npi deactivation date': '',
                       'npi reactivation date': '',
                       'provider gender code': '',
                       'authorized official last name': '',
                       'authorized official first name': '',
                       'authorized official middle name': '',
                       'authorized official title or position': '',
                       'authorized official telephone number': '',
                       'healthcare provider taxonomy code_1': '',
                       'provider license number_1': '',
                       'provider license number state code_1': '',
                       'healthcare provider primary taxonomy switch_1': '',
                       'healthcare provider taxonomy code_2': '',
                       'provider license number_2': '',
                       'provider license number state code_2': '',
                       'healthcare provider primary taxonomy switch_2': '',
                       'healthcare provider taxonomy code_3': '',
                       'provider license number_3': '',
                       'provider license number state code_3': '',
                       'healthcare provider primary taxonomy switch_3': '',
                       'healthcare provider taxonomy code_4': '',
                       'provider license number_4': '',
                       'provider license number state code_4': '',
                       'healthcare provider primary taxonomy switch_4': '',
                       'healthcare provider taxonomy code_5': '',
                       'provider license number_5': '',
                       'provider license number state code_5': '',
                       'healthcare provider primary taxonomy switch_5': '',
                       'healthcare provider taxonomy code_6': '',
                       'provider license number_6': '',
                       'provider license number state code_6': '',
                       'healthcare provider primary taxonomy switch_6': '',
                       'healthcare provider taxonomy code_7': '',
                       'provider license number_7': '',
                       'provider license number state code_7': '',
                       'healthcare provider primary taxonomy switch_7': '',
                       'healthcare provider taxonomy code_8': '',
                       'provider license number_8': '',
                       'provider license number state code_8': '',
                       'healthcare provider primary taxonomy switch_8': '',
                       'healthcare provider taxonomy code_9': '',
                       'provider license number_9': '',
                       'provider license number state code_9': '',
                       'healthcare provider primary taxonomy switch_9': '',
                       'healthcare provider taxonomy code_10': '',
                       'provider license number_10': '',
                       'provider license number state code_10': '',
                       'healthcare provider primary taxonomy switch_10': '',
                       'healthcare provider taxonomy code_11': '',
                       'provider license number_11': '',
                       'provider license number state code_11': '',
                       'healthcare provider primary taxonomy switch_11': '',
                       'healthcare provider taxonomy code_12': '',
                       'provider license number_12': '',
                       'provider license number state code_12': '',
                       'healthcare provider primary taxonomy switch_12': '',
                       'healthcare provider taxonomy code_13': '',
                       'provider license number_13': '',
                       'provider license number state code_13': '',
                       'healthcare provider primary taxonomy switch_13': '',
                       'healthcare provider taxonomy code_14': '',
                       'provider license number_14': '',
                       'provider license number state code_14': '',
                       'healthcare provider primary taxonomy switch_14': '',
                       'healthcare provider taxonomy code_15': '',
                       'provider license number_15': '',
                       'provider license number state code_15': '',
                       'healthcare provider primary taxonomy switch_15': '',
                       'other provider identifier_1': '',
                       'other provider identifier type code_1': '',
                       'other provider identifier state_1': '',
                       'other provider identifier issuer_1': '',
                       'other provider identifier_2': '',
                       'other provider identifier type code_2': '',
                       'other provider identifier state_2': '',
                       'other provider identifier issuer_2': '',
                       'other provider identifier_3': '',
                       'other provider identifier type code_3': '',
                       'other provider identifier state_3': '',
                       'other provider identifier issuer_3': '',
                       'other provider identifier_4': '',
                       'other provider identifier type code_4': '',
                       'other provider identifier state_4': '',
                       'other provider identifier issuer_4': '',
                       'other provider identifier_5': '',
                       'other provider identifier type code_5': '',
                       'other provider identifier state_5': '',
                       'other provider identifier issuer_5': '',
                       'other provider identifier_6': '',
                       'other provider identifier type code_6': '',
                       'other provider identifier state_6': '',
                       'other provider identifier issuer_6': '',
                       'other provider identifier_7': '',
                       'other provider identifier type code_7': '',
                       'other provider identifier state_7': '',
                       'other provider identifier issuer_7': '',
                       'other provider identifier_8': '',
                       'other provider identifier type code_8': '',
                       'other provider identifier state_8': '',
                       'other provider identifier issuer_8': '',
                       'other provider identifier_9': '',
                       'other provider identifier type code_9': '',
                       'other provider identifier state_9': '',
                       'other provider identifier issuer_9': '',
                       'other provider identifier_10': '',
                       'other provider identifier type code_10': '',
                       'other provider identifier state_10': '',
                       'other provider identifier issuer_10': '',
                       'other provider identifier_11': '',
                       'other provider identifier type code_11': '',
                       'other provider identifier state_11': '',
                       'other provider identifier issuer_11': '',
                       'other provider identifier_12': '',
                       'other provider identifier type code_12': '',
                       'other provider identifier state_12': '',
                       'other provider identifier issuer_12': '',
                       'other provider identifier_13': '',
                       'other provider identifier type code_13': '',
                       'other provider identifier state_13': '',
                       'other provider identifier issuer_13': '',
                       'other provider identifier_14': '',
                       'other provider identifier type code_14': '',
                       'other provider identifier state_14': '',
                       'other provider identifier issuer_14': '',
                       'other provider identifier_15': '',
                       'other provider identifier type code_15': '',
                       'other provider identifier state_15': '',
                       'other provider identifier issuer_15': '',
                       'other provider identifier_16': '',
                       'other provider identifier type code_16': '',
                       'other provider identifier state_16': '',
                       'other provider identifier issuer_16': '',
                       'other provider identifier_17': '',
                       'other provider identifier type code_17': '',
                       'other provider identifier state_17': '',
                       'other provider identifier issuer_17': '',
                       'other provider identifier_18': '',
                       'other provider identifier type code_18': '',
                       'other provider identifier state_18': '',
                       'other provider identifier issuer_18': '',
                       'other provider identifier_19': '',
                       'other provider identifier type code_19': '',
                       'other provider identifier state_19': '',
                       'other provider identifier issuer_19': '',
                       'other provider identifier_20': '',
                       'other provider identifier type code_20': '',
                       'other provider identifier state_20': '',
                       'other provider identifier issuer_20': '',
                       'other provider identifier_21': '',
                       'other provider identifier type code_21': '',
                       'other provider identifier state_21': '',
                       'other provider identifier issuer_21': '',
                       'other provider identifier_22': '',
                       'other provider identifier type code_22': '',
                       'other provider identifier state_22': '',
                       'other provider identifier issuer_22': '',
                       'other provider identifier_23': '',
                       'other provider identifier type code_23': '',
                       'other provider identifier state_23': '',
                       'other provider identifier issuer_23': '',
                       'other provider identifier_24': '',
                       'other provider identifier type code_24': '',
                       'other provider identifier state_24': '',
                       'other provider identifier issuer_24': '',
                       'other provider identifier_25': '',
                       'other provider identifier type code_25': '',
                       'other provider identifier state_25': '',
                       'other provider identifier issuer_25': '',
                       'other provider identifier_26': '',
                       'other provider identifier type code_26': '',
                       'other provider identifier state_26': '',
                       'other provider identifier issuer_26': '',
                       'other provider identifier_27': '',
                       'other provider identifier type code_27': '',
                       'other provider identifier state_27': '',
                       'other provider identifier issuer_27': '',
                       'other provider identifier_28': '',
                       'other provider identifier type code_28': '',
                       'other provider identifier state_28': '',
                       'other provider identifier issuer_28': '',
                       'other provider identifier_29': '',
                       'other provider identifier type code_29': '',
                       'other provider identifier state_29': '',
                       'other provider identifier issuer_29': '',
                       'other provider identifier_30': '',
                       'other provider identifier type code_30': '',
                       'other provider identifier state_30': '',
                       'other provider identifier issuer_30': '',
                       'other provider identifier_31': '',
                       'other provider identifier type code_31': '',
                       'other provider identifier state_31': '',
                       'other provider identifier issuer_31': '',
                       'other provider identifier_32': '',
                       'other provider identifier type code_32': '',
                       'other provider identifier state_32': '',
                       'other provider identifier issuer_32': '',
                       'other provider identifier_33': '',
                       'other provider identifier type code_33': '',
                       'other provider identifier state_33': '',
                       'other provider identifier issuer_33': '',
                       'other provider identifier_34': '',
                       'other provider identifier type code_34': '',
                       'other provider identifier state_34': '',
                       'other provider identifier issuer_34': '',
                       'other provider identifier_35': '',
                       'other provider identifier type code_35': '',
                       'other provider identifier state_35': '',
                       'other provider identifier issuer_35': '',
                       'other provider identifier_36': '',
                       'other provider identifier type code_36': '',
                       'other provider identifier state_36': '',
                       'other provider identifier issuer_36': '',
                       'other provider identifier_37': '',
                       'other provider identifier type code_37': '',
                       'other provider identifier state_37': '',
                       'other provider identifier issuer_37': '',
                       'other provider identifier_38': '',
                       'other provider identifier type code_38': '',
                       'other provider identifier state_38': '',
                       'other provider identifier issuer_38': '',
                       'other provider identifier_39': '',
                       'other provider identifier type code_39': '',
                       'other provider identifier state_39': '',
                       'other provider identifier issuer_39': '',
                       'other provider identifier_40': '',
                       'other provider identifier type code_40': '',
                       'other provider identifier state_40': '',
                       'other provider identifier issuer_40': '',
                       'other provider identifier_41': '',
                       'other provider identifier type code_41': '',
                       'other provider identifier state_41': '',
                       'other provider identifier issuer_41': '',
                       'other provider identifier_42': '',
                       'other provider identifier type code_42': '',
                       'other provider identifier state_42': '',
                       'other provider identifier issuer_42': '',
                       'other provider identifier_43': '',
                       'other provider identifier type code_43': '',
                       'other provider identifier state_43': '',
                       'other provider identifier issuer_43': '',
                       'other provider identifier_44': '',
                       'other provider identifier type code_44': '',
                       'other provider identifier state_44': '',
                       'other provider identifier issuer_44': '',
                       'other provider identifier_45': '',
                       'other provider identifier type code_45': '',
                       'other provider identifier state_45': '',
                       'other provider identifier issuer_45': '',
                       'other provider identifier_46': '',
                       'other provider identifier type code_46': '',
                       'other provider identifier state_46': '',
                       'other provider identifier issuer_46': '',
                       'other provider identifier_47': '',
                       'other provider identifier type code_47': '',
                       'other provider identifier state_47': '',
                       'other provider identifier issuer_47': '',
                       'other provider identifier_48': '',
                       'other provider identifier type code_48': '',
                       'other provider identifier state_48': '',
                       'other provider identifier issuer_48': '',
                       'other provider identifier_49': '',
                       'other provider identifier type code_49': '',
                       'other provider identifier state_49': '',
                       'other provider identifier issuer_49': '',
                       'other provider identifier_50': '',
                       'other provider identifier type code_50': '',
                       'other provider identifier state_50': '',
                       'other provider identifier issuer_50': '',
                       'is sole proprietor': '',
                       'is organization subpart': '',
                       'parent organization lbn': '',
                       'parent organization tin': '',
                       'authorized official name prefix text': '',
                       'authorized official name suffix text': '',
                       'authorized official credential text': '',
                       'healthcare provider taxonomy group_1': '',
                       'healthcare provider taxonomy group_2': '',
                       'healthcare provider taxonomy group_3': '',
                       'healthcare provider taxonomy group_4': '',
                       'healthcare provider taxonomy group_5': '',
                       'healthcare provider taxonomy group_6': '',
                       'healthcare provider taxonomy group_7': '',
                       'healthcare provider taxonomy group_8': '',
                       'healthcare provider taxonomy group_9': '',
                       'healthcare provider taxonomy group_10': '',
                       'healthcare provider taxonomy group_11': '',
                       'healthcare provider taxonomy group_12': '',
                       'healthcare provider taxonomy group_13': '',
                       'healthcare provider taxonomy group_14': '',
                       'healthcare provider taxonomy group_15': ''
                       }


def create_directory_if_not_exist(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def convert_date_value(obj, property_name):
    if property_name in obj:
        date_value = obj[property_name]
        if date_value is not None and date_value != '':
            obj[property_name] = datetime.strptime(date_value, '%Y-%m-%d').strftime("%Y%m%d")


def set_value(obj, property_name, redshift_column):
    if property_name in obj:
        npi_data_dictionary[redshift_column] = str(obj[property_name])


def populate_npi_dictionary(json_data):
    set_value(json_data, 'number', 'npi')
    npi_type_code = '1'
    if json_data['enumeration_type'] == 'NPI-2':
        npi_type_code = '2'
    json_data['enumeration_type'] = npi_type_code
    set_value(json_data, 'enumeration_type', 'entity type code')

    basic = json_data['basic']
    set_value(basic, 'replacement_npi', 'replacement npi')
    set_value(basic, 'ein', 'employer identification number (ein)')
    set_value(basic, 'organization_name', 'provider organization name (legal business name)')
    set_value(basic, 'last_name', 'provider last name (legal name)')
    set_value(basic, 'first_name', 'provider first name')
    set_value(basic, 'middle_name', 'provider middle name')
    set_value(basic, 'name_prefix', 'provider name prefix text')
    set_value(basic, 'name_suffix', 'provider name suffix text')
    set_value(basic, 'credential', 'provider credential text')

    if 'other_names' in json_data and len(json_data['other_names']) > 0:
        other_names = json_data['other_names'][0]
        set_value(other_names, 'organization_name', 'provider other organization name')
        set_value(other_names, 'code', 'provider other organization name type code')
        set_value(other_names, 'last_name', 'provider other last name')
        set_value(other_names, 'first_name', 'provider other first name')
        set_value(other_names, 'middle_name', 'provider other middle name')
        set_value(other_names, 'prefix', 'provider other name prefix text')
        set_value(other_names, 'suffix', 'provider other name suffix text')
        set_value(other_names, 'credential', 'provider other credential text')
        set_value(other_names, 'code', 'provider other last name type code')

    if 'addresses' in json_data and len(json_data['addresses']) > 1:
        addresses = json_data['addresses'][1]
        set_value(addresses, 'address_1', 'provider first line business mailing address')
        set_value(addresses, 'address_2', 'provider second line business mailing address')
        set_value(addresses, 'city', 'provider business mailing address city name')
        set_value(addresses, 'state', 'provider business mailing address state name')
        set_value(addresses, 'postal_code', 'provider business mailing address postal code')
        set_value(addresses, 'country_code', 'provider business mailing address country code (if outside u s )')
        set_value(addresses, 'telephone_number', 'provider business mailing address telephone number')
        set_value(addresses, 'fax_number', 'provider business mailing address fax number')

    if 'addresses' in json_data and len(json_data['addresses']) > 0:
        addresses = json_data['addresses'][0]
        set_value(addresses, 'address_1', 'provider first line business practice location address')
        set_value(addresses, 'address_2', 'provider second line business practice location address')
        set_value(addresses, 'city', 'provider business practice location address city name')
        set_value(addresses, 'state', 'provider business practice location address state name')
        set_value(addresses, 'postal_code', 'provider business practice location address postal code')
        set_value(addresses, 'country_code', 'provider business practice location address country code (if outside u s )')
        set_value(addresses, 'telephone_number', 'provider business practice location address telephone number')
        set_value(addresses, 'fax_number', 'provider business practice location address fax number')

    convert_date_value(basic, 'enumeration_date')
    set_value(basic, 'enumeration_date', 'provider enumeration date')

    convert_date_value(basic, 'last_updated')
    set_value(basic, 'last_updated', 'last update date')

    set_value(basic, 'deactivation_reason_code', 'npi deactivation reason code')

    convert_date_value(basic, 'deactivation_date')
    set_value(basic, 'deactivation_date', 'npi deactivation date')
    convert_date_value(basic, 'reactivation_date')
    set_value(basic, 'reactivation_date', 'npi reactivation date')

    set_value(basic, 'gender', 'provider gender code')
    set_value(basic, 'authorized_official_last_name', 'authorized official last name')
    set_value(basic, 'authorized_official_first_name', 'authorized official first name')
    set_value(basic, 'authorized_official_middle_name', 'authorized official middle name')
    set_value(basic, 'authorized_official_title_or_position', 'authorized official title or position')
    set_value(basic, 'authorized_official_telephone_number', 'authorized official telephone number')

    count = 1
    if 'taxonomies' in json_data:
        for item in json_data['taxonomies']:
            set_value(item, 'code', 'healthcare provider taxonomy code_{0}'.format(count))
            set_value(item, 'license', 'provider license number_{0}'.format(count))
            set_value(item, 'state', 'provider license number state code_{0}'.format(count))
            set_value(item, 'primary', 'healthcare provider primary taxonomy switch_{0}'.format(count))
            set_value(item, 'taxonomy_group', 'healthcare provider taxonomy group_{0}'.format(count))
            count += 1

    count = 1
    if 'identifiers' in json_data:
        for item in json_data['identifiers']:
            set_value(item, 'identifier', 'other provider identifier_{0}'.format(count))
            set_value(item, 'code', 'other provider identifier type code_{0}'.format(count))
            set_value(item, 'state', 'other provider identifier state_{0}'.format(count))
            set_value(item, 'issuer', 'other provider identifier issuer_{0}'.format(count))
            count += 1

    set_value(basic, 'sole_proprietor', 'is sole proprietor')
    set_value(basic, 'organizational_subpart', 'is organization subpart')
    set_value(basic, 'parent_organization_legal_business_name', 'parent organization lbn')
    set_value(basic, 'parent_organization_ein', 'parent organization tin')
    set_value(basic, 'authorized_official_name_prefix', 'authorized official name prefix text')
    set_value(basic, 'authorized_official_name_suffix', 'authorized official name suffix text')
    set_value(basic, 'authorized_official_credential', 'authorized official credential text')
