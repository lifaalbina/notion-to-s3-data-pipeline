import json
from typing import Dict, List, Union

REQUIRED_PROPERTIES = [
    'title', 'rich_text', 'date', 'select',
    'multi_select', 'number', 'checkbox', 'url'
    ]


def fetch_database_items(notion, database_id: str) -> List[Dict]:
    """
    Запросить элементы базы данных из Notion.

    :param database_id: Идентификатор базы данных в Notion.
    :return: Список элементов базы данных.
    """
    response = notion.databases.query(database_id=database_id)
    return response['results']


def extract_properties(item: Dict) -> Dict:
    """
    Извлечь свойства элемента базы данных Notion.

    :param item: Объект элемента базы данных Notion.
    :return: Словарь с извлеченными свойствами элемента.
    """
    properties = item['properties']
    extracted_data = {'id': item['id']}
    property_types = {}

    for prop_name, prop_values in properties.items():
        prop_type = prop_values['type']

        if prop_type in REQUIRED_PROPERTIES:
            if prop_type == 'title' or prop_type == 'rich_text':
                extracted_data[prop_name] = get_text(prop_values[prop_type])
                property_types[prop_name] = 'string'
            elif prop_type == 'date':
                start_date, end_date = get_date(prop_values.get('date', {}))
                extracted_data[f'{prop_name}_start'] = start_date
                extracted_data[f'{prop_name}_end'] = end_date
                property_types[f'{prop_name}_start'] = 'string'
                property_types[f'{prop_name}_end'] = 'string'
            elif prop_type == 'select':
                extracted_data[prop_name] = get_select(prop_values['select'])
                property_types[prop_name] = 'string'
            elif prop_type == 'multi_select':
                extracted_data[prop_name] = get_multi_select(
                    prop_values['multi_select']
                    )
                property_types[prop_name] = 'array'
            elif prop_type == 'url':
                extracted_data[prop_name] = prop_values.get(prop_type)
                property_types[prop_name] = 'string'
            elif prop_type == 'number':
                extracted_data[prop_name] = prop_values.get(prop_type)
                property_types[prop_name] = 'int64'
            elif prop_type == 'checkbox':
                extracted_data[prop_name] = prop_values.get(prop_type)
                property_types[prop_name] = 'bool'

    with open('property_types.json', 'w+') as f:
        json.dump(property_types, f)
    return extracted_data


def get_text(text_object: List[Dict]) -> str:
    """
    Получить текст из объекта текста базы данных Notion.

    :param text_object: Объект текста из базы данных Notion.
    :return: Объединенный текст.
    """

    text = ''
    for rt in text_object:
        text += rt.get('plain_text')
    return text


def get_date(date_object: Dict) -> tuple:
    """
    Получить дату или диапазон дат из объекта даты базы данных Notion.

    :param date_object: Объект даты из базы данных Notion.
    :return: Одиночная дата или кортеж с начальной и конечной датами.
    """

    start_date = date_object.get('start', None) if date_object else None
    end_date = date_object.get('end', None) if date_object else None
    return start_date, end_date


def get_select(select_object: Dict) -> Union[str, None]:
    """
    Получить имя выбранного элемента из объекта выбора базы данных Notion.

    :param select_object: Объект выбора из базы данных Notion.
    :return: Имя выбранного элемента или None, если объект отсутствует.
    """

    return select_object.get('name') if select_object else None


def get_multi_select(multi_select_object: List[Dict]) -> List[str]:
    """
    Получить список строк, объединяющих элементы выбора из базы данных Notion.

    :param multi_select_object: Список объектов выбора из базы данных Notion.
    :return: Список строк, объединяющих элементы выбора.
    """

    return [', '.join([item.get('name', '') for item in multi_select_object])]


def reorder_properties(data: List[Dict], order: List[str]) -> List[Dict]:
    """
    Переставить свойства в каждом словаре списка данных в заданном порядке.

    :param data: Список словарей с данными.
    :param order: Список строк, определяющий порядок свойств.
    :return: Новый список словарей с переставленными свойствами.
    """
    return [{key: item.get(key) for key in order if key in item}
            for item in data]


def reorder_types(data: Dict, order: List[str]) -> List[Dict]:
    """
    Переставить свойства в словаре данных в заданном порядке.

    :param data: Словарь с данными.
    :param order: Список строк, определяющий порядок свойств.
    :return: Новый словарь с переставленными свойствами.
    """
    return {key: data.get(key) for key in order if key in data}
