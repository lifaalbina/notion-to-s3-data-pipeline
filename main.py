import json
import os

import yaml
from dotenv import load_dotenv
from notion_client import Client

from notion import (extract_properties, fetch_database_items,
                    reorder_properties, reorder_types)
from s3_notion import (clear_s3_folder, get_notion_schema,
                       upload_to_s3_as_parquet)

load_dotenv()


def main():
    """
    Основная функция скрипта для работы с базой данных Notion.

    :return: None
    """
    global database_id
    global notion
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    yc_object_storage_url = os.getenv('YC_OBJECT_STORAGE_URL')
    yc_object_storage_bucket_name = os.getenv('YC_OBJECT_STORAGE_BUCKET_NAME')
    database_id = os.getenv('DATABASE_ID')
    notion_api_key = os.getenv('NOTION_API_KEY')
    notion = Client(auth=notion_api_key)
    print('Запуск main функции.')
    print('Получаем элементы из базы данных Notion.')
    items = fetch_database_items(notion, database_id)

    try:
        if items:
            print('Извлекаем свойства элементов базы данных.')
            extracted_items = [extract_properties(item) for item in items]
            print(f'Свойства получены: {extracted_items}')

            def load_config(config_path: str) -> dict:
                """Получить список упорядоченных свойств из конфига."""
                with open(config_path, 'r', encoding='utf-8') as file:
                    return yaml.safe_load(file)

            config = load_config('config.yaml')
            property_order = config['property_order']

            print('Формируем упорядоченный список в соответствии с конфигом.')
            reordered_data = reorder_properties(extracted_items,
                                                property_order
                                                )
            print(f'Список получен: {reordered_data}')

            with open('property_types.json') as f:
                property_types = json.load(f)

            print('Формируем упорядоченный словарь типов для создания схемы.')
            property_type = reorder_types(property_types, property_order)
            print(f'Словарь получен: {property_type}')

            print('Создаем схему.')
            notion_schema = get_notion_schema(property_type)
            print(f'Схема создана: {notion_schema}')

            # Очистка папки в S3 перед загрузкой
            clear_result = clear_s3_folder(
                aws_access_key_id,
                aws_secret_access_key,
                yc_object_storage_url,
                yc_object_storage_bucket_name,
                prefix='notion/'
            )

            # Загрузка данных в S3 в формате Parquet
            upload_result = upload_to_s3_as_parquet(
                reordered_data,
                'notion',
                aws_access_key_id,
                aws_secret_access_key,
                yc_object_storage_url,
                yc_object_storage_bucket_name,
                notion_schema
            )

            upload_status = upload_result['ResponseMetadata']['HTTPStatusCode']
            print(f'Upload status: {upload_status}')

        else:
            print('Не удалось получить данные из базы данных')

    except Exception as e:
        print(f'Произошла ошибка {e}')


if __name__ == '__main__':
    main()
