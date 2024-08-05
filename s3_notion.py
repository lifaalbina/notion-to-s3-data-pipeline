import time
from random import randint

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import BaseClient


def get_notion_schema(fields_dict) -> pa.Schema:
    """
    Создать схему таблицы Notion на основе словаря полей и их типов.

    :param fields_dict: Словарь: ключи - имена полей, значения - типы полей.
    :return: Схема таблицы Notion.
    """

    notion_schema = pa.schema([])
    notion_schema = notion_schema.append(pa.field('id',
                                                  pa.string(),
                                                  nullable=True)
                                         )

    for field_name, field_type in fields_dict.items():
        if field_type == 'string':
            notion_schema = notion_schema.append(pa.field(field_name,
                                                          pa.string(),
                                                          nullable=True)
                                                 )
        elif field_type == 'int64':
            notion_schema = notion_schema.append(pa.field(field_name,
                                                          pa.int64(),
                                                          nullable=True)
                                                 )
        elif field_type == 'bool':
            notion_schema = notion_schema.append(pa.field(field_name,
                                                          pa.bool_(),
                                                          nullable=True)
                                                 )
        elif field_type == 'array':
            notion_schema = notion_schema.append(pa.field(
                field_name,
                pa.list_(pa.string()),
                nullable=True))

    return notion_schema


def _get_s3_client(
        aws_access_key_id: str,
        aws_secret_access_key: str,
        yc_object_storage_url: str) -> BaseClient:
    """
    Создать и вернуть клиент S3 для работы с объектным хранилищем.

    :param aws_access_key_id: Идентификатор ключа доступа AWS.
    :param aws_secret_access_key: Секретный ключ доступа AWS.
    :param yc_object_storage_url: URL объектного хранилища Yandex Cloud.
    :return: Клиент S3.
    """

    boto_session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='ru-central1')
    s3 = boto_session.client(
        service_name='s3',
        endpoint_url=yc_object_storage_url)
    return s3


def clear_s3_folder(
        aws_access_key_id: str,
        aws_secret_access_key: str,
        yc_object_storage_url: str,
        yc_object_storage_bucket_name: str,
        prefix: str) -> dict:
    """
    Удалить объект из указанного префикса в объектном хранилище S3.

    :param aws_access_key_id: Идентификатор ключа доступа AWS.
    :param aws_secret_access_key: Секретный ключ доступа AWS.
    :param yc_object_storage_url: URL объектного хранилища Yandex Cloud.
    :param yc_object_storage_bucket_name: Имя бакета в объектном
    хранилище Yandex Cloud.
    :param prefix: Префикс объекта для удаления.
    :return: Ответ от S3 клиента в виде словаря.
    """

    s3 = _get_s3_client(
        aws_access_key_id,
        aws_secret_access_key,
        yc_object_storage_url
    )
    bucket_name = yc_object_storage_bucket_name
    print(f'Очистка объекта с ключом {prefix}')

    try:
        response = s3.delete_object(Bucket=bucket_name, Key=prefix)
        print('Объект успешно удален.')
        return response

    except s3.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print('Объект не найден (404).')
            return {'Deleted': []}
        else:
            print(f'Ошибка: {e}')
            return {'Error': str(e)}


def upload_to_s3_as_parquet(
        data: 'list[dict]',
        filename: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        yc_object_storage_url: str,
        yc_object_storage_bucket_name: str,
        schema: dict) -> dict:
    """
    Загрузить данные в формате Parquet в указанный бакет S3.

    :param data: Данные в формате списка словарей для загрузки.
    :param filename: Имя файла для сохранения в S3.
    :param aws_access_key_id: Идентификатор ключа доступа AWS.
    :param aws_secret_access_key: Секретный ключ доступа AWS.
    :param yc_object_storage_url: URL объектного хранилища Yandex Cloud.
    :param yc_object_storage_bucket_name: Имя бакета в объектном
    хранилище Yandex Cloud.
    :param schema: Схема данных в формате `dict` для преобразования в Parquet.
    :return: Ответ от S3 клиента в виде словаря.
    """

    s3 = _get_s3_client(
        aws_access_key_id,
        aws_secret_access_key,
        yc_object_storage_url
    )
    bucket_name = yc_object_storage_bucket_name

    print(schema.to_string())
    table = pa.Table.from_pylist(data, schema=schema)
    writer = pa.BufferOutputStream()
    pq.write_table(table, writer)
    time.sleep(randint(1, 5))
    s3_filename = f'{filename}.parquet'
    print('Данные отправлены в S3.')
    print(f'Имя файла: {s3_filename}')
    return s3.put_object(
        Body=bytes(writer.getvalue()),
        Bucket=bucket_name,
        Key=s3_filename)
