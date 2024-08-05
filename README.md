# Notion to S3 Data Pipeline

## Описание
Этот проект предназначен для извлечения данных из базы данных Notion, их обработки и загрузки в хранилище S3 в формате Parquet. 

## Требования
- Python 3.8+
- Notion API Client
- Boto3
- PyArrow
- Pyyaml
- python-dotenv

## Установка

1. Клонируйте репозиторий:
    ```bash
    git clone https://github.com/yourusername/notion-to-s3-data-pipeline.git
    cd notion-to-s3-data-pipeline
    ```

2. Установите необходимые зависимости:
    ```bash
    pip install -r requirements.txt
    ```

3. Создайте файл `.env` и добавьте следующие переменные окружения:
    ```
    AWS_ACCESS_KEY_ID=ваш_aws_access_key_id
    AWS_SECRET_ACCESS_KEY=ваш_aws_secret_access_key
    YC_OBJECT_STORAGE_URL=ваш_yc_object_storage_url
    YC_OBJECT_STORAGE_BUCKET_NAME=ваш_yc_object_storage_bucket_name
    DATABASE_ID=ваш_database_id
    NOTION_API_KEY=ваш_notion_api_key
    ```

4. Создайте файл `config.yaml` для указания порядка свойств:
    ```yaml
    property_order:
        - title
        - date_start
        - date_end
        - select
        - multi_select
        - number
        - checkbox
        - url
    ```

## Использование
Запустите скрипт `main.py` для выполнения всего процесса:
```bash
python main.py