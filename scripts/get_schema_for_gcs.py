import argparse
from typing import Any, Generator, Iterator
from bq_schema_generator import SchemaGenerator
from google.cloud import storage
import json


def get_json_records(
    client: storage.Client, bucket_name: str, path: str
) -> Generator[dict[str, Any], None, None]:
    bucket = client.bucket(bucket_name)
    blobs: Iterator[storage.Blob] = bucket.list_blobs(prefix=path)
    json_blobs = [blob for blob in blobs if blob.name.endswith(".json")]  # type: ignore
    for blob in json_blobs:
        print(f"Processing {blob.name}")

        json_records = json.loads(blob.download_as_text())

        for json_record in json_records:
            yield json_record


def get_jsonl_records(
    client: storage.Client, bucket_name: str, path: str
) -> Generator[dict[str, Any], None, None]:
    bucket = client.bucket(bucket_name)
    blobs: Iterator[storage.Blob] = bucket.list_blobs(prefix=path)
    json_blobs = [blob for blob in blobs if blob.name.endswith(".jsonl")]  # type: ignore
    for blob in json_blobs:
        print(f"Processing {blob.name}")

        json_records_str = blob.download_as_text().split("\n")
        for json_record_str in json_records_str:
            if json_record_str:
                yield json.loads(json_record_str)


def run(input_dir: str, output_dir: str):
    input_bucket_name, input_path = input_dir.split("/", 1)
    output_bucket_name, output_path = output_dir.split("/", 1)

    client = storage.Client()
    schema_generator = SchemaGenerator(use_int_in_hierarchy=True)

    for record in get_json_records(client, input_bucket_name, input_path):
        schema_generator.update_schema_columns([record])

    for record in get_jsonl_records(client, input_bucket_name, input_path):
        schema_generator.update_schema_columns([record])

    schema = schema_generator.get_bq_schema()
    print(schema)

    client.bucket(output_bucket_name).blob(output_path).upload_from_string(
        json.dumps(schema, indent=2)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", dest="input_dir", required=True)
    parser.add_argument("--output-path", dest="output_path", required=True)

    args = parser.parse_args()

    run(input_dir=args.input_dir, output_dir=args.output_dir)
