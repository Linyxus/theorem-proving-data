from typing import *
import os
import gzip
import json

from rich.progress import track


def list_data_files(data_dir: str):
    for root, dirs, files in os.walk(data_dir):
        for filename in files:
            if filename.endswith('.json.gz'):
                yield os.path.join(root, filename)


def stream_jsonl(data_file: str) -> Iterable[dict]:
    with gzip.open(data_file, 'rb') as f:
        content = f.read().decode('utf-8')
    for line in content.strip().split('\n'):
        yield json.loads(line)


if __name__ == '__main__':
    DATA_DIR = '/dataset/fd5061f6/yichen/theorem_proving_data/coq_code'

    dataset = []
    repo_paths = set()
    data_files = list(list_data_files(DATA_DIR))
    for data_file in track(data_files):
        for x in stream_jsonl(data_file):
            dataset.append(x)
            repo_paths.add(x['repo_path'])
