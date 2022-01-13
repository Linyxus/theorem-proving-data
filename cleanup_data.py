import math
import shutil
from typing import *
import os
import gzip
import json

from rich.progress import track
from rich import print

import ray


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


def contain_verilog_macro(content: str):
    macro_list = [
        '`ifdef',
        '`ifndef',
        '`endif',
        '`include',
        '`timescale',
        '`define'
    ]
    for line in content.split('\n'):
        for macro in macro_list:
            if line.startswith(macro):
                return True
    return False


def contain_verilog_module(content: str):
    has_module_start = False
    for line in content.split('\n'):
        if line.startswith('module '):
            has_module_start = True
        if line.startswith('endmodule') and has_module_start:
            return True
    return False


def is_verilog_file(content: str) -> bool:
    return contain_verilog_macro(content) or contain_verilog_module(content)


def is_good_file(content: str) -> bool:
    if len(content) > 100 * 1024:
        return False

    if len(content) < 10:
        return False

    if len([c for c in content if c.isalnum()]) / len(content) < 0.25:
        return False

    if max([len(line) for line in content]) > 1000:
        return False

    def mean(xs):
        return sum(xs) / len(xs)

    if mean([len(line) for line in content]) > 100:
        return False

    return True


if __name__ == '__main__':
    ray.init('auto')
    INPUT_DIR = '/dataset/fd5061f6/yichen/theorem_proving_data/coq_code'
    OUTPUT_DIR = '/dataset/fd5061f6/yichen/theorem_proving_data/coq_code.cleaned'

    if os.path.exists(OUTPUT_DIR):
        input(f"Will DELETE existing output, continue?")
        shutil.rmtree(OUTPUT_DIR)

    raw_dataset = []
    data_files = list(list_data_files(INPUT_DIR))
    for data_file in track(data_files, description="Loading raw data"):
        for x in stream_jsonl(data_file):
            raw_dataset.append(x)

    ds = ray.data.from_items(raw_dataset)
    print(f"Loaded dataset: {ds}")

    num_workers = math.floor(ray.available_resources()['CPU'])
    print(f"Using {num_workers} workers")

    ds = ds.repartition(num_workers)
    print(f"Re-partitioned: {ds}")

    ds = ds.filter(lambda x: is_good_file(x['content']) and not is_verilog_file(x['content']))
    print(f"Filtered: {ds}")

    ds.repartition(10).write_parquet(OUTPUT_DIR)
    print(f"Saved. Done!")
