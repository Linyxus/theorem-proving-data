'''
This file is based on EleutherAI/github-downloader/download_repos.py.
'''

import os
import csv
import json
import gzip
from tqdm import tqdm
from joblib import Parallel, delayed

from rich.console import Console

console = Console()

def download_repo(repo: str):
    file_name = repo.replace('/', '|')
    # file_name = repo.split("/")[-1]
    if file_name not in os.listdir("output/"):
        os.system(f'git clone --depth 1 --single-branch https://github.com/{repo} "output/{file_name}"')
    else:
        console.log(f"Already downloaded {repo}")

def load_json_gz(file_list):
    for filename in file_list:
        with gzip.open(filename, 'rb') as f:
            content = f.read().decode('utf-8')
        for line in content.strip().split('\n'):
            yield json.loads(line)

INFO_DIR = 'coq_repo_index/'

repo_infos = load_json_gz(os.path.join(INFO_DIR, x) for x in os.listdir(INFO_DIR))

repo_names = [repo['full_name'] for repo in repo_infos if repo['visibility'] == 'public']

console.log(f"Loaded {len(repo_names)} reposoitory names from {INFO_DIR}")

if 'output' not in os.listdir():
    os.makedirs('output')


Parallel(n_jobs=40, prefer="threads")(
    delayed(download_repo)(name) for name in tqdm(repo_names))
