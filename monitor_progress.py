import os
import gzip
import time
import tqdm
import json


def list_downloaded():
    items = os.listdir("output/")
    return [x.replace("|", "/") for x in items]


def load_json_gz(file_list):
    for filename in file_list:
        with gzip.open(filename, "rb") as f:
            content = f.read().decode("utf-8")
        for line in content.strip().split("\n"):
            yield json.loads(line)


INFO_DIR = "coq_repo_index/"


if __name__ == "__main__":
    repo_infos = load_json_gz(os.path.join(INFO_DIR, x) for x in os.listdir(INFO_DIR))
    repo_names = [
        repo["full_name"] for repo in repo_infos if repo["visibility"] == "public"
    ]

    pbar = tqdm.tqdm(total=len(repo_names), desc="Download")
    current = 0
    while True:
        downloaded = len(list_downloaded())
        pbar.update(downloaded - current)
        current = downloaded
        time.sleep(1)
