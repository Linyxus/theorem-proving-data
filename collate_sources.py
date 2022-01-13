"""Collecting all source files and turn them into json blocks."""
from typing import *
import os
from dataclasses import dataclass, asdict
import fire
import tqdm
import json
import gzip
from rich.console import Console
import copy
import chardet

console = Console()


@dataclass
class AppState:
    input_dir: str
    output_dir: str
    repo_list: List[str]
    file_ext: str
    buffer: List[dict]
    current_repo_id: int = 0
    next_block_id: int = 0
    buffer_size: int = 1000


def get_repo_list(root_dir: str) -> List[str]:
    return [x.replace("|", "/") for x in os.listdir(root_dir)]


def init_app_state(input_dir: str, output_dir: str, buffer_size: int, file_ext: str):
    repo_list = get_repo_list(input_dir)
    return AppState(
        input_dir=input_dir,
        output_dir=output_dir,
        repo_list=repo_list,
        file_ext=file_ext,
        buffer=[],
        current_repo_id=0,
        next_block_id=0,
        buffer_size=buffer_size,
    )


def output_json_block(json_list: List[dict], output_path: str):
    with gzip.open(output_path, "wb") as f:
        for obj in json_list:
            line = json.dumps(obj) + "\n"
            f.write(line.encode("utf-8"))


def save_state(state: AppState, data_dir: str):
    save_path = os.path.join(data_dir, "state.json")
    with open(save_path, "w") as f:
        json.dump(asdict(state), f)


def load_state(data_dir: str) -> Optional[AppState]:
    load_path = os.path.join(data_dir, "state.json")
    if not os.path.exists(load_path):
        return None

    with open(load_path, "r") as f:
        obj = json.load(f)
        return AppState(**obj)


def list_source_files(repo_dir: str, file_ext: str = ".v"):
    for root, _, files in os.walk(repo_dir):
        for name in files:
            if name.endswith(file_ext):
                yield os.path.join(root, name), name


def extract_repo_source(data_dir: str, repo_path: str, file_ext: str = ".v") -> Iterable[dict]:
    repo_dir = os.path.join(data_dir, repo_path.replace("/", "|"))
    for real_path, file_path in list_source_files(repo_dir, file_ext=file_ext):
        try:
            with open(real_path, "rb") as f:
                content = f.read()
            enc_detect = chardet.detect(content)
            enc = enc_detect["encoding"]
            if enc is None:
                print(
                    f"Could not guess encoding for file {real_path} ({enc_detect}), try utf-8."
                )
            enc = "utf-8"
            yield {
                "content": content.decode(enc),
                "file_path": file_path,
                "repo_path": repo_path,
                "encoding": enc,
            }
        except BaseException as e:
            print(f"Failed to read file {real_path}, error: {e}")


def step(state: AppState) -> AppState:
    # Do nothing if program is ended
    if state.current_repo_id >= len(state.repo_list):
        return state

    repo = state.repo_list[state.current_repo_id]
    repo_sources = list(extract_repo_source(state.input_dir, repo, file_ext=state.file_ext))

    next_state: AppState = copy.copy(state)
    next_state.current_repo_id += 1
    next_state.buffer = next_state.buffer + repo_sources

    # output when buffer is full
    if len(next_state.buffer) >= next_state.buffer_size:
        output_path = os.path.join(
            state.output_dir, f"block_{next_state.next_block_id:05d}.json.gz"
        )
        output_json_block(next_state.buffer[: next_state.buffer_size], output_path)
        next_state.buffer = next_state.buffer[next_state.buffer_size :]
        next_state.next_block_id += 1

    # output at last repo
    if (
        next_state.current_repo_id == len(next_state.repo_list)
        and len(next_state.buffer) > 0
    ):
        output_path = os.path.join(
            state.output_dir, f"block_{next_state.next_block_id:05d}.json.gz"
        )
        output_json_block(next_state.buffer, output_path)
        next_state.buffer = []
        next_state.next_block_id += 1

    return next_state


def app(
    input_dir: str = "./coq_repo",
    output_dir: str = "./processed_data",
    buffer_size: str = 1000,
    file_ext: str = ".v",
):
    state = load_state(output_dir)
    if state is None:
        state = init_app_state(input_dir, output_dir, buffer_size, file_ext)
        console.log(f"Initialized from scratch, current repo={state.current_repo_id}")
    else:
        console.log(f"Resumed from checkpoint, current repo={state.current_repo_id}")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    pb = tqdm.tqdm(total=len(state.repo_list))
    pb.update(state.current_repo_id)

    while state.current_repo_id < len(state.repo_list):
        state = step(state)

        if (state.current_repo_id + 1) % 100 == 0:
            save_state(state, output_dir)
        pb.update(1)

    pb.close()


if __name__ == "__main__":
    fire.Fire(app)
