import argparse
import os
import requests
import time
from time import perf_counter
import gzip
import math
import json
from datetime import datetime, timedelta


class GithubQueryScraper(object):
    def __init__(
        self,
        lang: str,
        page_size: int,
        delta_hours: int,
        start_date: str,
        end_date: str,
        block_size: int,
        output_dir: str,
        max_req_per_min: int = 10,
    ):
        self.lang = lang
        self.page_size = page_size
        self.block_size = block_size
        self.output_dir = output_dir
        self.max_req_per_min = max_req_per_min

        self.block_id = 1
        self.acc = []

        self.start_date = datetime.fromisoformat(start_date)
        self.end_date = datetime.fromisoformat(end_date)

        self.current_date = self.start_date
        self.time_delta = timedelta(hours=delta_hours)

    def __str__(self):
        res = (
            f"=== GithubQueryScraper ===\n"
            f"page_size = {self.page_size}\n"
            f"block_size = {self.block_size}\n"
            f"output_dir = {self.output_dir}\n"
            f"max_req_rate = {self.max_req_per_min} per minute\n"
            f"current_block_id = {self.block_id}\n"
            f"start = {self.start_date.isoformat()}\n"
            f"end = {self.end_date.isoformat()}\n"
            f"time_slot = {self.time_delta}\n"
            f"current_date_range = {self.current_date.isoformat()} .. {(self.current_date + self.time_delta).isoformat()}\n"
            f"=== GithubQueryScraper ==="
        )
        return res

    def reach_end_date(self):
        return self.current_date >= self.end_date

    def step_date(self):
        if not self.reach_end_date():
            self.current_date = self.current_date + self.time_delta

    def build_url(self, page_num: int):
        start_time = (self.current_date - self.time_delta).isoformat()
        end_time = self.current_date.isoformat()
        query_url = (
            f"https://api.github.com/search/repositories?q=language:{self.lang}+created:{start_time}..{end_time}"
            f"&sort=stars&order=desc&"
            f"per_page={self.page_size}&page={page_num}"
        )
        return query_url

    def get_page_items_with_retry(self, page_num: int, max_retry: int = 10):
        if max_retry <= 0:
            raise RuntimeError(
                f"fetching page {page_num} failed with maximal retry number reached"
            )

        response = None
        try:
            response = self.get_page(page_num)
            items = response["items"]
            return items
        except Exception as e:
            print(f"caught error when fetching page {page_num}: {e}")
            if response is not None:
                print(f"response is {response}")
            time.sleep(60 / self.max_req_per_min)
            return self.get_page_items_with_retry(page_num, max_retry=max_retry - 1)

    def get_page_count_with_retry(self, page_num: int, max_retry: int = 10):
        if max_retry <= 0:
            raise RuntimeError(
                f"fetching page {page_num} failed with maximal retry number reached"
            )

        response = None
        try:
            response = self.get_page(page_num)
            total_count = response["total_count"]
            return total_count
        except Exception as e:
            print(f"caught error when fetching page {page_num}: {e}")
            if response is not None:
                print(f"response is {response}")
            time.sleep(60 / self.max_req_per_min)
            return self.get_page_count_with_retry(page_num, max_retry=max_retry - 1)

    def get_page(self, page_num: int):
        return requests.get(self.build_url(page_num)).json()

    def log_results(self, items):
        self.acc = self.acc + items
        if len(self.acc) >= self.block_size:
            print(f"outputing block {self.block_id}")
            t = self.output_block()
            print(f"output done, took {t:.4f}")

    def output_block(self):
        save_path = os.path.join(self.output_dir, f"block_{self.block_id}.jsonl.gz")
        tic = perf_counter()
        with gzip.open(save_path, "w") as f:
            for item in self.acc:
                text = json.dumps(item) + "\n"
                f.write(text.encode("utf-8"))
        toc = perf_counter()

        self.block_id += 1
        self.acc = []

        return toc - tic

    def scrape_current_period(self):
        total_count = self.get_page_count_with_retry(1)
        start_time = self.current_date.isoformat()
        end_time = (self.current_date + self.time_delta).isoformat()
        print(f"fetching time period {start_time} .. {end_time}")
        num_steps = math.ceil(total_count / self.page_size)
        num_steps = min(num_steps, math.ceil(1000 / self.page_size))
        sleep_time = 60 / self.max_req_per_min
        print(f"total count = {total_count}")
        print(f"num steps = {num_steps}")
        for i in range(1, num_steps + 1):
            time.sleep(sleep_time)
            items = self.get_page_items_with_retry(i)
            print(
                f"page {i} / {num_steps}, size {len(items)}, "
                f"block {self.block_id} acc {len(self.acc)} / {self.block_size}"
            )
            self.log_results(items)

    def run_scraper(self):
        while not self.reach_end_date():
            self.scrape_current_period()
            self.step_date()

        print(f"scraping finished")
        if len(self.acc) > 0:
            print(f"outputing final block ...")
            self.output_block()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", type=str, default="coq")
    parser.add_argument("--per_page", type=int, default=100)
    parser.add_argument("--req_per_min", type=int, default=10)
    parser.add_argument("--block_size", type=int, default=1000)
    parser.add_argument("--time_slot_length", type=int, default=3)
    parser.add_argument(
        "--output_dir", type=str, default="/localdata/yichen/pri_2008-2010/0001"
    )
    parser.add_argument("--start_date", type=str, default="2020-01-01T00:00:00")
    parser.add_argument("--end_date", type=str, default="2021-01-01T00:00:00")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    scraper = GithubQueryScraper(
        args.lang,
        args.per_page,
        args.time_slot_length,
        args.start_date,
        args.end_date,
        args.block_size,
        args.output_dir,
        args.req_per_min,
    )
    print(scraper)
    scraper.run_scraper()
