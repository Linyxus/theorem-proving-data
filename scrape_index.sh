LANG=$1

python scrape_repo_list.py --lang $LANG --start_date 2010-01-01 --end_date 2022-01-01 --time_slot_length 8000 --output_dir ./"$LANG"_repo_index/
