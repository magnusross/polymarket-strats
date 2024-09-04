from datetime import datetime, timedelta

from ratelimit import limits
from tqdm import tqdm
from time import sleep
import pandas as pd
import requests

from constants import CALL_PERIOD, CLOB_URL, MAX_CALLS


def parse_raw_history_to_df(raw_history):
    return (
        pd.DataFrame(raw_history)
        .rename(columns={"p": "price"})
        .assign(timestamp=lambda x: pd.to_datetime(x["t"], unit="s"))
        .drop(columns=["t"])
        .set_index("timestamp")
    )


# @limits(calls=MAX_CALLS, period=CALL_PERIOD)
def get_historical_data(asset_id, startTs, fidelity):
    url = f"{CLOB_URL}?startTs={startTs}&market={asset_id}&earliestTimestamp={startTs}&fidelity={fidelity}"

    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to get data for {asset_id}")
        return pd.DataFrame([])

    raw_history = resp.json()["history"]
    df = parse_raw_history_to_df(raw_history)
    return df


def get_data_for_token(token_id, end_datetime):
    # date_format = "%Y-%m-%dT%H:%M:%SZ"


    # end_datetime = datetime.strptime(end_time_str, date_format)
    start_datetime = end_datetime - timedelta(weeks=4)

    start_unix_timestamp = start_datetime.timestamp()

    historical_data = get_historical_data(
        token_id,
        start_unix_timestamp,
        1,
    )
    # historical_data = drop_consecutive_duplicates(historical_data, "price")
    historical_data = historical_data.loc[:end_datetime]
    return historical_data


if __name__ == "__main__":
    markets_info_df = pd.read_parquet("epl_markets.parquet")

    all_histories = {}
    for i, row in tqdm(markets_info_df.iterrows()):
        if (row["first_token_id"] is None) or (row["game_start_time"] is None):
            print(f"No first token id, skipping for {row}")
            continue
        history = get_data_for_token(row["first_token_id"], row["game_start_time"])
        all_histories[row["first_token_id"]] = history
        sleep(1)

    all_histories_df = pd.concat(all_histories)
    all_histories_df.to_parquet("first_token_histories.parquet")
