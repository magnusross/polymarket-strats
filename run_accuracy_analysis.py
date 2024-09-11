from datetime import date
import numpy as np
import pandas as pd


def add_unique_match_id(df):
    return df.assign(
        match_id=markets_info_df[["winner", "loser", "game_start_time"]]
        .agg(frozenset, axis=1)
        .apply(hash)
    )


def get_price_at_time(info_df, history_df, token_col, pre_game_timedelta):
    joined_gametime_prices = (
        history_df.reset_index(names=["token_id", "timestamp"])
        .merge(
            info_df[
                [
                    "game_start_time",
                    token_col,
                ]
            ],
            left_on="token_id",
            right_on=token_col,
        )
        .assign(
            timestamp=lambda x: pd.to_datetime(x.timestamp),
        )
        .assign(pre_game_time=lambda x: x.game_start_time - pre_game_timedelta)
    )

    price_at_time = (
        joined_gametime_prices.loc[
            (joined_gametime_prices.timestamp < joined_gametime_prices.pre_game_time)
        ]
        .sort_values("timestamp")
        .groupby("token_id")
        .last()
    )

    return price_at_time.price


def collate_match_tokens(df):
    traded_draws_df = (
        df[df.is_draw]
        .rename(columns={c: c.replace("first", "draw_yes") for c in df.columns})
        .rename(columns={c: c.replace("second", "draw_no") for c in df.columns})
    ).drop(columns=["is_draw"])

    traded_wl_df = (
        df[~df.is_draw]
        .rename(columns={c: c.replace("first", "first_win_yes") for c in df.columns})
        .rename(columns={c: c.replace("second", "first_win_no") for c in df.columns})
    ).drop(columns=["is_draw"])

    traded_wl_df_switched = (
        df[~df.is_draw]
        .rename(columns={"winner": "loser", "loser": "winner"})
        .rename(columns={c: c.replace("first", "second_win_yes") for c in df.columns})
        .rename(columns={c: c.replace("second", "second_win_no") for c in df.columns})
    ).drop(columns=["is_draw"])

    merged_df = (
        traded_wl_df.merge(
            traded_wl_df_switched,
            on=["winner", "loser", "game_start_time", "match_id"],
            suffixes=("_first_win", "_second_win"),
        )
        .merge(
            traded_draws_df,
            on=["winner", "loser", "game_start_time", "match_id"],
            suffixes=("", "_draw"),
        )
        .rename(columns={"winner": "first_team", "loser": "second_team"})
        .groupby("match_id")
        # .set_index("match_id")
        .first()
        .reset_index()
    )

    return merged_df


if __name__ == "__main__":
    markets_info_df = pd.read_parquet("epl_markets.parquet")
    all_histories = pd.read_parquet("token_histories.parquet")

    print(markets_info_df.dtypes)
    print(markets_info_df.head())

    # drop duplicate rows and markets with no volume
    traded_df = (
        markets_info_df.drop_duplicates()
        .loc[markets_info_df.volume > 0.01]
        # add unqiue id for matches
        .pipe(add_unique_match_id)
        # drop useless columns for matches
        .drop(
            columns=[
                "question",
                "condition_id",
                "question_id",
                "closed",
                "id",
                "description",
                "end_date_iso",
                "uma_end_data",
            ]
        )
        .drop(columns=[c for c in markets_info_df.columns if "outcome" in c])
    )

    collated_df = collate_match_tokens(traded_df)

    token_cols_list = [
        "first_win_yes_token_id",
        "first_win_no_token_id",
        "second_win_yes_token_id",
        "second_win_no_token_id",
        "draw_yes_token_id",
        "draw_no_token_id",
    ]

    # what time relative to the start of the game do we want to look at the probabilities?
    pre_game_timedelta = pd.Timedelta(minutes=5)

    df = collated_df.copy()

    for s in token_cols_list:
        pre_price_col = get_price_at_time(
            collated_df, all_histories, s, pre_game_timedelta
        ).rename(s[:-3] + "_pre_price")
        df = df.join(pre_price_col, on=s)

    print(df[[c for c in df.columns if not c.endswith("_id")]])


    # Sanity check for known game 
    # https://polymarket.com/event/epl-manchester-city-vs-ipswich-town?tid=1726066349636
    test_df = df.loc[
        (df.first_team == "Manchester City")
        & (df.second_team == "Ipswich Town")
        & (df.game_start_time.dt.date == date(2024, 8, 24))
    ]
    assert test_df["first_win_yes_token_pre_price"].values[0] == 0.905, test_df

    true_outcomes = df[
        [
            "first_win_yes_token_price",
            "second_win_yes_token_price",
            "draw_yes_token_price",
        ]
    ]
    predicted_yes_outcomes = df[
        [
            "first_win_yes_token_pre_price",
            "second_win_yes_token_pre_price",
            "draw_yes_token_pre_price",
        ]
    ]

    print(
        "Accuracy: ",
        (predicted_yes_outcomes.round(0).values == true_outcomes.round(0).values)
        .all(axis=1)
        .sum()
        / len(true_outcomes),
    )
