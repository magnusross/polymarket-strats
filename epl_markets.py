import re
import time
import ast
import backoff
import pandas as pd
import py_clob_client
import ratelimit
import requests
from py_clob_client.client import ClobClient
from py_clob_client.exceptions import PolyApiException, PolyException
from ratelimit import limits

from constants import CALL_PERIOD, GAMMA_URL, MAX_CALLS, MEMORY, PREM_TEAMS


def extract_vs_match_details(text):
    if "vs." not in text:
        return False, None, None

    # print(text)

    first_team, second_team = text.strip().split("vs.")
    # print(first_team, second_team)

    first_team_match, second_team_match = False, False

    # for team in (first_team, second_team):
    for abbrvs in PREM_TEAMS.values():
        if first_team.strip() in abbrvs:
            first_team_match = True

        if second_team.strip() in abbrvs:
            second_team_match = True

    if first_team_match and second_team_match:
        return True, first_team, second_team

    else:
        return False, None, None


# Function to determine if the string describes a match and extract teams or a draw
def extract_match_details(text):
    # Define patterns that might indicate a match outcome
    match_patterns = [
        r"(\bwin\b.*\bvs\b)",  # e.g., "win vs"
        r"(\bbeat\b)",  # e.g., "beat"
        r"(\bdefeat\b)",  # e.g., "defeat"
        r"(\bwin\b.*\bagainst\b)",  # e.g., "win against"
        r"(\blose\b.*\bto\b)",  # e.g., "lose to"
        r"(\bdraw\b)",  # e.g., "draw"
        r"(\bend in a draw\b)",  # e.g., "end in a draw"
        r"(\btie\b)",  # e.g., "tie"
        r"(\bend in a tie\b)",  # e.g., "end in a tie"
    ]

    # Compile the regex patterns
    match_regex = re.compile("|".join(match_patterns), re.IGNORECASE)

    # Check if any match pattern exists in the text
    if not match_regex.search(text):
        return False, None, None, False

    # Create a dictionary to store which teams are found in the text
    found_teams = {}
    for full_team_name, abbreviations in PREM_TEAMS.items():
        for abbreviation in abbreviations:
            if abbreviation.lower() in text.lower():
                found_teams[full_team_name] = abbreviation

    # Ensure there are exactly two teams mentioned
    if len(found_teams) != 2:
        return False, None, None, False

    # Extract the team names
    team_names = list(found_teams.keys())

    # Determine if the text indicates a draw
    if "draw" in text.lower() or "tie" in text.lower():
        return True, team_names[0], team_names[1], True

    # Determine the order of teams based on the verb position (who is winning/beating)
    winner, loser = None, None

    # Checking for keywords to determine the winner and loser
    if "win" in text.lower() or "beat" in text.lower() or "defeat" in text.lower():
        winner = (
            team_names[0]
            if text.lower().index(found_teams[team_names[0]].lower())
            < text.lower().index(found_teams[team_names[1]].lower())
            else team_names[1]
        )
        loser = team_names[1] if winner == team_names[0] else team_names[0]
    elif "lose" in text.lower():
        loser = (
            team_names[0]
            if text.lower().index(found_teams[team_names[0]].lower())
            < text.lower().index(found_teams[team_names[1]].lower())
            else team_names[1]
        )
        winner = team_names[1] if loser == team_names[0] else team_names[0]

    return True, winner, loser, False


def check_is_match_simple(text):
    if any([(team in text) for team in PREM_TEAMS]) and (
        ("vs" in text) or ("beat" in text)
    ):
        return True


@MEMORY.cache
def get_clob_markets_paginated(cursor):
    time.sleep(1)
    return client.get_markets(next_cursor=cursor)


@MEMORY.cache
def get_gamma_markets_paginated(offset, limit):
    time.sleep(1)
    url = f"{GAMMA_URL}/markets?limit=100&closed=true&offset={limit*offset}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to get data for offset {offset}")
        return []
    return resp.json()


def parse_gamma_response(market):
    question = market["question"]

    is_vs_match, first_team, second_team = extract_vs_match_details(question)

    is_epl_match, winner, loser, draw = extract_match_details(question)

    if check_is_match_simple(question) and not is_epl_match:
        print("Gamma - Skipping question that could be a match - ", question)
        print("Gamma - vs match", is_vs_match)

    if is_epl_match:
        outcomes = ast.literal_eval(market["outcomes"])
        outcome_prices = ast.literal_eval(market["outcomePrices"])
        token_ids = ast.literal_eval(market["clobTokenIds"])
        row = {
            "winner": winner,
            "loser": loser,
            "is_draw": draw,
            "question": question,
            "condition_id": market["conditionId"],
            "question_id": market.get("questionID", None),
            "id": market["id"],
            "description": market["description"],
            "end_date_iso": market.get("endDateIso", None),
            "uma_end_data": market.get("umaEndDate", None),
            # "game_start_time": market["game_start_time"],
            "volume": float(market["volume"]),
            "closed": market["closed"],
            "first_token_id": token_ids[0],
            "first_token_outcome": outcomes[0],
            "first_token_price": float(outcome_prices[0]),
            "second_token_id": token_ids[1],
            "second_token_outcome": outcomes[1],
            "second_token_price": float(outcome_prices[1]),
        }
        return row


def get_epl_matches_gamma():
    limit = 100
    offset = 0
    resp_json = get_gamma_markets_paginated(offset, limit)

    output_rows = []
    while len(resp_json) == limit:
        offset += 1
        resp_json = get_gamma_markets_paginated(offset, limit)
        for market in resp_json:
            row = parse_gamma_response(market)
            if row:
                output_rows.append(row)

    return pd.DataFrame(output_rows)


def get_epl_matches_clob():
    # we need to hit this too to get the game start time
    resp = get_clob_markets_paginated(cursor="")

    output_rows = []
    while resp["limit"] == resp["count"]:
        resp = get_clob_markets_paginated(cursor=resp["next_cursor"])

        for market in resp["data"]:
            question = market["question"]
            is_vs_match, first_team, second_team = extract_vs_match_details(question)

            is_epl_match, winner, loser, draw = extract_match_details(question)

            if is_epl_match:
                row = {
                    "winner": winner,
                    "loser": loser,
                    "is_draw": draw,
                    "condition_id": market["condition_id"],
                    "question_id": market["question_id"],
                    "game_start_time": pd.to_datetime(market["game_start_time"])
                    .tz_convert("UTC")
                    .tz_localize(None),
                }
                output_rows.append(row)

            if check_is_match_simple(question) and not is_epl_match:
                print("CLOB - Skipping question that could be a match - ", question)
                print("CLOB - vs match", is_vs_match)
    return pd.DataFrame(output_rows)


if __name__ == "__main__":
    client = ClobClient("https://clob.polymarket.com/")

    clob_df = get_epl_matches_clob()
    gamma_df = get_epl_matches_gamma()
    df = pd.merge(
        gamma_df,
        clob_df,
        on=["winner", "loser", "is_draw", "condition_id", "question_id"],
    ).convert_dtypes()
    df.to_parquet("epl_markets.parquet")
