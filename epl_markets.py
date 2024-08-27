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


# List of EPL teams and common abbreviations


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


# Test cases
# texts = [
#     "Will Manchester City win vs Chelsea?",
#     "Will Brighton beat Manchester United?",
#     "Can Liverpool lose to Arsenal?",
#     "Is Tottenham going to win against Everton?",
#     "Will Aston Villa defeat Bournemouth?",
#     "Will Man City beat Bournemouth? (02/25/2023)",
#     "Will Tottenham beat Chelsea? (02/26/2023)",
#     "Will Man United draw with Liverpool?",
#     "Is it possible that Arsenal and Chelsea will tie?",
#     "Do you think the match between Spurs and Man City will end in a draw?"
# ]

# for text in texts:
#     match, winner, loser, is_draw = extract_match_details(text)
#     if match:
#         if is_draw:
#             print(f"Match Found: {winner} vs {loser}, Result - Draw")
#         else:
#             print(f"Match Found: Winner - {winner}, Loser - {loser}")
#     else:
#         print("No match description found.")


def check_is_match_simple(text):
    if any([(team in text) for team in PREM_TEAMS]) and (
        ("vs" in text) or ("beat" in text)
    ):
        return True


# @backoff.on_exception(backoff.expo,
#                       ratelimit.exception.RateLimitException, max_tries=20, max_time=120)
# @limits(calls=MAX_CALLS, period=CALL_PERIOD)
# @MEMORY.cache
# def get_markets_paginated(cursor):
#     time.sleep(1)
#     return client.get_markets(next_cursor=cursor)

@MEMORY.cache
def get_markets_paginated(offset, limit):
    time.sleep(1)
    url = f"{GAMMA_URL}/markets?limit=100&closed=true&offset={limit*offset}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to get data for offset {offset}")
        return []
    return resp.json()




def get_epl_matches(client):

    limit = 100
    offset = 0
    resp_json = get_markets_paginated(offset, limit)

    output_rows = []
    while len(resp_json) == limit:
        offset += 1
        resp_json = get_markets_paginated(offset, limit)
        print(resp_json[0]["question"])
        for market in resp_json:
            question = market["question"]

            is_epl_match, winner, loser, draw = extract_match_details(question)

            if is_epl_match:
                print("Match extracted", question)
                print(market["outcomes"])
                outcomes = ast.literal_eval(market["outcomes"])
                token_ids = ast.literal_eval(market["clobTokenIds"])
                row = {
                    "winner": winner,
                    "loser": loser,
                    "is_draw": draw,
                    "conditon_id": market["conditionId"],
                    "question_id": market["questionID"],
                    "id": market["id"],
                    "description": market["description"],
                    "end_date_iso": market.get("endDateIso", None),
                    "uma_end_data": market.get("umaEndDate", None),
                    # "game_start_time": market["game_start_time"],
                    "closed": market["closed"],
                    "first_token_id": token_ids[0],
                    "first_token_outcome": outcomes[0],
                    "second_token_id": token_ids[1],
                    "second_token_outcome": outcomes[1],
                }
                output_rows.append(row)

            if check_is_match_simple(question) and not is_epl_match:
                print("Skipping question that could be a match - ", question)

            # if is_epl_match:
            #     print(f"Match Found: Winner - {winner}, Loser - {loser}")

    return pd.DataFrame(output_rows)


if __name__ == "__main__":
    client = ClobClient("https://clob.polymarket.com/")

    df = get_epl_matches(client=client)
    df.to_parquet("epl_markets.parquet")
