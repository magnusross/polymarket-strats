import re
import pandas as pd
from py_clob_client.client import ClobClient

from constants import PREM_TEAMS


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


def get_epl_matches(client):
    resp = client.get_markets(next_cursor="")

    output_rows = []
    while resp["limit"] == resp["count"]:
        resp = client.get_markets(next_cursor=resp["next_cursor"])

        for market in resp["data"]:
            question = market["question"]

            is_epl_match, winner, loser, draw = extract_match_details(question)

            if is_epl_match:
                row = {
                    "winner": winner,
                    "loser": loser,
                    "is_draw": draw,
                    "conditon_id": market["condition_id"],
                    "question_id": market["question_id"],
                    "description": market["description"],
                    "end_date_iso": market["end_date_iso"],
                    "game_start_time": market["game_start_time"],
                    "closed": market["closed"],
                    "first_token_id": market["tokens"][0]["token_id"],
                    "first_token_outcome": market["tokens"][0]["outcome"],
                    "second_token_id": market["tokens"][1]["token_id"],
                    "second_token_outcome": market["tokens"][1]["outcome"],
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
