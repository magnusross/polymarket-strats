from joblib import Memory

PREM_TEAMS = {
    "Arsenal": ["Arsenal"],
    "Aston Villa": ["Aston Villa"],
    "Bournemouth": ["Bournemouth"],
    "Brentford": ["Brentford"],
    "Brighton & Hove Albion": ["Brighton", "Brighton & Hove Albion"],
    "Burnley": ["Burnley"],
    "Chelsea": ["Chelsea"],
    "Crystal Palace": ["Crystal Palace"],
    "Everton": ["Everton"],
    "Fulham": ["Fulham"],
    "Leeds United": ["Leeds", "Leeds United"],
    "Leicester City": ["Leicester", "Liecester", "Leicester City"],
    "Liverpool": ["Liverpool"],
    "Luton Town": ["Luton Town"],
    "Manchester City": ["Man City", "Manchester City"],
    "Manchester United": ["Man United", "Manchester United", "Manchester Utd"],
    "Newcastle United": ["Newcastle", "Newcastle United"],
    "Norwich City": ["Norwich", "Norwich City"],
    "Nottingham Forest": ["Nottingham Forest", "Forest"],
    "Sheffield United": ["Sheffield United", "Sheffield"],
    "Southampton": ["Southampton"],
    "Tottenham Hotspur": ["Tottenham", "Spurs", "Tottenham Hotspur"],
    "Watford": ["Watford"],
    "Ipswich Town": ["Ipswich Town", "Ipswich"],
    "West Bromwich Albion": ["West Brom", "West Bromwich Albion"],
    "West Ham United": ["West Ham", "West Ham United"],
    "Wolverhampton Wanderers": ["Wolves", "Wolverhampton Wanderers", "Wolverhampton"],
}

CLOB_URL = "https://clob.polymarket.com/prices-history"

MAX_CALLS = 6
CALL_PERIOD = 10
MEMORY = Memory(location="./cache")
