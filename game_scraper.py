"""
05/10/2024
Author: Aychid
Description: Scrapes the play by play data for all games of Lebron James
in the 2018 season and stores it in CSV files
Only use once to avoid request limits
"""

# Imports
import logging
from tqdm import tqdm
from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import OutputType, Team
import requests
from bs4 import BeautifulSoup

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s: %(message)s",
    style="%",
    level=logging.INFO,
    filemode='w',
    filename="scraper.log",
)


def get_table_info(rows: list) -> list:
    """ Extracts the date, opponent, and home/away status from given rows """

    game_data = []
    for row in rows:
        if 'thead' in row.get('class', []):  # Skip header rows
            continue

        # Extract the date, opponent, and home/away status
        date = row.find('td', {'data-stat': 'date_game'}).text
        opponent = row.find('td', {'data-stat': 'opp_id'}).text
        # Game_location column has "@" symbol for away games so check in text for "@"
        home_game = 'Home' if not (row.find('td', {'data-stat':
                                                   'game_location'}).text == "@") else 'Away'

        # Append to the list
        game_data.append([date, home_game, opponent])

    return game_data


def get_game_info(url: str) -> BeautifulSoup:
    """
    Gets the full page from the URL and returns
    ordered row list with the date, opponent, and home/away status
    """
    soup = BeautifulSoup(requests.get(url).content,
                         "html.parser")  # Gets full page and parses it with HTML parser

    if soup is None:
        logger.error("Failed to load page, check URL")
        raise ValueError("Failed to load page, check URL")

    else:
        table = soup.find('table', {'id': 'pgl_basic'})   # Find table by ID
        if table is None:
            logger.error("Table not found, check the page structure or URL")
            raise ValueError("Table not found, check the page structure or URL")
        rows = table.find('tbody').find_all('tr')  # Find all rows in the table

        rows = get_table_info(rows)

        # Split the string into day, month, year and convert to integers and keep in the same list
        for row in rows:
            row[0] = [int(i) for i in row[0].split("-")]
    return rows


# Output all advanced player season totals for the 2017-2018 season in CSV format to 2018_10_06_BOS_PBP.csv
def scrape_games(game_data: list) -> list:
    """
    Scrapes the play by play data for each game in the game_data list
    with the format [[year, month, day], home/away, opponent]
    and stores it in a CSV file in the pbp_games folder
    """

    # Dictionary of team names from bball reference to the web scraper API team names
    dict_teams = {"ATL": Team.ATLANTA_HAWKS, "BOS": Team.BOSTON_CELTICS, "BRK": Team.BROOKLYN_NETS,
                  "CHI": Team.CHICAGO_BULLS, "CHO": Team.CHARLOTTE_HORNETS,
                  "CLE": Team.CLEVELAND_CAVALIERS, "DAL": Team.DALLAS_MAVERICKS, "DEN": Team.DENVER_NUGGETS,
                  "DET": Team.DETROIT_PISTONS, "GSW": Team.GOLDEN_STATE_WARRIORS, "HOU": Team.HOUSTON_ROCKETS,
                  "IND": Team.INDIANA_PACERS, "LAC": Team.LOS_ANGELES_CLIPPERS, "LAL": Team.LOS_ANGELES_LAKERS,
                  "MEM": Team.MEMPHIS_GRIZZLIES, "MIA": Team.MIAMI_HEAT, "MIL": Team.MILWAUKEE_BUCKS,
                  "MIN": Team.MINNESOTA_TIMBERWOLVES, "NOP": Team.NEW_ORLEANS_PELICANS,
                  "NYK": Team.NEW_YORK_KNICKS, "OKC": Team.OKLAHOMA_CITY_THUNDER,
                  "ORL": Team.ORLANDO_MAGIC, "PHI": Team.PHILADELPHIA_76ERS,
                  "PHO": Team.PHOENIX_SUNS, "POR": Team.PORTLAND_TRAIL_BLAZERS,
                  "SAC": Team.SACRAMENTO_KINGS, "SAS": Team.SAN_ANTONIO_SPURS,
                  "TOR": Team.TORONTO_RAPTORS, "UTA": Team.UTAH_JAZZ, "WAS": Team.WASHINGTON_WIZARDS}

    for game in tqdm(game_data, desc="Scraping games"):
        year, month, day = game[0]

        print(f"Writing play-by-play for Cavs game on {year}-{month}-{day} to CSV file")
        try:  # Stores all PBP as CSV's in folder pbp_games
            if game[1] == "Home":
                client.play_by_play(home_team=Team.CLEVELAND_CAVALIERS, year=year,
                                    month=month, day=day, output_type=OutputType.CSV,
                                    output_file_path=f"pbp_games/{year}_{month}_{day}_CLE_PBP_HOME.csv")
            elif game[1] == "Away":
                client.play_by_play(home_team=dict_teams[game[2]], year=year,
                                    month=month, day=day, output_type=OutputType.CSV,
                                    output_file_path=f"pbp_games/{year}_{month}_{day}_CLE_PBP_AWAY.csv")
            else:
                raise ValueError("Invalid home/away status")
        except Exception as e:
            logger.exception(e)


def main():
    """
    Gets all the games of Lebron in the 2018 Season
    and stores the play by play data in CSV files
    """

    url = "https://www.basketball-reference.com/players/j/jamesle01/gamelog/2018/"

    game_rows = get_game_info(url)
    scrape_games(game_rows)


if __name__ == "__main__":
    main()
