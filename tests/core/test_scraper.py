import game_scraper
import os


class TestScraper:
    url = "https://www.basketball-reference.com/players/j/jamesle01/gamelog/2018/"

    def test_game_meta_data(self):
        """
        GIVEN the URL of game data LBJ in 2018
        WHEN the get_game_info function is called
        THEN the function should return a list of lists containing the date,
            home/away status, and opponent
        """
        assert game_scraper.get_game_info(self.url)[0] == [[2017, 10, 17], 'Home', 'BOS'], \
            "Test failed, game data not equal"

    def test_scrape_to_csv(self):
        """
        GIVEN list of rows containing date, home/away status, and opponent
        WHEN the get_table_info function is called
        THEN the function should return 82 csv files containing the play
            by play data for each game in the pbp_games folder
        """
        game_scraper.scrape_games(game_scraper.get_game_info(self.url))
        csv_file_count = len([name for name in os.listdir('pbp_games')
                             if os.path.isfile(os.path.join('pbp_games', name))
                             and name.endswith('.csv')])

        assert os.path.exists("pbp_games/2017_10_17_CLE_PBP_HOME.csv"), "Test failed to create first CSV file"
        assert csv_file_count == 82, "Test failed, incorrect number of CSV files in folder"
