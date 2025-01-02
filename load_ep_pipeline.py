from ep_data_loader import ep_data_loader

import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prod", required=True, help="Required for database writes")
    parser.add_argument("-s", "--start", default = 2024, help="Start Year for season scraping")
    parser.add_argument("-e", "--end", default = 2024, help="End Year for season scraping")
    parser.add_argument("-i", "--load_player_info", default = True, help="Load player bio info (This takes can take a day and up to a week depending on how many seasons are loaded)")

    args = parser.parse_args()

    ep = ep_data_loader.Scraper(start_year=args.start, end_year=args.end, prod_db=args.prod)

    ep.full_data_load(collect_player_info=args.load_player_info, output='postgres')
