from os.path import isfile, abspath
from sys import argv

from riak import RiakClient

from baseball import get_game_xml_from_url, get_filename_list

DB_PORT = 8087
XML_BUCKET_NAME = 'xml'
GET_USAGE_STR = ('Usage:\n'
                 '  - ./initialize_db_xml.py url [DATE] [AWAY CODE] [HOME CODE] '
                 '[GAME NUMBER]\n'
                 '  - ./initialize_db_xml.py files [START DATE] [END DATE] '
                 '[INPUT DIRECTORY]\n')


client = RiakClient(pb_port=DB_PORT)
xml_bucket = client.bucket(XML_BUCKET_NAME)


def db_set_item_tuple_list(*item_tuple_list):
    for key, value in item_tuple_list:
        entry = xml_bucket.new(key, data=value)
        entry.store()

def initialize_from_url(date_str, away_code, home_code, game_num):
    (game_id,
     boxscore_raw_xml,
     player_raw_xml,
     inning_raw_xml) = get_game_xml_from_url(date_str, away_code, home_code,
                                             game_num)

    db_set_item_tuple_list((game_id + '_boxscore', boxscore_raw_xml),
                           (game_id + '_player', player_raw_xml),
                           (game_id + '_inning', inning_raw_xml))

def initialize_from_file_range(start_date_str, end_date_str, input_dir):
    input_path = abspath(input_dir)
    filename_tuple_list = get_filename_list(start_date_str, end_date_str,
                                            input_path)

    for game_id, boxscore_file, player_file, inning_file in filename_tuple_list:
        if isfile(boxscore_file) and isfile(player_file) and isfile(inning_file):
            boxscore_raw_xml = open(boxscore_file, 'r', encoding='utf-8').read()
            player_raw_xml = open(player_file, 'r', encoding='utf-8').read()
            inning_raw_xml = open(inning_file, 'r', encoding='utf-8').read()

            db_set_item_tuple_list((game_id + '_boxscore', boxscore_raw_xml),
                                   (game_id + '_player', player_raw_xml),
                                   (game_id + '_inning', inning_raw_xml))

if __name__ == '__main__':
    if len(argv) < 3:
        print(GET_USAGE_STR)
        exit()
    if argv[1] == 'files' and len(argv) == 5:
        initialize_from_file_range(argv[2], argv[3], argv[4])
    elif argv[1] == 'url' and len(argv) == 6:
        initialize_from_url(argv[2], argv[3], argv[4], argv[5])
    else:
        print(GET_USAGE_STR)
