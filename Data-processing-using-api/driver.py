import configparser
from business_data_request import BusinessSearch
from queries import create_business_schema, create_business_table, insert_business_table
from databasedriver import DatabaseDriver
import argparse

config = configparser.ConfigParser()
config.read('config.cfg')

parser = argparse.ArgumentParser(description="A Example yelp business finder based on parameters such as term, location, price, ")


api_key = config['Key']['Apikeys']
headers = {'Authorization': 'Bearer %s' % api_key}

def to_string(data):
    return [str(value) for value in data.values()]


def main():
    args = parser.parse_args()
    # Pricing levels to filter the search result with: 1 = $, 2 = $$, 3 = $$$, 4 = $$$$.
    b = BusinessSearch(term=args.term, location=args.location, price=args.prices)
    db = DatabaseDriver()
    db.setup()

    queries = [insert_business_table.format(*to_string(result)) for result in b.get_results()]
    query_to_execute = "BEGIN; \n" + '\n'.join(queries) + "\nCOMMIT;"
    db.execute_query(query_to_execute)


if __name__ == "__main__":
    
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')
    required.add_argument("-t", "--term",  metavar='', required=True,
                          help="Search term, for example \"food\" or \"restaurants\". The term may also be business names, such as \"Starbucks.\".")
    required.add_argument("-l", "--location",  metavar='', required=True,
                          help="This string indicates the geographic area to be used when searching for businesses. ")
    optional.add_argument("-p", "--prices", type=int, metavar='', required=False, default=1,
                          help="Pricing levels to filter the search result with: 1 = $, 2 = $$, 3 = $$$, 4 = $$$$.")

    main()