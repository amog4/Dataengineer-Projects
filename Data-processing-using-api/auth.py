import configparser


config = configparser.ConfigParser()
config.read('config.cfg')


api_key = config['Key']['Apikeys']

headers = {'Authorization': 'Bearer %s' % api_key}

