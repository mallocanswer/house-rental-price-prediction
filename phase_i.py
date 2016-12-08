""" Phase I: Retrieve data from Airbnb """

import requests
from bs4 import BeautifulSoup
import pickle
import time
import requests
import os
import sys


def get(url, params):
    r = requests.get(url, params=params)
    return r.json()


def get_cities(url):
    response = requests.get(url)
    page = BeautifulSoup(response.content, "html.parser")
    potentials = page.find_all(name="td", bgcolor="#dddddd")
    cities = []
    for c in potentials:
        city = c.find(name="font", face="Arial", size="-2")
        if city is not None and str.isalpha(str(city.string[0])):
            city_name = str(city.string).replace(';', ',').replace('/', ' ')
            cities.append(city_name)
    return cities


def get_access_token():
    url = 'https://api.airbnb.com/v1/authorize'
    username = 'pds2016.15688@gmail.com'
    password = '15618.2016.pass'
    r = requests.post(url, data=
    {'password': password,
     'locale': 'en-US',
     'username': username,
     'grant_type': 'password',
     'currency': 'USD',
     'client_id': '3092nxybyb0otqw18e8nh5nty'})
    return r.json()['access_token']


default_params = {
    'client_id': '3092nxybyb0otqw18e8nh5nty',
    'locale': 'en-US',
    'currency': 'USD',
}


def search(location):
    url = 'https://api.airbnb.com/v2/search_results'
    params = default_params.copy()
    params['location'] = location
    limit = 20
    params['_limit'] = limit
    results = {}

    result = get(url, params)
    #     print result['search_results']
    results = result['search_results']
    result_count = result['metadata']['pagination']['result_count']
    next_count = result['metadata']['pagination']['next_offset']

    while result_count > 0:
        #         print result_count, next_count
        params['_offset'] = next_count
        result = get(url, params)
        if 'search_results' not in result:
            break
        results += result['search_results']
        result_count = result['metadata']['pagination']['result_count']
        next_count = result['metadata']['pagination']['next_offset']
        time.sleep(0.5)
    return results


def get_listing_info(listing_id):
    url = 'https://api.airbnb.com/v2/listings/' + str(listing_id)
    params = default_params.copy()
    params['_format'] = 'v1_legacy_for_p3'
    return get(url, params)


def get_features_and_prices(listings):
    prices = []
    features = []
    for i in range(len(listings)):
        res = listings[i]
        pricing_quote = res['pricing_quote']
        prices.append(pricing_quote['nightly_price'] / float(pricing_quote['guests']))
        listing = res['listing']
        info = get_listing_info(listing['id'])
        features.append(info)
        time.sleep(0.1)
        if i % 100 == 1:
            print i, "completed"
    return features, prices


if __name__ == "__main__":
    url = "http://www.citymayors.com/gratis/uscities_100.html"
    cities = get_cities(url)

    dir_name = '../data/'

    for city in cities:
        if os.path.isfile(dir_name + 'features/' + city.replace(" ", "_") + '_features.pkl'):
            continue
        print "Downloading data for", city
        i = 0
        t = 10
        while True:
            try:
                listings = search(city)
                features, prices = get_features_and_prices(listings)
                pickle.dump(features, open(dir_name + 'features/' + city.replace(" ", "_") + '_features.pkl', 'wb'))
                pickle.dump(prices, open(dir_name + 'prices/' + city.replace(" ", "_") + '_prices.pkl', 'wb'))
                break
            except Exception as e:
                print str(e)
                time.sleep(t)
                i += 1
                if i == 10:
                    print 'Failed after retrying', i, 'times'
                    t *= 1.5
                    sys.exit(1)

