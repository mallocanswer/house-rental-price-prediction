import os
import csv
import requests
from bs4 import BeautifulSoup
import pickle
import pandas as pd

def get_cities_states(url):
    response = requests.get(url)
    page = BeautifulSoup(response.content, "html.parser")
    potentials = page.find_all(name="td", bgcolor="#dddddd")
    cities = []
    states = []
    for c in potentials:
        city = c.find(name="font", face="Arial", size="-2")
        if city is not None and str.isalpha(str(city.string[0])):
            city_name = str(city.string).split(";")[0]
            cities.append(city_name)
            if "," in city_name:
                states.append("")
            else:
                state_name = str(city.string).split(";")[-1]
                states.append(state_name)
    return cities, states

def get_city_income_map(cities, states):
    city2income = {}
    for i in range(len(cities)):
        city = cities[i]
        state = states[i]
        # if state == 'DC':
        #   state = 'Dictrict of Columnbia'
        # DC:
        income_df = pd.read_csv('../income/'+state+'.csv')
        for index, row in income_df.iterrows():
            if city in row['city']:
                city2income[city] = row[' median_household_income']
                break
    return city2income


def extract_features(cities, states, city2income):

    # city2income = get_city_income_map(cities, states)

    housing_conditions = [ \
          'city', \
          'address', \
          'price', \
          'bedrooms', \
          'bathrooms', \
          'instant_bookable', \
          'cancellation_policy', \
          'has_availability', \
          'room_type_category', \
          'min_nights', \
          'person_capacity', \
          'bed_type_category', \
          'property_type', \
          'reviews_count', \
          'room_type_category', \
          'review_rating_accuracy', \
          'review_rating_checkin', \
          'review_rating_cleanliness', \
          'review_rating_communication', \
          'review_rating_location', \
          'review_rating_value'\
          ]

    geographical_conditions = [ \
          'num_food', \
          'num_restaurants', \
          'num_tours', \
          'num_trainstations', \
          'num_transport', \
          'num_attractions' \
          ]

    conditions = housing_conditions + geographical_conditions

    city2index = {}
    for i in xrange(len(cities)):
        city = cities[i]
        state = states[i]
        city2index[city+","+state] = i

        print "Extract features", i, city+",", state
        features3 = []

        if state == "":
            file_name = city.replace(" ", "_")
        else:
            file_name = (city+","+state).replace(" ", "_").replace("/", "_")

        try:
            with open('../data/features/'+file_name+'_features2.pkl', 'rb') as handle:
                features2 = pickle.load(handle)
        except EnvironmentError:
            print "failed to open file for city: " + city
            print "filename: " + file_name

        for feature in features2:
            f = {}
            for condition in conditions:
                f[condition] = feature[condition]
            f['median_household_income'] = city2income[city.replace(",", "").strip()]
            f['city'] = i
            features3.append(f)
        pickle.dump(city2index, open('../data/features/city2index.pkl', 'wb'))
        pickle.dump(features3, open('../data/features/'+file_name+'_features3.pkl', 'wb'))

if __name__ == "__main__":
    url = "http://www.citymayors.com/gratis/uscities_100.html"
    cities, states = get_cities_states(url)

    city2income = {}
    with open('../income/city2income.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            city2income[row[0]] = float(row[1])

    extract_features(cities, states, city2income)


