import requests
from bs4 import BeautifulSoup
import pickle
import pandas as pd
import numpy as np


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


def transform(cities):
    fields = ['reviews_count',
              'bathrooms',
              'room_type_category',
              'instant_bookable',
              'cancellation_policy',
              'bedrooms',
              'bed_type_category',
              'person_capacity',
              'property_type',
              'min_nights',
              'num_restaurants',
              'num_food',
              'num_attractions',
              'num_transport',
              'num_trainstations',
              'median_household_income',
              'city',
              'review_rating_accuracy',
              'review_rating_checkin',
              'review_rating_cleanliness',
              'review_rating_communication',
              'review_rating_location',
              'review_rating_value'
              ]

    cnt = 1
    falseCount = 0
    features4 = []
    prices4 = []
    for city in cities:
        print "Processing", cnt, city
        cnt += 1
        with open('../data/features/' + city.replace(" ", "_") + '_features3.pkl', 'rb') as handle:
            features3 = pickle.load(handle)
        with open('../data/prices/' + city.replace(" ", "_") + '_prices.pkl', 'rb') as handle:
            prices3 = pickle.load(handle)

        # Append features
        new_features = []
        new_prices = []
        for i, feature in enumerate(features3):
            cur_feature = []
            valid = True
            for field in fields:
                if feature[field] is None:
                    falseCount += 1
                    valid = False
                    break
                if str(feature[field]).lower() == 'nan':
                    falseCount += 1
                    valid = False
                    break
                cur_feature.append(feature[field])
            if not valid: continue

            new_features.append(cur_feature)
            new_prices.append(prices3[i])

        features4 += new_features

        # Append prices
        prices4 += new_prices

    print "Number of NaN:", falseCount

    df = pd.DataFrame(features4, columns=fields)
    df.loc[:, 'instant_bookable'] = df.loc[:, 'instant_bookable'].apply(lambda x: int(x))

    df = pd.get_dummies(df)  # , prefix=['', '', '', '', '']);
    features4 = df.as_matrix()

    print list(df.columns.values)
    print "Dumping"
    pickle.dump(list(df.columns.values), open('../data/features/feature_list.pkl', 'wb'))
    pickle.dump(features4, open('../data/features/features4.pkl', 'wb'))
    pickle.dump(prices4, open('../data/prices/prices4.pkl', 'wb'))


if __name__ == '__main__':
    url = "http://www.citymayors.com/gratis/uscities_100.html"
    cities = get_cities(url)

    transform(cities)

