""" Phase II: Retrieve data from Yelp and Foursquare """

import requests
from bs4 import BeautifulSoup
import io
import os
import pickle
import json
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator
from yelp.errors import ExceededReqs
import threading
import time
from auto_send import WechatAlert
from send_email import EmailAlert
import sys
from optparse import OptionParser
import multiprocessing

PROGRESS_FILENAME = 'phase_two_progress.pkl'
TRUNK_SIZE = 1

# Yelp param
cred1 = {"consumer_key": XXXXXXXXXXXXXXXXXXx,
         "consumer_secret": XXXXXXXXXXXXXXXXXXXX,
         "token": XXXXXXXXXXXXXXXXXXX,
         "token_secret": XXXXXXXXXXXXXXXXXXXX}

creds = [cred1]
START_CRED_ID = 6
global_max_cred = START_CRED_ID
change_cred_lock = threading.Lock()

see_you_tomorrow = False
# END Yelp Param

# Alert
send_lock = threading.Lock()
wechat = None
email = None
# END Wechat Alert Params

# Progress

# Timezone
os.environ['TZ'] = 'US/Eastern'
time.tzset()


# Timezon

def get_phase_two_progress():
    with open(PROGRESS_FILENAME, 'rb') as handle:
        progress = pickle.load(handle)
    return progress


def put_phase_two_progress():
    global progress, progress_lock
    with progress_lock:
        print 'checkpoint progress: ' + str(progress)
        pickle.dump(progress, open(PROGRESS_FILENAME, 'wb'))


def report_phase_two_progress():
    global progress
    report_prog = {}
    for c in progress:
        s, e = progress[c]
        if s < e:
            report_prog[c] = progress[c]
    print 'Progress: ' + str(report_prog)
    return


progress_lock = threading.Lock()
progress = get_phase_two_progress()

# END Progress

# Foursquare param
default_param = {
    'intent': 'browse',
    'limit': 50,
    'client_id': XXXXXXXXXXXXXXXXXXX,
    'client_secret': XXXXXXXXXXXXXXXXXXXX,
    'v': '20161029'
}


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


def search_attractions(location, radius, categories):
    oauth_token = ''
    url = 'https://api.foursquare.com/v2/venues/search'
    params = default_param.copy()
    params['ll'] = location
    params['radius'] = radius
    params['categoryId'] = categories
    return get(url, params)


def divide(cities):
    result = []
    n = len(cities)
    for i in range(n / TRUNK_SIZE):
        result.append(cities[i * TRUNK_SIZE: (i + 1) * TRUNK_SIZE])
    return result


def alert(msg):
    global wechat, send_lock, email
    if email is None:
        print msg
    else:
        with send_lock:
            email.send(msg)


class Download(threading.Thread):
    def __init__(self, idx, city):
        threading.Thread.__init__(self)
        self.idx = idx
        self.city = city
        self.cur_cred_id = START_CRED_ID
        auth = Oauth1Authenticator(**creds[self.cur_cred_id])
        self.client = Client(auth)

    def run(self):
        self.download_geo_features()

    def change_cred(self):
        global creds, see_you_tomorrow, global_max_cred
        with change_cred_lock:
            if self.cur_cred_id + 1 < len(creds):
                self.cur_cred_id += 1
                if self.cur_cred_id > global_max_cred:
                    global_max_cred = self.cur_cred_id
                    print 'Changing yelp credential to #' + str(global_max_cred + 1) + '.'
                auth = Oauth1Authenticator(**creds[self.cur_cred_id])
                self.client = Client(auth)
            else:
                if not see_you_tomorrow:
                    print 'Running out of yelp credentials. See you tomorrow.'
                    see_you_tomorrow = True
                    time.sleep(1000)
                sys.exit(1)

    def get_number_of_businesses(self, lat, lng, business='grocery', radius=1000):
        """ get_number_of_businesses returns the number of businesses 
            of interest with a radius inside a specified bounding box
            Input:
                - client: yelp client object
                - sw_lat: latitude of southwest position
                - sw_lng: longitude of southwest position
                - ne_lat: latitude of northeast position
                - ne_lng: longitude of northeast position
                - business: business of interest
                - radius: within radius of the center of bounding box
            Output:
                - number of businesses in the bounding box satisfying condition
        """
        params = {
            'category_filter': business,
            'radius_filter': radius
        }
        t = 10
        excpetion = None
        while t < 1000:
            try:
                response = self.client.search_by_coordinates(lat, lng, **params)
                # time.sleep(0.1)
                return response.total
            except ExceededReqs as exceed:
                excpetion = str(exceed)
                print 'Got exception in Yelp API: ' + str(exceed) + ' Sleeping for 10 seconds.'
                time.sleep(10)
                self.change_cred()
            except Exception as e:
                excpetion = str(e)
                print 'Got exception in Yelp API: ' + str(e) + ' Sleeping for ' + str(t) + ' seconds.'
                time.sleep(t)
                # reinitializing client?
                auth = Oauth1Authenticator(**creds[self.cur_cred_id])
                self.client = Client(auth)
                t *= 1.5
        alert('Failed in Yelp API, last Exception: ' + str(excpetion))
        sys.exit(1)

    def download_geo_features(self):
        global progress
        idx, city = self.idx, self.city
        current, target = None, None
        if city not in progress:
            current = 0
        else:
            current, target = progress[city]
            if current >= target:
                print 'City ' + city + ' already finished.'
                return
        print 'Current Progress for ' + city + str((current, target))

        list_of_businesses = ['restaurants', 'food', 'tours', 'trainstations', 'transport']

        DEFAULT_RADIUS = 400
        DEFAULT_CAT = ['4d4b7104d754a06370d81259', '4d4b7105d754a06377d81259']

        dir_name = '../data/'

        with open(dir_name + 'features/' + city.replace(" ", "_") + '_features.pkl', 'rb') as handle:
            features = pickle.load(handle)
            if target is None:
                target = len(features)
        dir_name = '../tmp/'
        print 'Loading features finished for ' + city

        features2 = []
        if os.path.isfile(dir_name + 'features/' + city.replace(" ", "_") + '_features2.pkl'):
            with open(dir_name + 'features/' + city.replace(" ", "_") + '_features2.pkl', 'rb') as handle:
                features2 = pickle.load(handle)
                current = len(features2)
                progress[city] = (current, target)
                put_phase_two_progress()

        print 'Loading features2 finished for ' + city
        finished = current >= target
        if current >= target:
            print 'City ' + city + ' already finished.'
            return

        alert('Downloding geo features from ' + str(len(features2)) + ' to ' + str(len(features)) + ' for ' + city)
        for cnt in xrange(current, target):
            feature = features[cnt]

            feature = feature['listing']

            lat = feature['lat']
            lng = feature['lng']

            # From Yelp
            for business in list_of_businesses:
                feature['num_' + business] = self.get_number_of_businesses(lat, lng, business=business, radius=2000)

            # From Foursquare
            location = '%0.2lf,%0.2lf' % (lat, lng)
            t = 10
            while t < 1000:
                try:
                    res = search_attractions(location, DEFAULT_RADIUS, DEFAULT_CAT)
                    if res['meta']['code'] != 200:
                        alert('Exception in search_attractions: ' + str(res['meta']))
                        sys.exit(1)
                    else:
                        if res['response']:
                            feature['num_attractions'] = len(res['response']['venues'])
                        else:
                            feature['num_attractions'] = 0
                    time.sleep(0.05)
                    break
                except Exception as e:
                    print 'Got exception in search_attractions: ' + str(e) + ' Sleeping for ' + str(t) + ' seconds.'
                    time.sleep(t)
                    t *= 1.5
            if t >= 1000:
                print 'Failed in search_attractions.'
                sys.exit(1)

            # Add current feature # remove listing
            features2.append(feature)

            cnt += 1
            if cnt % 10 == 0:
                print time.ctime() + ":" + str(cnt) + "/" + str(len(features)) + " completed for " + str(
                    idx) + ' ' + city + '\n'
            if cnt % 50 == 0:
                pickle.dump(features2, open(dir_name + 'features/' + city.replace(" ", "_") + '_features2.pkl', 'wb'))
                progress[city] = (len(features2), len(features))
                put_phase_two_progress()

        pickle.dump(features2, open(dir_name + 'features/' + city.replace(" ", "_") + '_features2.pkl', 'wb'))
        progress[city] = (len(features2), len(features))
        put_phase_two_progress()
        alert('Download Finished for ' + str(idx) + ' ' + city + '.')
        report_phase_two_progress()


def parse_args():
    parser = OptionParser(usage='%prog [options]',
                          description='download geofeatures from airbnb and yelp')
    parser.add_option('-w', '--wechatenable', action='store_true', default=False, help='enable wechat alert')
    parser.add_option('-t', '--trunksize', type=int, default=1, help='Trunk size')
    (options, args) = parser.parse_args()
    return options


def main(option):
    global wechat, see_you_tomorrow, TRUNK_SIZE, email
    if option.wechatenable:
        # wechat = WechatAlert()
        # wechat.init_wechat()
        email = EmailAlert()
    TRUNK_SIZE = option.trunksize
    # Get 100-city list
    url = "http://www.citymayors.com/gratis/uscities_100.html"
    cities = get_cities(url)
    # finished = ['Mesa, Arizona']
    # cities = ['Omaha, Nebraska', 'Cincinnati, Ohio', 'Winston-Salem, North Carolina', 'Reno, Nevada']
    trunks = divide(cities)
    for tr_id, cities in enumerate(trunks):
        if see_you_tomorrow:
            return
        print('Processing Trunk: ' + str(tr_id))
        print('Cities: ' + str(cities))

        threads = []
        # processes = []
        for i, city in enumerate(cities):
            try:
                # Download features from Yelp and Foursquare
                print 'Processing ' + str(i + TRUNK_SIZE * tr_id) + ' ' + city
                thread = Download(i + TRUNK_SIZE * tr_id, city)
                threads.append(thread)
                thread.daemon = True
                thread.start()

            except (KeyboardInterrupt, SystemExit):
                print '\n! Received keyboard interrupt/sys exit, quitting threads.\n'
                sys.exit(1)
            except Exception as e:
                print "Error: unable to start thread for ", city
                print 'Exception: ', e
        for t in threads:
            try:
                t.join()
            except (KeyboardInterrupt, SystemExit):
                print '\n! Received keyboard interrupt/sys exit, quitting threads.\n'
                sys.exit(1)
            except Exception as e:
                print "Error: unable to start thread for ", city
                print 'Exception: ', e

    alert('Done.')


if __name__ == "__main__":
    main(parse_args())
