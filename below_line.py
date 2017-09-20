# below_line.py
# automate metric pulling from Keen for use in wrap reports

######### imports #########################################################
import os
import pickle
import datetime
import pandas as pd
from keen.client import KeenClient
from functools import wraps
from queue import Queue
from threading import Thread

#load keen credentials and initialize the class
os.chdir('/users/csaunders/Desktop')
with open('Keen_API_credentials.pickle', 'rb') as f:
    Keen_API_credentials = pickle.load(f)

Keen_silo = 'QZ prod'
projectID = Keen_API_credentials[Keen_silo]['projectID']
readKey = Keen_API_credentials[Keen_silo]['readKey']
keen = KeenClient(project_id=projectID, read_key=readKey)

######### THREADING MODULE #################################################
def thread_module(*args, *kwargs):
    """insert the threading module here
    """
    pass

######### Keen API calls ###################################################
### Grabbing cookies ###

def ad_interaction(start, end, *kwargs):
    """Keen ad_interaction event collection
    *kwargs
    + filter properities on campaign
    + fitler on interaction type (most likely click), or return all interactions
    returns:
    + permanent cookies
    + keen timestamp
    +
    """
    pass

def ad_video_progress(start, end, *kwargs):
    """Keen ad_video_progress event collection
    *kwargs
    + filter properities on campaign
    + fitler on interaction type (most likely click)
    returns:
    + permanent cookies
    + keen timestamp
    + progress type
    """
    pass

def ad_time_spent(start, end, *kwargs):
    """Keen ad_time_spent event collection
    *kwargs
    + filter properities on campaign
    + fitler on interaction type (most likely click)
    returns:
    + permanent cookies
    + keen timestamp
    + time spent
    """
    pass

def read_article_cookie(start, end, *kwargs):
    """Keen read_article event collection; to PULL cookies
    *kwargs
    + filter properities on campaign / bulletin
    returns:
    + permanent cookies
    + keen timestamp
    + time spent (read.type), incremental seconds
    """
    pass

### Putting cookies to work ###

def ad_interaction(start, end, *kwargs):
    """Keen ad_interaction event collection: for metrics
    *kwargs
    + filter on COOOKIES
    + flter on Campaign
    returns:
    + number of impressions
    """
    pass

def read_article_metrics(start, end, *kwargs):
    """Keen read_article event collection: for metrics
    *kwargs
    + filter on COOOKIES
    returns:
    + obsessions
    + topics
    + article.id
    + device
    + geography
    + keen timestamp
    + TK
    """
    pass

######### Classes ###################################################

class cookie_picker():
    """class that stores cookies pulled from KEEN API calls; it then allocates
    the cookies into different cookie jars, and then distributes the cookie
    jars back to KEEN API calls in order to pull metrics
    """
    def __init__(self, name):
        self.name = name

    def load_data(self, data):
        """pull in the KEEN data"""
        self.raw_data = data

    def find_unique_cookies(self):
        """find the unique cookies in the data"""
        pass

    def containerize_cookies(self):
        """allocate cookies into different jars, based upon the maximum filter
        size that KEEN can handle"""
        pass

class metric_generator():
    """class that receives cookie jars, sends them to KEEN, and then recieves
    back data; compiles the multiple data from multiple cookie jars,
    munges data, returns & exports data in a usable format for producers
    """
    def __init__(self, name):
        self.name = name

######### Execute ###################################################

def execute_below_line(*args, *kwargs):
    """the function to pull into jupyter notebook"""
    campaign_name = input('campaign name')
    start_date = input('start_date')
    end_date = input('end_date')


    run_thread(timeframe, dump_directory, filter_paramters)
    x = dump_data(dump_directory)

    INT = cookie_picker('interact')
    INT.raw_data = x

    INT.find_unique_cookies()

    INT.containerize_cookies()


#1 Pull cookies based upon above parameters

#2 In cookie_picker, split cookies into their appropriate categories (ad_interaction, video, etc)

#3 Make cookie jars

#4 Deicde how many cookies to send to Keen for metrics

#5 push cookie jars to Keen

#6 store metric data in metric_generator

#7 munge the data

#8 output the data

#9 drink a beer and call it a day