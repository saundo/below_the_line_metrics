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

def ad_interaction(start, end, **kwargs):
    """Keen ad_interaction event collection
    **kwargs
        Client: filter on client.name
            ex. Client='amex'
        Campaign: filter on campaign.name
            ex. Campaign='platinum'
        Interaction: filter on interaction name
            ex. Interaction='clicked'
    + filter properties on client
    + filter properties on campaign
    + filter on interaction name (most likely click), or return all interactions by excluding
    returns:
    + permanent cookies
    + keen timestamp
    +
    """
    if 'Interaction' in kwargs:
        interaction = kwargs['Interaction']
        op2 = 'contains'
    else:
        op2 = 'exists'
        interaction = True
    
    if 'Client' in kwargs:
        client = str.lower(kwargs['Client'])
        op3 = 'contains'
    else:
        op3 = 'exists'
        client = True
    
    if 'Campaign' in kwargs:
        campaign = str.lower(kwargs['Campaign'])
        op4 = 'contains'
    else:
        op4 = 'exists'
        campaign = True
    
    
    event = 'ad_interaction'
    
    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('user.cookie.permanent.id','keen.timestamp')
    
    property_name1 = 'ad_meta.unit.type'
    operator1 = 'eq'
    property_value1 = 'display'
    
    property_name2 = 'interaction.name'
    operator2 = op2
    property_value2 = interaction
    
    property_name3 = 'ad_meta.client.name'
    operator3 = op3
    property_value3 = client
    
    property_name4 = 'ad_meta.campaign.name'
    operator4 = op4
    property_value4 = campaign
    
    
    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
              {"property_name":property_name2, "operator":operator2, "property_value":property_value2},
              {"property_name":property_name3, "operator":operator3, "property_value":property_value3},
              {"property_name":property_name4, "operator":operator4, "property_value":property_value4}]    

    data = keen.count(event, 
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)
    
    return data
    

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