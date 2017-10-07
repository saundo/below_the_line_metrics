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
from datetime import datetime, timedelta

#load keen credentials and initialize the class
os.chdir('/users/csaunders/Desktop')
with open('Keen_API_credentials.pickle', 'rb') as f:
    Keen_API_credentials = pickle.load(f)

Keen_silo = 'QZ prod'
projectID = Keen_API_credentials[Keen_silo]['projectID']
readKey = Keen_API_credentials[Keen_silo]['readKey']
keen = KeenClient(project_id=projectID, read_key=readKey)

######### TIMEFRAME GENERATOR ##############################################
def timeframe_gen(start, end, hour_interval=24, tz='US/Eastern'):
    """creates timeframe for use in making Keen API calls
    args:
        start - start date (str - '2017-08-04'); inclusive
        end - end date (str - '2017-08-04'); inclusive
    kwargs:
        hour_interval - interval for breaking up start, end tuple
        tz - timezone

    returns:
        List of tuples, tuple - (start, end)
    """

    freq = str(hour_interval) + 'H'
    start_dates = pd.date_range(start, end, freq=freq, tz=tz)
    start_dates = start_dates.tz_convert('UTC')
    end_dates = start_dates.shift(1)

    start_times = [datetime.strftime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in start_dates]
    end_times = [datetime.strftime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in end_dates]
    timeframe = [(start_times[i], end_times[i]) for i in range(len(start_times))]
    return timeframe

######### THREADING MODULE #################################################
class DownloadWorker1(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            func, start, end, kwargs = self.queue.get()
            run_func(func, start, end, kwargs)
            self.queue.task_done()

def run_func(func, start, end, kwargs):
    """
    """
    key = func.__name__ + '-' + str(start)
    thread_storage[key] = func(start, end, **kwargs)

def run_thread(func, timeframe, kwargs):
    """
    """
    global thread_storage
    thread_storage = {}
    queue = Queue()
    for x in range(8):
        worker = DownloadWorker1(queue)
        worker.daemon = True
        worker.start()

    for start,end in timeframe:
        queue.put((func, start, end, kwargs))

    queue.join()
    return thread_storage

######### Keen API calls ###################################################
### Grabbing cookies ###

def ad_interaction(start, end, **kwargs):
    """Keen ad_interaction event collection
    **kwargs; keys must be:
            'interaction.name' --> single value or list of values

            'ad_meta.client.name'
                ex. {'ad_meta.client.name':'amex'}
            'ad_meta.campaign.name'

            'campaign.name'
            'client.name'

    returns:
    + permanent cookies
    + keen.created_at
    """

    if 'interaction.name' in kwargs:
        interaction = kwargs['interaction.name']
        if isinstance(interaction, str):
            op2 = 'contains'
        elif isinstance(interaction, list):
            op2 = 'in'
    else:
        op2 = 'exists'
        interaction = True

    if 'ad_meta.client.name' in kwargs:
        client = str.lower(kwargs['ad_meta.client.name'])
        op3 = 'contains'
    else:
        op3 = 'exists'
        client = True

    if 'ad_meta.campaign.name' in kwargs:
        campaign = str.lower(kwargs['ad_meta.campaign.name'])
        op4 = 'contains'
    else:
        op4 = 'exists'
        campaign = True

    event = 'ad_interaction'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = list(kwargs.keys()) + list(('user.cookie.permanent.id','keen.created_at', 'interaction.target'))

    property_name1 = 'ad_meta.unit.type'    #deprecated - should switch to creative_placement.type
    operator1 = 'eq'
    property_value1 = 'display'

    property_name2 = 'interaction.name'
    operator2 = op2
    property_value2 = interaction

    property_name3 = 'ad_meta.client.name' #deprecated - should switch to client.name
    operator3 = op3
    property_value3 = client

    property_name4 = 'ad_meta.campaign.name' #deprecated - should switch to campaign.name
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

def ad_impression(start, end, **kwargs):
    """Keen ad_impression event collection
    **kwargs
        Client: filter on client.name
            ex. Client='gates-foundation'
        Campaign: filter on campaign.name
            ex. Campaign='gates-foundation_q3_2017'
        Creative: filter on creative.name
            ex. Video='incredible-progress'
        Version: filter on campaign.version.name
            ex. Version='N/A'
    returns:
    + filter properties on client
    + filter properties on campaign
    + filter properties on creative
    + filter properties on version
    + permanent cookies
    + keen timestamp
    +
    """

    if 'Client' in kwargs:
        client = str.lower(kwargs['Client'])
        op2 = 'contains'
    else:
        op2 = 'exists'
        client = True

    if 'Campaign' in kwargs:
        campaign = str.lower(kwargs['Campaign'])
        op3 = 'contains'
    else:
        op3 = 'exists'
        campaign = True

    if 'Creative' in kwargs:
        creative = str.lower(kwargs['Creative'])
        op4 = 'contains'
    else:
        op4 = 'exists'
        creative = True

    if 'Version' in kwargs:
        version = str.lower(kwargs['Version'])
        op5 = 'contains'
    else:
        op5 = 'exists'
        version = True


    event = 'ad_impression'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('user.cookie.permanent.id','keen.created_at')

    property_name1 = 'ad_meta.unit.type'
    operator1 = 'eq'
    property_value1 = 'display'

    property_name2 = 'ad_meta.client.name'
    operator2 = op2
    property_value2 = client

    property_name3 = 'ad_meta.campaign.name'
    operator3 = op3
    property_value3 = campaign

    property_name4 = 'ad_meta.creative.name'
    operator4 = op4
    property_value4 = creative

    property_name5 = 'ad_meta.campaign.version.name'
    operator5 = op5
    property_value5 = version


    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2},
               {"property_name":property_name3, "operator":operator3, "property_value":property_value3},
               {"property_name":property_name4, "operator":operator4, "property_value":property_value4},
               {"property_name":property_name5, "operator":operator5, "property_value":property_value5}]

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    return data

def ad_video_progress(start, end, **kwargs):
    """Keen ad_video_progress event collection
    **kwargs
        Client: filter on client.name
            ex. Client='amex'
        Campaign: filter on campaign.name
            ex. Campaign='platinum'
        Video_Progress: filter on specific point watched in video
            ex. Video_Progress=5 or Video_Progress=[5,25,50,75,100]
    returns:
    + permanent cookies
    + keen.created_at
    + progress type
    """
    if 'Video_Progress' in kwargs:
        video_mark = kwargs['Video_Progress']
        op2 = 'in'
    else:
        op2 = 'exists'
        video_mark = True

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


    event = 'ad_video_progress'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('user.cookie.permanent.id','keen.created_at','video.title', 'video.progress.percent_viewed')

    property_name1 = 'ad_meta.unit.type'
    operator1 = 'eq'
    property_value1 = 'display'

    property_name2 = 'video.progress.percent_viewed'
    operator2 = op2
    property_value2 = video_mark

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

def ad_time_spent(start, end, *kwargs):
    """Keen ad_time_spent event collection
    *kwargs
    + filter properities on campaign
    + fitler on interaction type (most likely click)
    returns:
    + permanent cookies
    + keen.created_at
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

# def ad_interaction(start, end, *kwargs):
#     """Keen ad_interaction event collection: for metrics
#     *kwargs
#     + filter on COOOKIES
#     + flter on Campaign
#     returns:
#     + number of impressions
#     """
#     pass

def read_article_metrics(start, end, **kwargs):
    """Keen read_article event collection: for metrics
    **kwargs
        Cookie_df: filters on list of permanent cookie ids from dataframe
            ex. Cookie_df = dataframe with column 'user.cookie.permanent.id'

    + filter on COOOKIES
    returns:
    + obsessions
    + topics
    + article.id
    + device
    + geography
    + keen timestamp
    + Cookie.ids
    """
    if 'Cookie_df' in kwargs:
        cookie_list = kwargs['Cookie_df']
        op2 = 'in'
    else:
        op2 = 'exists'
        interaction = True

    event = 'read_article'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('article.obsessions',
                'article.topic',
                'glass.device',
                'user.geolocation.country',
                'keen.created_at',
                'article.permalink',
                'read.type',
                'read.time.incremental.seconds',
                'user.cookie.session.id',
                'user.cookie.permanent.id')

    # property_name1 = 'read.type'
    # operator1 = 'eq'
    # property_value1 = 'start'

    property_name1 = 'user.cookie.permanent.id'
    operator1 = op2
    property_value1 = cookie_list

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1}]
              # {"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    print('x', end='|')
    return data

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

    - Receives list of DataFrames as tuples
        - First df contains list of permanent cookies and time of event action being measured
        - Second df contains metrics to be analyzed within this class (obsessions/topics/articles read)
    - Concatenates each into two DataFrames, then combines into one df
    - Sorts time values by whether the actions occurred within 30 days
    - Removes actions outside of previous 30 days
    - Methods can organize by different criteria
    - Export however we want (excel, charts, etc.)

    """
    def __init__(self, raw):
        self.raw = raw
        self.dataframe = pd.concat([pd.DataFrame(i[1]) for i in raw])
        self.dataframe = self.dataframe.dropna()
        self.dataframe['keen.created_at'] = pd.to_datetime(self.dataframe['keen.created_at'])
        self.cookie_jars = pd.concat([i[0] for i in raw])
        self.cookie_jars = self.cookie_jars.dropna()

    def merge(self):
        storage = {}
        for cookie in list(set(self.cookie_jars['user.cookie.permanent.id'])):
            dft = self.cookie_jars[self.cookie_jars['user.cookie.permanent.id']==cookie]
            try:
                mins = min(dft['keen.created_at'])
            except:
                pass
            storage.setdefault('user.cookie.permanent.id', []).append(cookie)
            storage.setdefault('Time_of_action', []).append(mins)
        df = pd.DataFrame(storage)
        df['Time_of_action'] = pd.to_datetime(df['Time_of_action'])
        df['Delta'] = df['Time_of_action'] - timedelta(days=30)
        self.all = pd.merge(df,self.dataframe,how='right',on='user.cookie.permanent.id')
        self.all['in30days'] = ((self.all['keen.created_at'] > self.all['Delta'][0])&(self.all['keen.created_at'] < self.all['Time_of_action']))
        self.false = self.all[self.all['in30days']==False]
        self.true = self.all[self.all['in30days']==True]
        return(self.true)

    def obsessions(self):
        """
        Function within metrics class. Returns all obsessions read by permanent ids by devices in last 30 days
        """
        self.obsession = self.all.groupby(['glass.device'])['article.obsessions'].value_counts()
        self.obsession = self.obsession.unstack("glass.device")
        self.obsession_plot = self.obsession.unstack("glass.device").plot(kind="barh")
        return(self.obsession)

    def topics(self):
        """
        Function within metrics class. Returns all topics read by permanent ids by devices in last 30 days
        """
        self.topic = self.all.groupby(['glass.device'])['article.topic'].value_counts()
        self.topic = self.topic.unstack("glass.device")
        self.topic_plot = self.topic.unstack("glass.device").plot(kind="barh")
        return(self.topic)

    def countries(self):
        """
        Function within metrics class. Returns all countries where permanent ids are located
        """
        self.country = self.all.groupby(['glass.device'])['user.geolocation.country'].value_counts()
        self.country = self.country.unstack("glass.device")
        self.country_plot = self.country.unstack("glass.device").plot(kind="barh")
        return(self.country)

    def articles(self):
        """
        Function within metrics class. Returns all articles read by permanent ids in last 30 days
        """
        self.article = self.all.groupby(['glass.device'])['article.id'].value_counts()
        self.article = self.article.unstack("glass.device")
        self.article_plot = self.article.unstack("glass.device").plot(kind="bar")
        return(self.article)

######### Execute ###################################################

def execute_below_line(*args, **kwargs):
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