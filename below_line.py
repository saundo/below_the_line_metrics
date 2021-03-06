# below_line.py
# automate metric pulling from Keen for use in wrap reports

######### imports #########################################################
import os
import pickle
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
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

    actual_end = pd.date_range(start, end, freq='24H', tz=tz)
    actual_end = actual_end.tz_convert('UTC')
    actual_end = actual_end.shift(1)

    start_times = [datetime.strftime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in start_dates]
    end_times = [datetime.strftime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in end_dates]
    actual_end = [datetime.strftime(i, '%Y-%m-%dT%H:%M:%S.000Z') for i in actual_end]

    timeframe = [(start_times[i], end_times[i]) for i in range(len(start_times))]
    if timeframe[-1][1] > actual_end[-1]:
        timeframe[-1] = (start_times[-1], actual_end[-1])
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
### Grabbing cookies - EVENTS ###

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
        'ad_meta.campaign.name': filter on campaign.name
            string: 'inspiredmatters'
        'glass.device': filter on glass.device
            string: 'mobile'
        'video.progress.percent_viewed': filter on specific point watched in video
            int or list: 5 or [5,25,50,75,100]
        'ad_meta.client.name': filter on client.name
            string: 'amex'
    returns:
    + permanent cookies
    + keen.created_at
    + progress type
    + unit.id
    + video.title
    """
    op1 = op2 = op3 = op4 =  'eq'
    if 'ad_meta.campaign.name' in kwargs: campaign_name = kwargs['ad_meta.campaign.name']
    else:
        campaign_name = True
        op1 = 'exists'

    if 'glass.device' in kwargs: glass_device = kwargs['glass.device']
    else:
        glass_device = True
        op2 = 'exists'

    if 'video.progress.percent_viewed' in kwargs:
        percent_viewed = kwargs['video.progress.percent_viewed']
        if isinstance(percent_viewed, list):
            op3 = 'in'
        elif isinstance(percent_viewed, int):
            op3 = 'eq'

    else:
        percent_viewed = True
        op3 = 'exists'

    if 'ad_meta.client.name' in kwargs: client_name = kwargs['ad_meta.campaign.name']
    else:
        client_name = True
        op4 = 'exists'

    event = 'ad_video_progress'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('ad_meta.campaign.name', 'ad_meta.client.name',
                'ad_meta.unit.id',
                'glass.device', 'user.cookie.permanent.id',
                'video.progress.percent_viewed',
                'keen.created_at','video.title',)

    property_name1 = 'ad_meta.campaign.name'
    operator1 = op1
    property_value1 = campaign_name

    property_name2 = 'glass.device'
    operator2 = op2
    property_value2 = glass_device

    property_name3 = 'video.progress.percent_viewed'
    operator3 = op3
    property_value3 = percent_viewed

    property_name4 = 'ad_meta.client.name'
    operator4 = op4
    property_value4 = client_name

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

def read_article_metrics_lite(start, end, **kwargs):
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
                'keen.created_at',
                'user.cookie.permanent.id')

    property_name1 = 'read.type'
    operator1 = 'eq'
    property_value1 = 'start'

    property_name2 = 'user.cookie.permanent.id'
    operator2 = op2
    property_value2 = cookie_list

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    print('x', end='|')
    return data

def read_article_metrics_var(start, end, **kwargs):
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

    if 'tag' in kwargs:
        pass


    event = 'read_article'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = kwargs['group_by']

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

def read_article_metrics_time(start, end, **kwargs):
    """Keen read_article event, capturing time spent of cookies

    **kwargs
        Cookie_df: filters on list of permanent cookie ids from dataframe
            ex. Cookie_df = dataframe with column 'user.cookie.permanent.id'

    + filter on COOOKIES, and an obsession
    returns:
    + obsessions
    + device
    + keen timestamp
    + Cookie.ids
    + read.type
    + read.time.incremental.seconds
    """

    if 'Cookie_df' in kwargs:
        cookie_list = kwargs['Cookie_df']
        op1 = 'in'
    else:
        op2 = 'exists'
        interaction = True

    if 'article.obsessions' in kwargs:
        article_obsession = kwargs['article.obsessions']
        op2 = 'eq'
    else:
        op2 = 'exists'
        interaction = True



    event = 'read_article'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('article.obsessions',
                'glass.device',
                'keen.created_at',
                'read.type',
                'read.time.incremental.seconds',
                'user.cookie.permanent.id')

    property_name1 = 'user.cookie.permanent.id'
    operator1 = op1
    property_value1 = cookie_list

    property_name2 = 'article.obsessions'
    operator2 = op2
    property_value2 = article_obsession


    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2}]

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    print('x', end='|')
    return data

def read_article_sum_time(start, end, **kwargs):
    """
    """

    if 'Cookie_df' in kwargs:
        cookie_list = kwargs['Cookie_df']
        op1 = 'in'
    else:
        op1 = 'exists'
        cookie_list = True

    if 'article.obsessions' in kwargs:
        article_obsession = kwargs['article.obsessions']
        op2 = 'eq'
    else:
        op2 = 'exists'
        article_obsession = True

    event = 'read_article'

    target_property = 'read.time.incremental.seconds'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('article.obsessions',
                'glass.device')

    property_name1 = 'user.cookie.permanent.id'
    operator1 = op1
    property_value1 = cookie_list

    property_name2 = 'article.obsessions'
    operator2 = op2
    property_value2 = article_obsession

    property_name3 = 'read.time.incremental.seconds'
    operator3 = 'gt'
    property_value3 = 0

    property_name4 = 'read.time.incremental.seconds'
    operator4 = 'lt'
    property_value4 = 300

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1},
               {"property_name":property_name2, "operator":operator2, "property_value":property_value2},
               {"property_name":property_name3, "operator":operator3, "property_value":property_value3},
               {"property_name":property_name4, "operator":operator4, "property_value":property_value4}]

    data = keen.sum(event, target_property=target_property,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    print('x', end='|')
    return data

def read_article_pv(start, end, **kwargs):
    event = 'read_article'

    timeframe = {'start':start, 'end':end}
    interval = None
    timezone = None

    group_by = ('article.obsessions', 'glass.device')

    property_name1 = 'read.type'
    operator1 = 'eq'
    property_value1 = 'start'

    filters = [{"property_name":property_name1, "operator":operator1, "property_value":property_value1}]

    data = keen.count(event,
                    timeframe=timeframe,
                    interval=interval,
                    timezone=timezone,
                    group_by=group_by,
                    filters=filters)

    print('x', end='|')
    return data

######### Classes ###################################################

class cookie_jars:
    def __init__(self, raw_data):
        """
        raw_data from Keen API call
        initializes self.cookie_data, which contains only unique cookies
        based upon the first event that they triggered
        """
        d1 = raw_data
        df_list = [pd.DataFrame(d1[key]) for key in d1.keys()]
        df = pd.concat(df_list)
        df['keen.created_at'] = pd.to_datetime(df['keen.created_at'])

        #remove duplicates
        df = df.sort_values('keen.created_at')
        df_non_duplicated = df[~df['user.cookie.permanent.id'].duplicated()]

        self.cookie_data = df_non_duplicated
        print('data loaded', np.shape(self.cookie_data))
        print('unique cookies', self.cookie_data.nunique())
        print('columns', self.cookie_data.columns)

    def __repr__(self):
        class_name = 'cookie-jar'
        return class_name + '_' + str(np.shape(self.cookie_data))

    def create_jars(self, jar_capacity=250):
        """
        creates jars based upon jar_capacity
        """
        unique_cookies = len(self.cookie_data)
        self.cookie_data = self.cookie_data.sample(frac=1)
        print('cookies randomized!')

        jars = unique_cookies // jar_capacity
        overflow = unique_cookies % jar_capacity
        self.pull_sequence = [jar_capacity for i in range(jars)]

        if overflow > 0:
            jars += 1
            self.pull_sequence.append(overflow)
        print('jars', jars)
        print('pull_sequence', self.pull_sequence)

    def fill_jars(self):
        self.jar_container = {}
        for n, i in enumerate(self.pull_sequence):
            if n == 0:
                s1 = 0
                s2 = i
            else:
                s1 = s2
                s2 += i
            name = str(s1) + '-' + str(s2)

            self.jar_container[name] = self.cookie_data[s1:s2]
            print(name, ' filled', end=' | ')

class behavior_event():
    def __init__(self, jar_container):
        """
        jar_container - all the jars containing cookies
        """
        self.jar_container = jar_container

    def inspect_behavioral_event(self, behavior_function, jars_to_process=1, hour_interval=24):
        """
        behavior function - function intending to run; e.g. read_article_metrics
        """
        self.behavior_function = behavior_function
        jars = list(self.jar_container.values())

        #loop over jars, and run API calls
        container = []
        for i in range(jars_to_process):
            cookie_list = list(jars[i]['user.cookie.permanent.id'])

            #timeframe -- should just make this a function
            d1 = jars[i]['keen.created_at'].min() - timedelta(days=30)
            d2 = jars[i]['keen.created_at'].max() + timedelta(days=1)
            d1 = datetime.strftime(d1, '%Y-%m-%dT%H:%M:%S.000Z')
            d2 = datetime.strftime(d2, '%Y-%m-%dT%H:%M:%S.000Z')
            timeframe = timeframe_gen(d1, d2, hour_interval=hour_interval, tz=None)

            #API calls
            failure_log = []
            data = run_thread(behavior_function, timeframe, {'Cookie_df':cookie_list})

            dft = pd.concat([pd.DataFrame(data[key]) for key in data.keys()])
            dft['keen.created_at'] = pd.to_datetime(dft['keen.created_at'])

            #append data to container; storing as a tuple
            container.append(
                (jars[i][['user.cookie.permanent.id', 'keen.created_at']], dft)
            )
            print(len(jars[i]))

        df_cookies = []
        df_events = []
        for pairs in container:
            #pairs are tuples; 0-cookies, 1-events
            df_cookies.append(pairs[0])
            df_events.append(pairs[1])

        self.df_cookies = pd.concat(df_cookies)
        self.df_events = pd.concat(df_events)
        print(len(self.df_cookies))
        print(self.df_events['user.cookie.permanent.id'].nunique())

    def enforce_time_window(self, prior_days=30, forward_days=1):
        """
        calculate the time window of consideration for inluding / excluding
        behavorial events

        kwargs:
            prior_days - number of days before event to consider in window
            forward_days - number of day(s) after event to consider
        """

        #bring events and cookies together, for identifying the event_time
        dfz = pd.merge(self.df_events, self.df_cookies, on='user.cookie.permanent.id')

        #rename columns for easier idenitifcation
        dfz = dfz.rename(index=str, columns={"keen.created_at_x": "metric_time", "keen.created_at_y": "event_time"})

        #create timedelta for 30 days in the past
        col_name_p = 'event_time_-' + str(prior_days)
        dfz[col_name_p] = dfz['event_time'] - timedelta(days=prior_days)

        #create timedelta for 1 day ahead of the event
        col_name_f = 'event_time_-' + str(forward_days)
        dfz[col_name_f] = dfz['event_time'] + timedelta(days=forward_days)

        #boolean test to see if metric time falls within 30 day window
        dfz['metric_in_timeframe'] = (dfz['metric_time'] < dfz[col_name_f]) & (dfz['metric_time'] > dfz[col_name_p])

        self.df = dfz
        self.processed = dfz[dfz['metric_in_timeframe'] == True]
        #some error checking....hmmmmmmm
        print('whole shape', np.shape(dfz))
        print('processed shape', np.shape(dfz[dfz['metric_in_timeframe'] == True]))
        print()

        print('whole unique cookies', self.df_cookies['user.cookie.permanent.id'].nunique())
        print('1st stage unique cookies', dfz['user.cookie.permanent.id'].nunique())
        print('processed unique cookies', self.processed['user.cookie.permanent.id'].nunique())

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

######### PLOTTING ###################################################

class article_starts_bar_chart():
    def __init__(self, df, tag='obsessions'):
        """
        df - the input dataframe; columns contain
            + cookies
            + device
            + obsessions
            + topics
            + result
        tag - either 'obsessions' or 'topics'
        """
        self.df = df
        self.tag = tag

        if self.tag != 'obsessions' and self.tag != 'topics':
            raise ValueError('must be obsessions or topics')

        for column in ['cookies', 'device', 'result', 'obsessions', 'topics']:
            if column not in self.df.columns:
                raise ValueError('input dataframe columns not compatible')

    def prep_data(self, device='mobile'):
        """
        prepares data for plotting bar charts in seaborn
        """
        dfx = self.df[self.df['device'] == device]
        if self.ignore_untagged == 'yes_sneaky':
            dfx = dfx[dfx[self.tag] != '']
        elif self.ignore_untagged == 'yes_honest':
            pass
        elif self.ignore_untagged == 'no':
            pass
        else:
            raise ValueError('ignore_untagged not a recognized kwarg')

        dfx1 = dfx.groupby(self.tag).nunique()
        dfx1 = dfx1[['cookies']]
        dfx1['c share (%)'] = (dfx1['cookies'] / dfx['cookies'].nunique()) * 100
        dfx1['c share (%)'] = dfx1['c share (%)'].apply(lambda x: round(x, 1))
        dfx1 = dfx1.reset_index()

        dfx2 = dfx.groupby(self.tag).sum()
        dfx2 = dfx2.sort_values('result', ascending=False)
        dfx2['pv share (%)'] = (dfx2['result'] / dfx2['result'].sum()) * 100
        dfx2['pv share (%)'] = dfx2['pv share (%)'].apply(lambda x: round(x, 1))
        dfx2 = dfx2.reset_index()

        dfx3 = pd.merge(dfx1, dfx2, on=self.tag)
        return dfx3

    def plot_data(self, max_results=15, display='pv', ignore_untagged='no'):
        """
        seaborn bar plot
        max_results - to limit number of obssessions that are plotted
        display can be either:
            - 'pv'
            - 'cookies'
        ignore_untagged can be either:
            - 'no'; default, will show everything
            - 'yes_honest'; the truthful way of showing
            - 'yes_sneaky'; leaving untagged out from denominator

        """
        self.ignore_untagged = ignore_untagged
        self.mobile = self.prep_data(device='mobile')
        self.desktop = self.prep_data(device='desktop')

        if display == 'pv':
            value_col = 'pv share (%)'
        elif display == 'cookies':
            value_col = 'c share (%)'
        else:
            raise ValueError("not a recognized display kwarg, must be either 'pv' or 'cookies'")

        sns.set(style="white", context="talk")
        f, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 6), sharey=True)
        f.subplots_adjust(wspace=.1, hspace=0)
        plot_dict = {}

        if ignore_untagged == 'yes_honest':
            i1 = 1
        else:
            i1 = 0

        # ax1, mobile
        z = self.mobile.sort_values(value_col, ascending=False).iloc[i1:max_results]
        z = z.reset_index()

        device = 'mobile'
        total_cookies = self.df[self.df['device'] == device]['cookies'].nunique()
        plot_dict[device] = sns.barplot(z[self.tag], z[value_col], palette="BuGn_d", ax=ax1)
        label_name = device + ' n = ' + str(total_cookies) + " ignore '' value= " + ignore_untagged
        ax1.set_ylabel(value_col)
        ax1.set_title(label_name)

        # ax2, desktop
        z = self.desktop.sort_values(value_col, ascending=False).iloc[i1:max_results]
        z = z.reset_index()

        device = 'desktop'
        total_cookies = self.df[self.df['device'] == device]['cookies'].nunique()
        plot_dict[device] = sns.barplot(z[self.tag], z[value_col], palette="GnBu_d", ax=ax2)
        label_name = device + ' n = ' + str(total_cookies) + " ignore '' value= " + ignore_untagged
        ax2.set_title(label_name)
        ax2.set_ylabel('')

        #adjust the labels to be readable
        for device in plot_dict.keys():
            for item in plot_dict[device].get_xticklabels():
                item.set_rotation(75)

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