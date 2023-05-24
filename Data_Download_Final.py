import requests
import pandas as pd
import urllib.parse
import time
import numpy as np
from time import sleep
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler

#Initializing all variables
API_KEY = 
EMAIL = 
BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-2-2-download.json?"
#Points will be very long. Is fine as long as under 142 points
POINTS = ['114645, 112172, 112639, 113635, 114134, 113128, 114647, 112164, 113124, 114141, 114145, 112644, 113125, 113121, 112640, 113629, 111713, 112641, 113126, 114143, 113638, 114142, 114652, 112649, 114138, 113631, 114642, 113634, 112167, 112646, 113632, 114137, 114140, 113627, 114644,  113131'
              #'115155, 117787, 116724, 115667, 118854, 118323, 115161, 120461, 119388, 122085, 117788, 122081, 122623, 119397, 118859, 124246, 117255, 119931, 119933, 120470, 124251, 123164, 118851, 116718, 123161, 118861, 123712, 121544, 123165, 115676, 115160, 119398, 118324, 121547, 119923, 121007'
]

m = 0 # starting variable for count of arrays
year_list= [1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020]

def requesting():
    global m, num_arrays, year_list
    n = 142
    vals = POINTS[0].split(',')
    if (year_list[m] == 1998):
        year_str = '1998'
    elif(year_list[m] == 1999):
        year_str = '1999'
    elif (year_list[m] == 2000):
        year_str = '2000'
    elif (year_list[m] == 2001):
        year_str = '2001'
    elif (year_list[m] == 2002):
        year_str = '2002'
    elif (year_list[m] == 2003):
        year_str = '2003'
    elif (year_list[m] == 2004):
        year_str = '2004'
    elif (year_list[m] == 2005):
        year_str = '2005'
    elif (year_list[m] == 2006):
        year_str = '2006'
    elif (year_list[m] == 2007):
        year_str = '2007'
    elif (year_list[m] == 2008):
        year_str = '2008'
    elif (year_list[m] == 2009):
        year_str = '2009'
    elif (year_list[m] == 2010):
        year_str = '2010'
    elif (year_list[m] == 2011):
        year_str = '2011'
    elif (year_list[m] == 2012):
        year_str = '2012'
    elif (year_list[m] == 2013):
        year_str = '2013'
    elif (year_list[m] == 2014):
        year_str = '2014'
    elif (year_list[m] == 2015):
        year_str = '2015'
    elif (year_list[m] == 2016):
        year_str = '2016'
    elif (year_list[m] == 2017):
        year_str = '2017'
    elif (year_list[m] == 2018):
        year_str = '2018'
    elif (year_list[m] == 2019):
        year_str = '2019'
    elif (year_list[m] == 2020):
        year_str = '2020'
    POINTS1 = [','.join(v) for v in zip(*(vals[i::n] for i in range(n)))]

    def main():
        input_data = {
            'attributes': 'clearsky_ghi,ghi',
            'interval': '30',
            'half_hour': 'true',
            'include_leap_day': 'true',

            'api_key': API_KEY,
            'email': EMAIL,
        }
        for name in [year_str]:
            print(f"Processing name: {name}")
            for id, location_ids in enumerate(POINTS):
                input_data['names'] = [name]
                input_data['location_ids'] = location_ids
                print(f'Making request for point group {id + 1} of {len(POINTS)}...')

                if '.csv' in BASE_URL:
                    url = BASE_URL + urllib.parse.urlencode(data, True)
                    # Note: CSV format is only supported for single point requests
                    # Suggest that you might append to a larger data frame
                    data = pd.read_csv(url)
                    print(f'Response data (you should replace this print statement with your processing): {data}')
                    # You can use the following code to write it to a file
                    # data.to_csv('SingleBigDataPoint.csv')
                else:
                    headers = {
                        'x-api-key': API_KEY
                    }
                    data = get_response_json_and_handle_errors(requests.post(BASE_URL, input_data, headers=headers))
                    download_url = data['outputs']['downloadUrl']
                    # You can do with what you will the download url
                    print(data['outputs']['message'])
                    print(f"Data can be downloaded from this url when ready: {download_url}")

                    # Delay for 1 second to prevent rate limiting
                    time.sleep(1)
                print(f'Processed')
    def get_response_json_and_handle_errors(response: requests.Response) -> dict:
        """Takes the given response and handles any errors, along with providing
        the resulting json

        Parameters
        ----------
        response : requests.Response
            The response object

        Returns
        -------
        dict
            The resulting json
        """
        if response.status_code != 200:
            print(
                f"An error has occurred with the server or the request. The request response code/status: {response.status_code} {response.reason}")
            print(f"The response body: {response.text}")
            exit(1)

        try:
            response_json = response.json()
        except:
            print(
                f"The response couldn't be parsed as JSON, likely an issue with the server, here is the text: {response.text}")
            exit(1)

        if len(response_json['errors']) > 0:
            errors = '\n'.join(response_json['errors'])
            print(f"The request errored out, here are the errors: {errors}")
            exit(1)
        return response_json

    if __name__ == "__main__":
        main()
    #print('This is m: '+ str(m))
    m += 1
    #print('This is m+1: ' + str(m))







scheduler = BlockingScheduler()
job_id = scheduler.add_job(requesting,'interval', seconds = 10)

scheduler.start()
