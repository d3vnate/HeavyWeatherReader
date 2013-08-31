#!/usr/bin/python
"""
@author Nate Vogel
@website http://funtothinkabout.com
@last_revision 2013-08-31

PLEASE READ README.md!!

"""
import ConfigParser 
import pymysql 
import signal
from sys import exit
from time import sleep
from datetime import datetime

# TODO: replace debug with proper logging.
# debug mode does not write to the DB, strictly prints data
global debug

class HeavyWeatherData:
    """
    Applies mapping logic between the HW Pro Software output file (as consumed by ConfigParser)
    Initialization reads and parses the data file into dict 'HeavyWeatherData.parsed_data'.
    """
    def __init__(self, cfg_parser, temp_unit):
        self.__cfg_parser = cfg_parser
        self.__c_or_f = temp_unit
        self.parsed_data = self._read_wd()

    def _get_hwd(self, metric, unit_type):
        hwd_value = self.__cfg_parser.get(metric, unit_type).strip('"')
        # occasionally, the data source file contains dashes rather than a numeric value
        # a -1 value indicates such a condition and is ignored by hwr_wunderground
        if '-' in hwd_value:
            hwd_value = -1
            if debug:
                print "metric %s had no data" % metric
        return hwd_value
    
    def _read_wd(self):
        wd = {}
        wd['temperature'] = {
            'outdoor': float(self._get_hwd('outdoor_temperature', 'deg_%s' % self.__c_or_f)), 
            'indoor': float(self._get_hwd('indoor_temperature', 'deg_%s' % self.__c_or_f)),
            'dewpoint': float(self._get_hwd('dewpoint', 'deg_%s' % self.__c_or_f))
        }
        wd['humidity'] = {
            'outdoor': int(self._get_hwd('outdoor_humidity', 'percent')),
            'indoor': int(self._get_hwd('indoor_humidity', 'percent'))
        }
        wd['wind'] = {
            'chill': float(self._get_hwd('windchill', 'deg_%s' % self.__c_or_f)),
            'speed': float(self._get_hwd('wind_speed', 'mph')),
            'direction_deg': float(self._get_hwd('wind_direction', 'deg')),
            'direction': self._get_hwd('wind_direction', 'name')
        }
        wd['rain'] = {
            'total': float(self._get_hwd('rain_total', 'inch')),
            '24h': float(self._get_hwd('rain_24h', 'inch')),
            '1h': float(self._get_hwd('rain_1h', 'inch'))
        }
        wd['pressure'] = {
            'relative_inhg': float(self._get_hwd('pressure_relative', 'inHg'))
        }
        wd['meta'] = {
            'actualisation': int(self._get_hwd('time','last_actualisation')),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'datehour': datetime.now().strftime("%Y-%m-%d %H")
        }
        return wd

    def is_new(self, actualisation):
        if self.parsed_data and self.parsed_data['meta']['actualisation'] > actualisation:
            return True
        else:
            return False
    

def store_wd(db_cur, wd):
    sql_data_str = "INSERT INTO raw_stats SET "
    for weather_metric,sub_metrics in wd.items():
        for sub_metric,data_value in sub_metrics.items():
            if data_value != -1:
                sql_data_str += "%s_%s = '%s'," % (weather_metric, sub_metric, data_value)
    if debug:
        print sql_data_str[:-1]
    # execute insertion, remove trailing ,
    else:
        try:
            db_cur.execute(sql_data_str[:-1])
        except pymysql.err.IntegrityError, e:
            print e
            print "skipping data storage..."

"""
main!!
"""
if __name__=="__main__":
    
    def signal_handler(signalnum, frame):
        if debug:
            print signalnum
        if signalnum == signal.SIGINT:
            print "Exiting..."
            weather_data_cur.close()
            weather_data_db.close()
            exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    cfg_parser = ConfigParser.SafeConfigParser()
    hwd_parser = ConfigParser.SafeConfigParser()

    # get and set configuration options
    cfg_parser.read('hwreader.ini')
    current_weather_data_file = cfg_parser.get('heavyweatherreader','current_weather_data_file')
    poll_interval = int(cfg_parser.get('heavyweatherreader','poll_interval'))
    print_updates_status_interval = int(cfg_parser.get('heavyweatherreader','print_updates_status_interval'))
    store_c_or_f = cfg_parser.get('heavyweatherreader', 'store_temperature_C_or_F')
    debug = bool(int(cfg_parser.get('heavyweatherreader','debug')))

    # counters
    last_actualisation = 0
    num_updates_stored = 0

    # initialize database for weather data storage
    weather_data_db = pymysql.connect(host=cfg_parser.get('weather_data_db','host'), 
                                      user=cfg_parser.get('weather_data_db','user'), 
                                      passwd=cfg_parser.get('weather_data_db','pass'), 
                                      db=cfg_parser.get('weather_data_db','name'))
    weather_data_cur = weather_data_db.cursor()

    # App setup complete
    # Now run forever
    while True:
        hwd_parser.read(current_weather_data_file)
        wd_handler = HeavyWeatherData(hwd_parser, store_c_or_f) 

        # only store if there is new data
        if wd_handler.is_new(last_actualisation):
            last_actualisation = wd_handler.parsed_data['meta']['actualisation']
            store_wd(weather_data_cur, wd_handler.parsed_data)
            if debug:
                print wd_handler.parsed_data
            else:
                weather_data_db.commit()
            num_updates_stored += 1
        else:
            if debug:
                print "Same weather data as last check...moving on"

        # status updates are returned to stdout on the configured interval, unless set to 0
        if print_updates_status_interval:
            # probably more efficient to call datetime once and split it 
            # than multiple calls and ensures consistent time subvars
            ymdh,curmin,cursec = datetime.now().strftime("%Y-%m-%d %H,%M,%S").split(",")
            if (int(cursec) <= poll_interval) and (int(curmin) % 30 == 0):
                print "%s:%s >> %s updates" % (ymdh,curmin,num_updates_stored)
                num_updates_stored = 0

        sleep(poll_interval)
