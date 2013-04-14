#!/usr/bin/python
"""
@author Nate Vogel
@website http://funtothinkbabout.com
@last_revision 2013-04-14

HeavyWeatherReader parses data output by the La Crosse Weather Station
software for storage in a MySQL DB. HWReader maintains a persistent 
connection to MySQL while it is active. Control+C (SIGINT) is the proper 
way to exit this application.    

Source data generated by: 
  "Standard Version Heavy Weather PC Interface Software For WS-2800 Series"
  http://www.lacrossetechnology.com/alerts/pws/software/setup_hw_wv5_us-1.5.4.exe
  http://www.lacrossetechnology.com/support/software.php

MySQL DB table DDL:
    CREATE TABLE raw_stats (
        meta_actualisation int(11) unsigned,
        meta_timestamp datetime,
        meta_datehour datetime,
        temperature_indoor decimal(4,1) default 0,
        temperature_outdoor decimal(4,1) default 0,
        temperature_dewpoint decimal(4,1) default 0,
        rain_1h decimal(5,2) default 0,
        rain_24h decimal(5,2) default 0,
        rain_total decimal(5,2) default 0,
        wind_direction char(3) default 0,
        wind_direction_deg decimal(4,1) default 0,
        wind_speed decimal(4,1) default 0,
        wind_chill decimal(5,2) default 0,
        humidity_indoor int(3) unsigned default 0,
        humidity_outdoor int(3) unsigned default 0,
        pressure_relative_inhg decimal(4,2) default 0,
    PRIMARY KEY (meta_actualisation),
    INDEX(meta_datehour)
    );
"""
import ConfigParser 
import pymysql 
import signal
from sys import exit
from time import sleep
from datetime import datetime

global debug
debug = False

def read_wd(wd_parser, c_or_f):
    wd = {}

    wd['temperature'] = {
        'outdoor': float(wd_parser.get('outdoor_temperature', 'deg_%s' % c_or_f).strip('"')), 
        'indoor': float(wd_parser.get('indoor_temperature', 'deg_%s' % c_or_f).strip('"')),
        'dewpoint': float(wd_parser.get('dewpoint', 'deg_%s' % c_or_f).strip('"'))
    }
    wd['humidity'] = {
        'outdoor': int(wd_parser.get('outdoor_humidity', 'percent').strip('"')),
        'indoor': int(wd_parser.get('indoor_humidity', 'percent').strip('"'))
    }
    wd['wind'] = {
        'chill': float(wd_parser.get('windchill', 'deg_%s' % c_or_f).strip('"')),
        'speed': float(wd_parser.get('wind_speed', 'mph').strip('"')),
        'direction_deg': float(wd_parser.get('wind_direction', 'deg').strip('"')),
        'direction': wd_parser.get('wind_direction', 'name').strip('"')
    }
    wd['rain'] = {
        'total': float(wd_parser.get('rain_total', 'inch').strip('"')),
        '24h': float(wd_parser.get('rain_24h', 'inch').strip('"')),
        '1h': float(wd_parser.get('rain_1h', 'inch').strip('"'))
    }
    wd['pressure'] = {
        'relative_inhg': float(wd_parser.get('pressure_relative', 'inHg').strip('"'))
    }
    wd['meta'] = {
        'actualisation': int(wd_parser.get('time','last_actualisation').strip('"')),
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'datehour': datetime.now().strftime("%Y-%m-%d %H")
    }

    if debug:
        print wd
    return wd
    

def store_wd(db_cur, wd):
    sql_data_str = "INSERT INTO raw_stats SET "
    for weather_metric,sub_metrics in wd.items():
        for sub_metric,data_value in sub_metrics.items():
            sql_data_str += "%s_%s = '%s'," % (weather_metric, sub_metric, data_value)
    if debug:
        print sql_data_str[:-1]
    # execute insertion, remove trailing ,
    db_cur.execute(sql_data_str[:-1])

"""
main
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

    cfg_parser.read('hwreader.ini')
    current_weather_data_file = cfg_parser.get('heavyweatherreader','current_weather_data_file')
    poll_interval = int(cfg_parser.get('heavyweatherreader','poll_interval'))
    store_c_or_f = cfg_parser.get('heavyweatherreader', 'store_temperature_C_or_F')
    last_actualisation = 0

    weather_data_db = pymysql.connect(host=cfg_parser.get('weather_data_db','host'), user=cfg_parser.get('weather_data_db','user'), passwd=cfg_parser.get('weather_data_db','pass'), db=cfg_parser.get('weather_data_db','name'))
    weather_data_cur = weather_data_db.cursor()

    while True:
        hwd_parser.read(current_weather_data_file)
        wd = read_wd(hwd_parser, store_c_or_f)

        # only store if there is new data
        if wd['meta']['actualisation'] != last_actualisation:
            last_actualisation = wd['meta']['actualisation']
            store_wd(weather_data_cur, wd)
            weather_data_db.commit()

        sleep(poll_interval)

