#!/usr/bin/python
"""
@author Nate Vogel
@website http://funtothinkbabout.com
@last_revision 2013-04-19

Reads stats written from the HeavyWeatherReader app, and uploads data to WUnderground.com
Follows the API format provided http://wiki.wunderground.com/index.php/PWS_-_Upload_Protocol 

"""
import ConfigParser
import pymysql
import signal
import time
import urllib

global debug
debug = False

"""
main
"""
if __name__=="__main__":

    def signal_handler(signalnum, frame):
        if debug:
            print "SIGNAL CAUGHT: %d" % signalnum
        if signalnum == signal.SIGINT:
            print "Exiting..."
            try:
                weather_data_cur.close()
                weather_data_db.close()
            except pymysql.err.Error, e:
                print e 
            exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    cfg_parser = ConfigParser.SafeConfigParser()
    hwd_parser = ConfigParser.SafeConfigParser()

    # get and set configuration options
    cfg_parser.read('hwr_wunderground.ini')
    wu_id = cfg_parser.get('heavyweatherreader_wunderground','user_id')
    wu_pass = cfg_parser.get('heavyweatherreader_wunderground','passwd')
    wu_url = cfg_parser.get('heavyweatherreader_wunderground','wu_api_url')
    wu_interval = int(cfg_parser.get('heavyweatherreader_wunderground','wu_interval'))

    # leverage hwreader db credentials
    cfg_parser.read('hwreader.ini')

    """
    the stats selection pulls together the most recent data written, as well as generates averages
    as requested by the WUnderground API.
    not currently an efficient select, produces full tablescan produced 
    TODO: improve raw_stats schema &/| split raw data into subtables -- maybe done by hwreader.py
    """
    stats_sql = "select wind_direction_deg as winddir, wind_speed as windspeedmph,\
                 temperature_outdoor as tempf, temperature_dewpoint as dewptf,\
                 pressure_relative_inhg as baromin, humidity_outdoor as humidity,\
                 rain_1h as rainin,\
                 (select sum(rain_1h) from raw_stats where meta_datehour >= concat(curdate(), ' ', '00:00:00')) as dailyrain,\
                 (select avg(wind_speed) from raw_stats where meta_timestamp > date_sub(now(), INTERVAL 2 MINUTE)) as windspdmph_avg2m,\
                 (select avg(wind_direction_deg) from raw_stats where meta_timestamp > date_sub(now(), INTERVAL 2 MINUTE)) as winddir_avg2m,\
                 (select max(wind_speed) from raw_stats where meta_timestamp > date_sub(now(), INTERVAL 10 MINUTE)) as windgustmph_avg10m\
                 from raw_stats order by meta_actualisation desc limit 1" 

    while True:
        # TODO: figure out why db connection ha to be closed in order to get a clean result dict next run
        # initialize database for weather data storage
        weather_data_db = pymysql.connect(host=cfg_parser.get('weather_data_db','host'),
                                      user=cfg_parser.get('weather_data_db','user'),
                                      passwd=cfg_parser.get('weather_data_db','pass'),
                                      db=cfg_parser.get('weather_data_db','name'))
        weather_data_cur = weather_data_db.cursor(pymysql.cursors.DictCursor)
        weather_data_cur.execute(stats_sql)        

        d = weather_data_cur.fetchone()
        date_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        upload_url = "%s?ID=%s&PASSWORD=%s&dateutc=%s&winddir=%s&windspeedmph=%s&dailyrain=%s&tempf=%s&rainin=%s&baromin=%s&dewptf=%s&humidity=%s&windspdmph_avg2m=%s&winddir_avg2m=%s&windgustmph_10m=%s&softwaretype=ftta_wunderground&action=updateraw&realtime=1&rtfreq=%s" % (wu_url, wu_id, wu_pass, date_utc, d['winddir'], d['windspeedmph'], d['dailyrain'], d['tempf'], d['rainin'], d['baromin'], d['dewptf'], d['humidity'], d['windspdmph_avg2m'], d['winddir_avg2m'], d['windgustmph_avg10m'], wu_interval)

        if debug:
            print d,upload_url
        else:
            status = urllib.urlopen(upload_url)
            print "%s >> %s" % (date_utc, status.read().strip())
            status.close()

        d.clear()
        weather_data_cur.close()
        weather_data_db.close()
        time.sleep(wu_interval)
