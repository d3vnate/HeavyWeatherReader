#!/usr/bin/python
"""
@author Nate Vogel
@website http://funtothinkabout.com
@last_revision 2013-08-31

Reads stats written from the HeavyWeatherReader app, and uploads data to WUnderground.com
Follows the API format provided http://wiki.wunderground.com/index.php/PWS_-_Upload_Protocol 

"""
import ConfigParser
import pymysql
import signal
import time
import urllib
import urllib2

global debug

"""
encapsulates connection handling in sending weather data
to wunderground. retries 5 times with 5 seconds between attempts.
"""
def send_to_wunderground(upload_url, date_utc):
    conn_attempt = 1
    result_str = "%s >>" % date_utc
    exception_msg = ""

    while conn_attempt < 6 and conn_attempt != 0:
        try:
            conn_status = urllib2.urlopen(upload_url)
            result_str = "%s %s after %d %s" % (result_str, conn_status.read().strip(), conn_attempt, "try" if conn_attempt == 1 else "tries")
            # set successful connection condition
            conn_attempt = 0 
        except Exception, e:
            exception_msg = e
            conn_attempt += 1
            time.sleep(5)

    if conn_attempt == 6:
        # failed 5 retries so give up on this data submission and return err msg
        return "%s %s" % (result_str, exception_msg) 
    else:
        # probably success 
        return result_str


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
    debug = bool(int(cfg_parser.get('heavyweatherreader_wunderground','debug')))

    # leverage hwreader db credentials
    cfg_parser.read('hwreader.ini')

    """
    the stats selection pulls together the most recent data written, as well as generates averages
    as requested by the WUnderground API.
    avg,max calculations utilize derived stats sets ordered by primary key descending so they're always most recent.
    i.e. in a 2 minute interval, there will be no more than 10 sensor updates (but a set of 50 is used for safety sake).
    in effect, I exploit pk ordering to yield a constant set size and performance.
    """
    stats_sql = "select wind_direction_deg as winddir, wind_speed as windspeedmph,\
                 temperature_outdoor as tempf, temperature_dewpoint as dewptf,\
                 wind_chill as temp2f, humidity_outdoor as humidity,\
                 pressure_relative_inhg as baromin,\
                 rain_1h as rainin,\
                 rain_24h as dailyrainin,\
                 (select avg(s1.wind_speed) from\
                    (select wind_speed,meta_timestamp from raw_stats order by meta_actualisation desc limit 50) as s1\
                    where s1.meta_timestamp > date_sub(now(), INTERVAL 2 MINUTE)) as windspdmph_avg2m,\
                 (select avg(s2.wind_direction_deg) from \
                    (select wind_direction_deg,meta_timestamp from raw_stats order by meta_actualisation desc limit 50) as s2\
                    where s2.meta_timestamp > date_sub(now(), INTERVAL 2 MINUTE)) as winddir_avg2m,\
                 (select max(s3.wind_speed) from\
                     (select wind_speed,meta_timestamp from raw_stats order by meta_actualisation desc limit 100) as s3\
                     where s3.meta_timestamp > date_sub(now(), INTERVAL 10 MINUTE)) as windgustmph_avg10m\
                 from raw_stats order by meta_actualisation desc limit 1"

    last_weather_data = {}
    duplicate_count = 0
    while True:
        # TODO: figure out why db connection has to be closed in order to get a clean result dict next run
        # initialize database for weather data storage
        weather_data_db = pymysql.connect(host=cfg_parser.get('weather_data_db','host'),
                                      user=cfg_parser.get('weather_data_db','user'),
                                      passwd=cfg_parser.get('weather_data_db','pass'),
                                      db=cfg_parser.get('weather_data_db','name'))
        weather_data_cur = weather_data_db.cursor(pymysql.cursors.DictCursor)
        weather_data_cur.execute(stats_sql)        

        # fetch data and build request URL
        d = weather_data_cur.fetchone()
        date_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        d['ID'] = wu_id
        d['PASSWORD'] = wu_pass
        d['dateutc'] = date_utc
        d['rtfreq'] = wu_interval

        # avoid uploading duplicate weather data
        if d == last_weather_data :
            duplicate_count += 1
            if duplicate_count > 10:
                print '%s >> No weather data changes in 10 tries. :/ Check weather station.' % date_utc
        else:
            encoded_args = urllib.urlencode(d)
            upload_url = "%s%s" % (wu_url, encoded_args)

            duplicate_count = 0
            last_weather_data = d

            if debug:
                print d,upload_url
            else:
                print send_to_wunderground(upload_url, date_utc)

        d.clear()
        weather_data_cur.close()
        weather_data_db.close()
        time.sleep(wu_interval)
