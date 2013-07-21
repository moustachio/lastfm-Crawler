"""
Sets up interface with database
"""

import MySQLdb
import logging

def closeDBConnection(cursor):
        db.commit()
        cursor.close()
        
db = MySQLdb.connect(host="localhost", user="root", passwd="root",db="crawler_lastfm")


# We should change how we're handling logging, but we'll leave it as is for now
logger = logging.getLogger('lastfm')
hdlr = logging.FileHandler('lastfm.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.WARNING)
