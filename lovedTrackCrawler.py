"""
Script to extract lists of users' loved tracks, and write to lastfm_lovedTracks.
Crawls all users from lastfm_crawlqueue who we have have already crawled, and uses new column "loved_tracks" to keep track of who we have crawled
"""


from dbSetup import *
import apiMethods
import urllib2 as ul
import sys
import time
import dbMethods
import traceback
import datetime

flag = True
while flag:
	trys = 0
	while trys<5:
		trys += 1
		try:
			ul.urlopen("http://www.last.fm")
			break
		except:	
			traceback.print_exc()
			if trys == 5:
				sys.exit('No Network connection?')
			else:
				time.sleep(30)
		

	cursor = db.cursor()
	cursor.execute("select user_name from lastfm_crawlqueue where (crawl_flag=1 or crawl_flag=3 or crawl_flag=4 or crawl_flag=5) and loved_tracks=0 limit 1;")
	result = cursor.fetchone()
	closeDBConnection(cursor)
	if result:
		username = result[0]
		uid = dbMethods.fetchUID(username)
		cursor=db.cursor()
		cursor.execute("update lastfm_crawlqueue set loved_tracks=1 where user_name=%s",(username))
		closeDBConnection(cursor)
		print username, uid, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		apiMethods.getLovedTracks(username,uid)	
	else:
		print 'queue exhausted'
		sys.exit()