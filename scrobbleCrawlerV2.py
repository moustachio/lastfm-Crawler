"""
Functions for extracting listening information from last.fm api. Designed to enforce that we crawl roughly the same number of users from each of our 4 defined tagging levels.

UPDATED VERSION DOES NOT COLLECT EXTENDED INFO, AND SHOULD GO ON INDEFINITELY

NOW FLAGGING ALL USERS WITH TAGGING LEVEL = 0 WITH CRAWL_FLAG=5
"""

import apiMethods
from dbSetup import *
from dbMethods import *
from htmlParsingMethods import *
import datetime
import random


# We have defined four "tagger levels", based on the user's total number of annotations. This determines that level.
def taggerLevel(annoCount):
    annoCount = int(annoCount)
    if annoCount <10:
        return 0
    elif annoCount>=10 and annoCount<100:
        return 1
    elif annoCount>=100 and annoCount<1000:
        return 2
    elif annoCount>=1000:
        return 3

# What level of tagger should we crawl next?
def nextLevel(last):
    if last == 3:
        return 1
    else:
        return last + 1

# Cleanup function to be triggered on Keyboard interrupt
def cleanup(username):
	print 'Cleaning up username %s' % (username)
	cursor = db.cursor()
	cursor.execute("select user_id from lastfm_userlist where user_name=%s",(username))
	result = cursor.fetchone()
	uid = result[0]
	print 'Deleting scrobbles...'
	cursor.execute("delete from lastfm_scrobbles where user_id=%s",(uid))
	print 'Deleting any errors...'
	cursor.execute("delete from lastfm_errorqueue where user_id=%s and error_type='scrobbles';",(uid))
	print 'Updating crawl flag...'
	cursor.execute("update lastfm_crawlqueue set crawl_flag=1 where user_name=%s",(username))
	closeDBConnection(cursor)
	print 'Done!'
    
# Just start randomly now
next = random.randint(1,3)

# Get total number of people left to crawl
cursor = db.cursor()
cursor.execute("select count(*) from lastfm_crawlqueue where crawl_flag=3;")
total = cursor.fetchone()[0]
closeDBConnection(cursor)

crawlFlag = True
while crawlFlag and total >= 0:
	try:
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


		# Grab a username from the crawl queue for which we have already crawled annotations (crawl_flag=1)
		cursor = db.cursor()                        
		offset = random.randint(0,total)
		cursor.execute("SELECT * from lastfm_crawlqueue where crawl_flag=3 or crawl_flag=1 limit 1 offset %s;",(offset))
		#cursor.execute("SELECT * from lastfm_crawlqueue where user_name='JaimieMurdock' and crawl_flag=3")
		result = cursor.fetchone()
		closeDBConnection(cursor)

		if result:
			
			username = result[0]
			print username
			cursor = db.cursor()
			cursor.execute("select user_id from lastfm_extended_user_info where user_name=%s",(username))
			uid = cursor.fetchone()
			
			if uid:
				uid = uid[0]	
				print username, uid, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")			
				
				# Get user's annotation count and write to extended_user_info_table
				cursor.execute("SELECT anno_count from lastfm_extended_user_info where user_id=%s",(uid))
				annoCount = cursor.fetchone()[0]
				closeDBConnection(cursor)
				
				# calculate tagger level, and get scrobbles if it's the next level we want to crawl
				level = taggerLevel(annoCount)
				print level

				log = open('scrobbleLog','a')
				log.write(','.join([str(i) for i in [username,uid,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), level]])+'\n')
				log.close()


				if level == 0:
					cursor=db.cursor()
					cursor.execute("UPDATE lastfm_crawlqueue set crawl_flag=5 where user_name=%s;",(username))
					closeDBConnection(cursor)

				else:
					
					print 'Collecting scrobbles...'
					next = nextLevel(next) # increment value for next tagger level

					# update crawl flag to say we've crawled scrobbles for this user (crawl_flag=4)
					cursor = db.cursor()
					cursor.execute("UPDATE lastfm_crawlqueue set crawl_flag=4 where user_name=%s;",(username))
					closeDBConnection(cursor)

					# get the scrobbles!
					apiMethods.getUserScrobbles(username,uid)

					total -= 1
			
			# Flag users in the crawlqueue if we can't get their UIDs
			else:
				cursor=db.cursor()
				cursor.execute("UPDATE lastfm_crawlqueue set crawl_flag=22 where user_name=%s;",(username))
				closeDBConnection(cursor)       		
		else:
			cursor = db.cursor()
			cursor.execute("select count(*) from lastfm_crawlqueue where crawl_flag=3;")
			total = cursor.fetchone()[0]
			closeDBConnection(cursor)
			if total > 0:
				continue
			else:
				crawlFlag = False

	# clean things up if we trigger a Keyboard Interrupt
	except KeyboardInterrupt:
		print '...Interrupt!'
		closeDBConnection(cursor)
		if username:
			cleanup(username)
		sys.exit()
