"""
Functions for extracting listening information from last.fm api. Designed to enforce that we crawl roughly the same number of users from each of our 4 defined tagging levels.
Also collects extended_user_info for each user
"""

import apiMethods
from dbSetup import *
from dbMethods import *
from htmlParsingMethods import *
import datetime


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
        return 0
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
    

### Get counts so far
counts = {}
cursor = db.cursor()
cursor.execute("select count(*) from lastfm_extended_user_info where anno_count<10;")
counts[0] = cursor.fetchone()[0]
cursor.execute("select count(*) from lastfm_extended_user_info where anno_count>=10 and anno_count<100;")
counts[1] = cursor.fetchone()[0]
cursor.execute("select count(*) from lastfm_extended_user_info where anno_count>=100 and anno_count<1000;")
counts[2] = cursor.fetchone()[0]
cursor.execute("select count(*) from lastfm_extended_user_info where anno_count>=1000;")
counts[3] = cursor.fetchone()[0]
closeDBConnection(cursor)

### Determine the next tagger level to crawl
if counts[0]==counts[3]:
    next=0
elif counts[2]!=counts[3]:
    next=3
elif counts[1]!=counts[2]:
    next=2
else:
    next=1

crawlFlag = True
while crawlFlag:
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
		cursor.execute("SELECT * from lastfm_crawlqueue where crawl_flag=1 limit 1;")
		result = cursor.fetchone()
		closeDBConnection(cursor)

		if result:
			
			username = result[0]
			print username
			uid = apiMethods.getUserInfo(username) # get extended user information
			
			if uid:
				print username, uid
				
				# update crawl queue to say we have craw
				cursor = db.cursor()                        
				cursor.execute("UPDATE lastfm_crawlqueue set crawl_flag=3 where user_name=%s;",(username))
				
				# Get user's annotation count and write to extended_user_info_table
				cursor.execute("SELECT count(*) from lastfm_annotations where user_id=%s",(uid))
				annoCount = cursor.fetchone()[0]
				cursor.execute("UPDATE lastfm_extended_user_info set anno_count=%s where user_id=%s",(annoCount,uid))
				closeDBConnection(cursor)
				
				# calculate tagger level, and get scrobbles if it's the next level we want to crawl
				level = taggerLevel(annoCount)
				print level
				if level==next:
					
					print 'Collecting scrobbles...'
					counts[next]+=1 # update tagger level count
					next = nextLevel(next) # increment value for next tagger level

					# update crawl flag to say we've crawled scrobbles for this user (crawl_flag=4)
					cursor = db.cursor()
					cursor.execute("UPDATE lastfm_crawlqueue set crawl_flag=4 where user_name=%s;",(username))
					closeDBConnection(cursor)

					# get the scrobbles!
					apiMethods.getUserScrobbles(username,uid)
			
			# Flag users in the crawlqueue if we can't get their UIDs
			else:
				cursor=db.cursor()
				cursor.execute("UPDATE lastfm_crawlqueue set crawl_flag=22 where user_name=%s;",(username))
				closeDBConnection(cursor)       		
		else:
			crawlFlag = False

	# clean things up if we trigger a Keyboard Interrupt
	except KeyboardInterrupt:
		print '...Interrupt!'
		closeDBConnection(cursor)
		if username:
			cleanup(username)
		sys.exit()
