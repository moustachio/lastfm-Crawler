"""
Main script for crawling user annotations (crawls via friendship relations, hence the name)
"""

import apiMethods
from dbSetup import *
from dbMethods import *
from htmlParsingMethods import *
import datetime
import sys

# Cleanup function for when we trigger a KeyboardInterrupt
# Deletes all annotations and errors for the user we're looking at, and resets crawl_flag to 0
def cleanup(username):
	print 'Cleaning up username %s' % (username)
	cursor = db.cursor()
	cursor.execute("select user_id from lastfm_userlist where user_name=%s",(username))
	result = cursor.fetchone()
	if result:
		uid = result[0]
		print 'Deleting annotations...'
		cursor.execute("delete from lastfm_annotations where user_id=%s",(uid))
		print 'Deleting any errors...'
		cursor.execute("delete from lastfm_errorqueue where user_id=%s",(uid))
	print 'Updating crawl flag...'
	cursor.execute("update lastfm_crawlqueue set crawl_flag=0 where user_name=%s",(username))
	closeDBConnection(cursor)
	print 'Done!'

# Main loop. Runs as long as their are uncrawled users in the queue
crawlFlag = True
while crawlFlag:
	username = None
	try:

		# Check network connectivity
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

		# update the crawl queue (function grabs a user with crawl_flag=0)
		# maybe do away with this in future version?
		cursor = db.cursor()                        
		cursor.callproc("updatecrawlerqueue",('@'))
		cursor.execute("SELECT @_updatecrawlerqueue_0")
		username = cursor.fetchone()[0]
		closeDBConnection(cursor)

		if username:

			# get the user's numeric ID, and list of friends 
			print username
			friends,uid = apiMethods.extractFriends(username)

			# add each friend to the crawl queue (with default crawl_flag=0)
			for name in friends:
				if name:
					cursor = db.cursor()
					cursor.execute("INSERT IGNORE INTO lastfm_crawlqueue (user_name) VALUES (%s)", (name))
					closeDBConnection(cursor)    

			# Get current user's annotations
			if uid:
				# start with the list of unique tags
				tags = getUserTags(username,uid)
				if tags:
					# if any, get annotations for each tag
					getUserAnnotations(tags,username,uid)
			# flag with crawl_flag=2 if we weren't able to get a uid
			else:
				cursor = db.cursor()
				cursor.execute("update lastfm_crawlqueue set crawl_flag=2 where user_name=%s", (username))
		
		else:
			crawlFlag = False
	
	# cleanup up and wuite if we trigger a KeyboardInterrupt
	except KeyboardInterrupt:
		print '...Interrupt!'
		closeDBConnection(cursor)
		if username:
			cleanup(username)
		sys.exit()
			
