"""
Error Processor. Pulls errors from the errorqueue and deals with them.
"""

import apiMethods
from dbSetup import *
from dbMethods import *
from htmlParsingMethods import *
import datetime
import time
import sys

# cleanup function for when we trigger a keyboard interrupt. Basically checks if the error we're working has been deleted from the queue, and if so, throws it back in.
def cleanup(uid,error_type,desc):
	print 'Cleaning up user ID %s' % (uid)
	cursor = db.cursor()
	if error_type=='annotations':
		cursor.execute("select * from lastfm_errorqueue where user_id=%s and error_type=%s and tag_name=%s",(uid,error_type,desc))
	else:
		cursor.execute("select * from lastfm_errorqueue where user_id=%s and error_type=%s",(uid,error_type))
	result = cursor.fetchone()
	if result:
		print "Error still there...we're OK"
	else:
		print "Re-inserting error..."
		cursor.execute("insert into lastfm_errorqueue (user_id,error_type,tag_name,retry_count) values (%s,%s,%s,0);",(uid,error_type,desc))
	closeDBConnection(cursor)
	print 'Done!'

# Main loop, runs indefinitely
while True:
	uid = None
	error_type = None
	desc = None
	retry_count = None
	
	try:

		# check for network connectivity, and shut things down if we don't have it.
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

		
		# randomly select an error from the queue where the user is not banned/non-existent (404) or has private listening history (403)
		cursor = db.cursor()                        
		cursor.execute("SELECT * from lastfm_errorqueue where retry_count != '404'and retry_count != '403' and error_type!='tags' and error_type!='annotations' order by rand() limit 1")
		result = cursor.fetchone()
		closeDBConnection(cursor)
		
		if result:
			uid = str(result[0])
			print uid
			username = fetchUsername(uid)

			# if for some reason we have a user_id in the errorqueue, but don't have the user_name in lastfm_userlist, get that information before moving on
			if not username:
				try:
					username = apiMethods.nameFromUID(uid)
					cursor=db.cursor()
					if username:
						cursor.execute("insert into lastfm_userlist (user_id,user_name) values (%s,%s);",(uid,username))
						cursor.execute("insert ignore into lastfm_crawlqueue (user_name) values (%s);",(username))
						closeDBConnection(cursor)
					else:
						cursor.execute("delete from lastfm_errorqueue where user_id=%s;",(uid))
						closeDBConnection(cursor)
						continue 
				except:
					continue

			# get descriptors for the error
			error_type = result[1]
			desc = result[2]
			print username, uid, error_type, desc
			
			# If spaces in the username caused the problem, replace them with '%20's
			if ' ' in username:
				new = username.replace(' ','%20')
				cursor=db.cursor()
				cursor.execute("update lastfm_userlist set user_name=%s where user_name=%s;",(new,username))
				closeDBConnection(cursor)
				username = new
			
			# now that we're working on the error, delete it from the queue (and retry if the DB hangs)
			errorDeleted = False
			while not errorDeleted:
				try:
					cursor=db.cursor()
					if error_type=='annotations':
						cursor.execute("DELETE from lastfm_errorqueue where user_id=%s and error_type=%s and tag_name=%s;",(uid,error_type,desc))
					else:
						cursor.execute("DELETE from lastfm_errorqueue where user_id=%s and error_type=%s;",(uid,error_type))
					closeDBConnection(cursor)
					errorDeleted=True
				except:
					print 'Retrying...'
					continue

			# Now call the appropriate HTML/API parsing method to handle the type of error we're looking at.
			if error_type == 'tags':
				tags = getUserTags(username,uid)
				if tags:
					getUserAnnotations(tags,username,uid)
			elif error_type == 'annotations':
				tags = (desc,)
				getUserAnnotations(tags,username,uid)
			elif error_type == 'friends':
				friends,uid = apiMethods.extractFriends(username)
			elif error_type=='scrobbles':
				# we record the last recorded scrobble timestamp in the 'desc', so if that's present use the 'getUserScrobblesBefore' method
				if desc:
					apiMethods.getUserScrobblesBefore(username,uid,desc)
				else:
					apiMethods.getUserScrobbles(username,uid)
			elif error_type == 'loved':
				apiMethods.getLovedTracks(username,uid)
			elif error_type == 'banned':
				apiMethods.getbannedTracks(username,uid)
		
		# if the queue is empty, wait for ten seconds before checking again
		else:
			print 'queue exhausted'
			sys.exit()
			time.sleep(10)
	
	# if we trigger a KeyboardInterrupt, close things and cleanup
	except KeyboardInterrupt:
		print '...Interrupt!'
		closeDBConnection(cursor)
		if uid:
			cleanup(uid,error_type,desc)
		sys.exit()
		cleanup(uid)
