"""
Methods for extracting data from the Last.fm API
"""

api = open('api.key').read().strip()
import urllib2 as ul
from lxml import etree
from dbSetup import *
import sys, traceback
import time

# Default parser doesn't play nice with special characters, so use this to handle utf8
parser=etree.XMLParser(encoding='utf-8',recover=True)

# given a username, returns the corresponding user ID (from API)
def UIDfromName(username):
	try:
		url = 'http://ws.audioscrobbler.com/2.0/?method=user.getinfo&user='+username.replace(' ','%20')+'&api_key='+api
		page = ul.urlopen(url).read()
		tree = etree.fromstring(page)
		uid = tree[0].findtext('id')
		return uid
	except KeyboardInterrupt:
		raise

# given a numeric user ID, retrieve corresponding username from API
# use with caution! If there's a user with a *name* the same as the number (e.g. "http://www.last.fm/user/99999"), it will retrieve that user, instead of the username with that numeric ID!
def nameFromUID(uid):
	try:
		url = 'http://ws.audioscrobbler.com/2.0/?method=user.getinfo&user='+str(uid)+'&api_key='+api
		page = ul.urlopen(url).read()
		tree = etree.fromstring(page)
		name = tree[0].findtext('name')
		if name != str(uid):
			return name
		else:
			return none
	except KeyboardInterrupt:
		raise

#Given a username, extracts all the friend IDs for that userID
def extractFriends(username):

	friends = []
	uid = None
	errorType='friends'
	infoUrl = 'http://ws.audioscrobbler.com/2.0/?method=user.getinfo&user='+username.replace(' ','%20')+'&api_key='+api
	friendsUrl = 'http://ws.audioscrobbler.com/2.0/?method=user.getfriends&user='+username.replace(' ','%20')+'&api_key='+api
	
	try:
		# get user ID and add info to lastfm_userlist
		page = ul.urlopen(infoUrl).read()
		tree = etree.fromstring(page)
		uid = tree[0].findtext('id')
		cursor = db.cursor()
		cursor.execute('INSERT IGNORE INTO lastfm_userlist (user_id, user_name) VALUES (%s,%s)', (uid,username))
		closeDBConnection(cursor)		
		# get tree containing list of user's friends
		page = ul.urlopen(friendsUrl).read()
		tree = etree.fromstring(page)
		totalPages = int(tree[0].get('totalPages'))
	
	except KeyboardInterrupt:
		raise               
	
	# handle errors if any 
	except:
		print username
		print traceback.format_exc()
		cursor = db.cursor()                        
		if uid:
			cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
		else:
			cursor.execute('UPDATE lastfm_crawlqueue SET crawl_flag=%s WHERE user_name=%s',(2,username))
			closeDBConnection(cursor)
		return friends, uid
	
	# if we succesfully retrieved a UID, record all the friendship relations
	if uid:
		for page in [p+1 for p in range(totalPages)]:
			# after the first page, specify which page we're on
			if page > 1:
				try:
					friendsUrl='http://ws.audioscrobbler.com/2.0/?method=user.getfriends&user='+username.replace(' ','%20')+'&page='+str(p)+'&api_key='+api
					tree = etree.fromstring(ul.urlopen(friendsUrl).read())
				except:
					print username
					print traceback.format_exc()
					cursor = db.cursor()                        
					if uid:
						cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
					else:
						cursor.execute('UPDATE lastfm_crawlqueue SET crawl_flag=%s WHERE user_name=%s',(2,username))
					closeDBConnection(cursor)
					return friends, uid
			# for each friend
			cursor=db.cursor() 
			for i in tree[0]:
				# assign uid to friend_id1, and name of current friend to friend_id2
				friend_id1 = uid
				friend_id2 = i.findtext('id')
				newFriend = friend_id2
				# add friend's name to friendlist 
				friendName = i.findtext('name')
				friends.append(friendName)     

				# ensure that friend_id1 is the smaller of the two ids (for database purposes)
				# sanity check id is concatenation of the two IDs, with the smaller one first
				if int(friend_id1) > int(friend_id2):
					tempId = friend_id1
					friend_id1 = friend_id2
					friend_id2 = tempId

				# add friendship relation to database (retrying if DB hangs)
				cursor.execute('INSERT IGNORE INTO lastfm_friendlist (friend_id1, friend_id2,sanity_check_id) VALUES ('+friend_id1+','+friend_id2+',"'+friend_id1+'_'+friend_id2+'")')
			closeDBConnection(cursor)

	return friends, uid
        
# get listening history of user and write to DB
def getUserScrobbles(username,uid):
	
	timestamp = None
	errorType = 'scrobbles'
	
	try:
		# use getRecentTracks API method, with max of 200 entries per page (this gets the first page)
		url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user='+username.replace(' ','%20')+'&limit=200&api_key='+api
		tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
		totalPages = int(tree[0].get('totalPages'))
		
		for page in [p+1 for p in range(totalPages)]:
			
			# after the first page, specify which page we're on
			if page > 1:
				url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user='+username.replace(' ','%20')+'&limit=200&page='+str(page)+'&api_key='+api
			
			tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
			
			# For each scrobble, get the unixtime (uts), convert to MySQL format, and write to database
			cursor = db.cursor()
			for i in tree[0]:
				trackURL = i.findtext('url')[25:]
				timestamp = int(i.find('date').get('uts'))
				timestamp = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(timestamp))
				cursor.execute("INSERT IGNORE INTO lastfm_scrobbles (user_id, item_url, scrobble_time) VALUES (%s,%s,%s)", (uid,trackURL,timestamp))
			closeDBConnection(cursor)
	
	except KeyboardInterrupt:
		raise
	
	# write any error to DB, retrying if DB hangs
	except ul.HTTPError, error:
		
		contents = error.read()
		try:
			# if user's recent tracks are private, update error queue to refelct this (403)
			if "private" in contents:
				cursor = db.cursor()
				cursor.execute("insert into lastfm_errorqueue (user_id,error_type,retry_count) values (%s,%s,%s)",(uid,errorType,'403'))
			else:
				print traceback.format_exc()
				cursor = db.cursor()
				# we want to record whatever the last time stamp we grabbed was, so we don't have to recrawl everything when we process the error
				if timestamp:
					cursor.callproc("UpdateErrorQueue", (uid,errorType,timestamp,'@retryCount'))
				else:
					cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
				closeDBConnection(cursor)
			errorRecorded = True
		
		except KeyboardInterrupt:
			raise
			
	
	# catch any other errors
	except:
		try:
			print traceback.format_exc()
			cursor = db.cursor()
			if timestamp:
				cursor.callproc("UpdateErrorQueue", (uid,errorType,timestamp,'@retryCount'))
			else:
				cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
			closeDBConnection(cursor)
			errorRecorded=True
		except KeyboardInterrupt:
			raise


# Gets all scrobbles recorded before time T
# same as getUserScrobbles method, except it uses the "to" option of the getRecentTracks method to record only scrobbles before a specified time
# we get the time from the error queue
def getUserScrobblesBefore(username,uid,t):
	timestamp = None
	errorType = 'scrobbles'
	try:
		unixtime = int(time.mktime(time.strptime(t,'%Y-%m-%d %H:%M:%S')))
		url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user='+username.replace(' ','%20')+'&limit=200&api_key='+api+'&to='+str(unixtime)
		tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
		totalPages = int(tree[0].get('totalPages'))
		for page in [p+1 for p in range(totalPages)]:
			if page > 1:
				url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user='+username.replace(' ','%20')+'&limit=200&page='+str(page)+'&api_key='+api+'&to='+str(unixtime)
			tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
			cursor = db.cursor()
			for i in tree[0]:
				trackURL = i.findtext('url')[25:]
				timestamp = int(i.find('date').get('uts'))
				timestamp = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(timestamp))
				cursor.execute("INSERT IGNORE INTO lastfm_scrobbles (user_id, item_url, scrobble_time) VALUES (%s,%s,%s)", (uid,trackURL,timestamp))
			closeDBConnection(cursor)
	except KeyboardInterrupt:
		raise
	except ul.HTTPError, error:
		contents = error.read()
		if "private" in contents:
			cursor = db.cursor()
			cursor.execute("insert into lastfm_errorqueue (user_id,error_type,retry_count) values (%s,%s,%s)",(uid,errorType,'403'))
			closeDBConnection(cursor)
		else:
			print traceback.format_exc()
			cursor = db.cursor()
			if timestamp:
				cursor.callproc("UpdateErrorQueue", (uid,errorType,timestamp,'@retryCount'))
			else:
				cursor.callproc("UpdateErrorQueue", (uid,errorType,t,'@retryCount'))
			closeDBConnection(cursor)
		errorRecorded = True
	except:
		print traceback.format_exc()
		cursor = db.cursor()
		if timestamp:
			cursor.callproc("UpdateErrorQueue", (uid,errorType,timestamp,'@retryCount'))
		else:
			cursor.callproc("UpdateErrorQueue", (uid,errorType,t,'@retryCount'))
		closeDBConnection(cursor)
		errorRecorded = True

# Retrieved extended user info using user.getInfo API method
def getUserInfo(username):
	try:

		# shorthand for extracting value of a given user info variable
		def getVal(tree,name):
			return tree[0].findtext(name)

		results = []
		url = "http://ws.audioscrobbler.com/2.0/?method=user.getinfo&user="+username.replace(' ','%20')+"&api_key="+api
		tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
		
		for item in ('id', 'country', 'age', 'gender', 'subscriber', 'playcount', 'playlists', 'bootstrap', 'registered', 'type'):
			value = tree[0].findtext(item)
			if not value:
				results.append(None)
			else:
				results.append(value)
		
		cursor = db.cursor()
		cursor.execute("insert ignore into lastfm_extended_user_info (user_name,user_id,country,age,gender,subscriber,playcount,playlists,bootstrap,registered,type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", [username]+results)#(username,id,country,age,gender,subscriber,playcount,playlists,bootstrap,registered,type))
		closeDBConnection(cursor)
		
		# return user's ID
		return results[0]
	
	except KeyboardInterrupt:
		raise
	
	except:
		return False

# Record timestamped list of a user's loved tracks
def getLovedTracks(username,uid):
	errorType = 'loved'
	
	try:
		# use getLovedTracks API method, with 50 entries per page (this gets the first page)
		url = 'http://ws.audioscrobbler.com/2.0/?method=user.getlovedtracks&user='+username.replace(' ','%20')+'&limit=1000&api_key='+api
		tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
		totalPages = int(tree[0].get('totalPages'))
		if totalPages>0:
			for page in [p+1 for p in range(totalPages)]:
				
				# after the first page, specify which page we're on
				if page > 1:
					url = 'http://ws.audioscrobbler.com/2.0/?method=user.getlovedtracks&user='+username.replace(' ','%20')+'&page='+str(page)+'&limit=1000&api_key='+api
					tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
				
				# For each scrobble, get the unixtime (uts), convert to MySQL format, and write to database
				cursor = db.cursor()
				for track in tree[0]:
					trackURL = track.findtext('url')[25:]
					timestamp = int(track.find('date').get('uts'))
					timestamp = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(timestamp))
					cursor.execute("INSERT IGNORE INTO lastfm_lovedTracks (user_id, item_url, love_time) VALUES (%s,%s,%s)", (uid,trackURL,timestamp))
				closeDBConnection(cursor)
		
	except KeyboardInterrupt:
		raise
	
	# catch any other errors
	except:
		try:
			print traceback.format_exc()
			cursor = db.cursor()
			cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
			closeDBConnection(cursor)
		except KeyboardInterrupt:
			raise

#Record timestamped list of a user's *banned* tracks
def getBannedTracks(username,uid):
	errorType = 'banned'
	
	try:
		# use getLovedTracks API method, with 50 entries per page (this gets the first page)
		url = 'http://ws.audioscrobbler.com/2.0/?method=user.getbannedtracks&user='+username.replace(' ','%20')+'&limit=1000&api_key='+api
		tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
		totalPages = int(tree[0].get('totalPages'))
		if totalPages>0:
			for page in [p+1 for p in range(totalPages)]:
				
				# after the first page, specify which page we're on
				if page > 1:
					url = 'http://ws.audioscrobbler.com/2.0/?method=user.getbannedtracks&user='+username.replace(' ','%20')+'&page='+str(page)+'&limit=1000&api_key='+api
					tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
				
				# For each scrobble, get the unixtime (uts), convert to MySQL format, and write to database
				cursor = db.cursor()
				for track in tree[0]:
					trackURL = track.findtext('url')[25:]
					timestamp = int(track.find('date').get('uts'))
					timestamp = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(timestamp))
					cursor.execute("INSERT IGNORE INTO lastfm_bannedtracks (user_id, item_url, ban_time) VALUES (%s,%s,%s)", (uid,trackURL,timestamp))
				closeDBConnection(cursor)
		
	except KeyboardInterrupt:
		raise
	
	# catch any other errors
	except:
		try:
			print traceback.format_exc()
			cursor = db.cursor()
			cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
			closeDBConnection(cursor)
		except KeyboardInterrupt:
			raise

# gets total playcount (as reocorded in "getRecentTracks" API call, not as listed on profile)
def getTotalPlaycount(username):
	url = 'http://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user='+username.replace(' ','%20')+'&api_key='+api
	tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
	return int(tree[0].get('total'))

