"""
Original version of friend crawler was only getting the first page of a user's friend list. This goes back and ensures we have all friends for all users.
"""

import dbMethods
import MySQLdb
import urllib2 as ul
from lxml import etree
import sys
import traceback
import time

def closeDBConnection(cursor):
        db.commit()
        cursor.close()
       
db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root",db="crawler_lastfm")
api = 'a7783eed0e7a281f855704fab477a1d3'


# Modified version of extractFriends for use only in this script
def extractFriends(username):

	uid = dbMethods.fetchUID(username)
	print uid
	if not uid:
		return None
	friends = []
	errorType='friends'
	friendsUrl = 'http://ws.audioscrobbler.com/2.0/?method=user.getfriends&user='+username.replace(' ','%20')+'&api_key='+api
	
	try:
		# get tree containing list of user's friends
		page = ul.urlopen(friendsUrl).read()
		tree = etree.fromstring(page)
		totalPages = int(tree[0].get('totalPages'))
	
	except KeyboardInterrupt:
		raise               
	
	# handle errors if any 
	except:
		print username
		cursor = db.cursor()                        
		cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
		closeDBConnection(cursor)
		errorRecorded = True
		return friends
		
	# if we succesfully retrieved a UID, record all the friendship relations

	for page in [p+1 for p in range(totalPages)]:
		# after the first page, specify which page we're on
		if page > 1:
			try:
				friendsUrl='http://ws.audioscrobbler.com/2.0/?method=user.getfriends&user='+username.replace(' ','%20')+'&page='+str(page)+'&api_key='+api
				tree = etree.fromstring(ul.urlopen(friendsUrl).read())
			except:
				print username
				cursor = db.cursor()                        
				if uid:
					cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
				else:
					cursor.execute('UPDATE lastfm_crawlqueue SET crawl_flag=%s WHERE user_name=%s',(2,username))
					closeDBConnection(cursor)
					errorRecorded = True
				return friends
		# for each friend
		for i in tree[0]:

				# assign uid to friend_id1, and name of current friend to friend_id2
				friend_id1 = str(uid)
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
				try:
					cursor=db.cursor()                          
					cursor.execute('INSERT IGNORE INTO lastfm_friendlist (friend_id1, friend_id2,sanity_check_id) VALUES ('+friend_id1+','+friend_id2+',"'+friend_id1+'_'+friend_id2+'")')
					closeDBConnection(cursor)
				except KeyboardInterrupt:
					raise

	return friends

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
	cursor.execute("select user_name from lastfm_crawlqueue where (crawl_flag=1 or crawl_flag=3 or crawl_flag=4) and (friends_fixed=0) limit 1;")
	result = cursor.fetchone()
	closeDBConnection(cursor)
	if result:
		username = result[0]
		cursor=db.cursor()
		cursor.execute("update lastfm_crawlqueue set friends_fixed=1 where user_name=%s",(username))
		closeDBConnection(cursor)
		print username
		friends = extractFriends(username)	
		if friends:
			cursor = db.cursor()
			for name in friends:
				if name:
					cursor.execute("INSERT IGNORE INTO lastfm_crawlqueue (user_name) VALUES (%s)", (name))
			closeDBConnection(cursor)    
	else:
		flag = False
