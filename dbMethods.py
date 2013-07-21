"""
Misc. ethods for interacting with database 
"""

from dbSetup import *
from apiMethods import *
import traceback
import sys

# Retrieves the user ID of name "username" from the DB (assuming it's already in lastfm_userlist)
# Nothing is using this at the moment
def fetchUID(username):
	userID = None
	cursor = db.cursor()
	# all this branching is to check if usernames with spaces have been re-formatted from "name with space" to "name%20with%20space" (html format)
	# and then to check lastfm_extended_user_info in the event that the username is not presetn in the userlist for some reason
	try:
		cursor.execute("select user_id from lastfm_userlist where user_name = %s",(username))
		result = cursor.fetchone()
		if result:
			userID = result[0]
		else:
			cursor.execute("select user_id from lastfm_userlist where user_name = %s",(username.replace(' ','%20')))
			result = cursor.fetchone()
			if result:
				userID = result[0]
			else:
				cursor.execute("select user_id from lastfm_extended_user_info where user_name = %s",(username))
				result = cursor.fetchone()
				if result:
					userID = result[0]
				else:
					cursor.execute("select user_id from lastfm_extended_user_info where user_name = %s",(username.replace(' ','%20')))
					result = cursor.fetchone()
					if result:
						userID= result[0]

	except KeyboardInterrupt:
		raise
	except:
		sys.exit()
		print traceback.format_exc()
		logger.error(traceback.format_exc())
	closeDBConnection(cursor)
	return userID

# retrieve the username of user ID "uid" from lastfm_userlist
def fetchUsername(uid):
	cursor = db.cursor()
	try:
	    cursor.execute("select user_name from lastfm_userlist where user_id = %s",(uid))
	    result = cursor.fetchone()
	    if result:
	    	username = result[0]
	    else:
	    	username = None
	except KeyboardInterrupt:
		raise
	closeDBConnection(cursor)
	return username	