import urllib2 as ul
from lxml import etree
parser=etree.XMLParser(encoding='utf-8',recover=True)
import time
import datetime
import sys

import MySQLdb
db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root",db="analysis_lastfm",use_unicode=True,charset='utf8')
cursor = db.cursor()


api = open('api.key').read().strip()

def getUserInfo(username):

		# shorthand for extracting value of a given user info variable
		def getVal(tree,name):
			return tree[0].findtext(name)

		url = "http://ws.audioscrobbler.com/2.0/?method=user.getinfo&user="+username.replace(' ','%20')+"&api_key="+api
		tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
		
		value = tree[0].findtext('realname')

		return value

result = True
count = 0 
start = time.time()

if result:
	cursor.execute("select user_id, user_name from temp_users where realname is null;") # and user_id > 19500000;
	result = cursor.fetchall()
	n = float(len(result))

	for user_id,user_name in result:
		try:
			name = getUserInfo(user_name).encode('utf8')
			if not name:
				name = '<UNKNOWN>'
			cursor.execute(u"update temp_users set realname=%s where user_id=%s",(name,user_id))
			sys.stdout.write(' '.join(map(str,[count,str(count/n),user_id,user_name,name,str(datetime.timedelta(seconds=(time.time()-start))),str(datetime.timedelta(seconds=(time.time()-start)/(count+1)))]))+'\n')
			sys.stdout.flush()
			count += 1

		except KeyboardInterrupt:
			sys.exit()
			
		except ul.HTTPError, error:
			contents = error.read()
			if 'No user with that name was found' in contents:
				sys.stdout.write('User %s (%s) NOT FOUND\n' %(user_name,user_id))
				sys.stdout.flush()
				cursor.execute(u"update temp_users set realname=%s where user_id=%s",('<USER_NOT_FOUND>',user_id))
			else:
				sys.stdout.write('HTTP error for user %s (%s) \n' %(user_name,user_id))
				sys.stdout.flush()
		
		except MySQLdb.DataError:
			sys.stdout.write('Data error for user %s (%s) \n' %(user_name,user_id))
			sys.stdout.flush()

		except:
			sys.stdout.write('Some other error for user %s (%s) \n' %(user_name,user_id))
			sys.stdout.flush()

		if count%1000==0:
			db.commit()

db.commit()
db.close()
