import os
import sys
from dbSetup import *

errorLogs = [fi for fi in os.listdir('.') if 'errorLog' in fi ]
if len(errorLogs) != 3:
	print 'SOMETHING WRONG'
	sys.exit()


latestEntries = [open(f).readlines()[-1].strip().split(',') for f in errorLogs]

cursor = db.cursor()

for entry in latestEntries:
	print entry
	user_id = entry[1]
	cursor.execute("select * from lastfm_scrobbles where user_id=%s order by scrobble_time asc limit 1;",user_id)
	lastGoodTS = cursor.fetchall()[0][2]
	print lastGoodTS
	cursor.execute("delete from errorqueue_updated where user_id=%s and error_type='scrobbles';",user_id)
	db.commit()
	cursor.execute("insert into errorqueue_updated (user_id,error_type,tag_name,retry_count) values (%s,'scrobbles',%s,0);",(user_id,lastGoodTS))
	db.commit()
