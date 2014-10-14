api = open('api.key').read().strip()
import urllib2 as ul
from lxml import etree
import MySQLdb

parser=etree.XMLParser(encoding='utf-8',recover=True)
db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root",db="analysis_lastfm")

cursor=db.cursor()


def getTopTags(artist,track=None):
	artist = artist.replace('&','%26')
	result = []
	if track:
		url = 'http://ws.audioscrobbler.com/2.0/?method=track.gettoptags&artist=%s&track=%s&api_key=%s' % (artist,track,api)
	else:
		url = 'http://ws.audioscrobbler.com/2.0/?method=artist.gettoptags&artist=%s&api_key=%s' % (artist,api)
	tree = etree.fromstring(ul.urlopen(url).read(),parser=parser)
	for t in tree[0]:
		result.append(t.findtext('name'))
	return result

cursor.execute("select item_id,artist from lastfm_itemlist where item_type=0 and top_tag is null;")
artistDict = {i[1]:i[0] for i in cursor.fetchall()}
nArtists = float(len(artistDict))

err = open('topTagCrawler_errors','w')
for i,artist in enumerate(artistDict):
	print artist,artistDict[artist],str(i)+' / '+str(nArtists),i/nArtists
	try:
		topTags = getTopTags(artist)
		if len(topTags)>0:
			topTag = topTags[0]
		else:
			topTag = ''
		print '---> '+topTag
		cursor.execute("update lastfm_itemlist set top_tag=%s where item_id=%s", (topTag,artistDict[artist]))
	except KeyboardInterrupt:
		break
	except ul.HTTPError:
		print 'HTTP ERROR for %s' % artist
		err.write('HTTP\t'+str(artistDict[artist])+'\t'+artist+'\n')
	except:
		print 'other ERROR for %s' % artist
		err.write('other\t'+str(artistDict[artist])+'\t'+artist+'\n')
	if i%10000==0:
		db.commit()

db.commit()

err.close()
cursor.close()
db.close()