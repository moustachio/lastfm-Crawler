"""
Functions for extracing data from HTML of user profile pages.
"""

import urllib2 as ul
import sys, traceback
import lxml.html
import time
from lxml import etree
from dbSetup import *
from dbMethods import *


# this is big and kinda messy, but it converts relative dates in users' annotations lists to absolute dates.
def fixTime(timestring):
	t = None
	monthDict = {'January':1,'February':2,'March':3,'April':4,'May':5,'June':6,'July':7,'August':8,'September':9,'October':10,'November':11,'December':12}
	weekdayDict = {'Sunday':0,'Monday':1,'Tuesday':2,'Wednesday':3,'Thursday':4,'Friday':5,'Saturday':6}
	timestring = timestring.split(' ')
	if timestring[0] in monthDict:
		t = timestring[1]+'-'+str(monthDict[timestring[0]])+'-01'
	elif timestring[1] == 'month':
		current = time.localtime()
		y = current.tm_year
		m = current.tm_mon - 1
		if m == 0:
			m = 12
			y = current.tm_year - 1
		t = '%i-%i-01'% (y,m)
	elif timestring[1] == 'week': # if "last week", assume it was last Wednesday (Thursday?)
		today = time.localtime().tm_wday
		tagDay = 3 
		adjust = today-tagDay
		current = time.localtime((time.time() - (86400*adjust)) - (86400*7))
		y = current.tm_year
		m = current.tm_mon
		d = current.tm_mday
		t = '%i-%i-%i'% (y,m,d)
	else:
		if timestring[0] in weekdayDict:
			ago = 0
			today = time.localtime().tm_wday
			tagDay = weekdayDict[timestring[0]]
			if today > tagDay:
				ago = today - tagDay
			if today < tagDay:
				ago = (7 - tagDay) + today
			current = time.localtime(time.time() - (86400 * ago))
		elif timestring[1] == 'now':
			current = time.localtime()
		elif timestring[0] == 'yesterday':
			current = time.localtime(time.time() - 86400)                   
		elif timestring[1] == 'minutes':
			ago = int(timestring[0])
			current = time.localtime(time.time()-(60*ago))
		elif timestring[1] == 'hours':
			ago = int(timestring[0])
			current = time.localtime(time.time()-(3600*ago))
		elif timestring[-1] == 'ago':
			if timestring[1] == 'days':
				current = time.localtime(time.time()-(86400 * int(timestring[0])))
		y = current.tm_year
		m = current.tm_mon
		d = current.tm_mday
		t = '%i-%i-%i'% (y,m,d)
	
	# return the converted timestring, and print out the raw string if we weren't able to calculate one for some reason
	if t:
		return t
	else:
		print timestring

#Given a taglist url, loads page, extracts all the tags listed for that page, and retrieves "lastpage" value (i.e. how many total pages of tags there are) if requested
def extractTagsFromPage(url,getLastPage=False): 
	page = ul.urlopen(url).read()
	tree = lxml.html.document_fromstring(page)
	tags = []
	if tree is not None:
		try:
			for i in tree.get_element_by_id('libraryList').find('tbody'):
				tag=i[0][0].get('href')
				tags.append(tag.split('?tag=')[1].split('&')[0])
			if getLastPage:
				if tree.find_class('lastpage'):
					lastpage = int(tree.find_class('lastpage')[0].text_content())
				else:
					lastpage = 1
				return (tags,lastpage)
			else:
				return (tags,1)
		# all KeyboardInterrup exceptions are raised, to propagate up and trigger cleaup functions
		except KeyboardInterrupt:
			raise
		except KeyError: # this means we only have one page
			return (tags,1)        

#Given an annotations list url, loads page extracts all annotations for that tag (item ID, item URL, and datestamp) present on the page, and retrieves "lastpage" value if requested
def extractAnnotations(url,uid,tag,getLastPage):
	errorType='annotations' #corresponds to error while extracting annotations
	
	try:
		page = ul.urlopen(url).read()         
		tree = lxml.html.document_fromstring(page)
		for itemType in ('artist','track','album'):
			for item in tree.find_class(itemType):
				itemURL = item.find_class('subjectCell')[0].find_class('primary')[-1].get('href')[7:] # gets the URL to the page for that item
				date = item.find_class('dateCell')[0].text_content().strip()
				date = fixTime(date)
				cursor = db.cursor()
				cursor.execute("INSERT INTO lastfm_annotations (user_id, item_url, tag_name, tag_date) VALUES (%s,%s,%s,%s)", (uid,itemURL.lower(),tag,date))
				closeDBConnection(cursor)                    
		if getLastPage:
			try:
				lastpage = int(tree.find_class('lastpage')[0].text_content())
			except:
				lastpage = 1
			return lastpage

	except KeyboardInterrupt:
		raise
	
	except:
		
		print traceback.format_exc()
		logger.error(traceback.format_exc())
		print 'annotation error for user ID ' + str(uid) + ' and tag "' + tag +'"'
		
		# If the database hangs, keep trying to insert the error unttil it works.
		errorRecorded = False
		while not errorRecorded:
			try:
				cursor = db.cursor()                        
				cursor.callproc("UpdateErrorQueue", (uid,errorType,tag,'@retryCount'))
				closeDBConnection(cursor)
				errorRecorded = True
			except:
				print traceback.format_exc()
				print 'Retrying...'
				continue

#Given a user ID, generates a list of all tags used by that user
def getUserTags(username,uid):
	errorType = 'tags' #Corresponds to error while getting user Tags
	try:
		# get data for first page of tags
		startUrl = 'http://www.last.fm/user/'+username+'/library/tags?view=list&sortOrder=asc&sortBy=name'
		tags,lastpage = extractTagsFromPage(startUrl,True)
		# if there are more pages, get those, too
		if lastpage > 1:
			for p in range(2,lastpage+1):
				url = 'http://www.last.fm/user/'+username+'/library/tags?view=list&sortOrder=asc&sortBy=name&page='+str(p)
				tags = tags + extractTagsFromPage(url,False)[0]
		return tags
	
	except KeyboardInterrupt:
		raise

	# a 404 error means we have a banned/non-existent user, so set retry count to "404" in the error queue 
	except ul.HTTPError as e:
		if str(e)=='HTTP Error 404: Not Found':
			errorRecorded = False
			while not errorRecorded:
				try:
					cursor = db.cursor()                        
					cursor.execute("insert into lastfm_errorqueue (user_id,error_type,retry_count) values (%s,%s,%s);", (uid,errorType,'404'))
					closeDBConnection(cursor)
					errorRecorded = True
				except KeyboardInterrupt:
					raise
				except:
					print traceback.format_exc()
					print 'Retrying...'
					continue
		
		# for other errors, write to error queue 
		else:
			errorRecorded = False
			while not errorRecorded:
				try:
					print traceback.format_exc()
					logger.error(traceback.format_exc())
					print (username,'tagListError')
					cursor = db.cursor()
					# example of inconsistency in error handling. This should eventually change                        
					cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
					closeDBConnection(cursor)  
					errorRecorded = True 
				except KeyboardInterrupt:
					raise
				except:
					print traceback.format_exc()
					print 'Retrying...'
					continue

	# Catch any other errors and write to errorqueue 				
	except:
		errorRecorded = False
		while not errorRecorded:
			try:
				print traceback.format_exc()
				logger.error(traceback.format_exc())
				print (username,'tagListError')
				cursor = db.cursor()                        
				cursor.callproc("UpdateErrorQueue", (uid,errorType,'','@retryCount'))
				closeDBConnection(cursor)  
				errorRecorded = True 
			except KeyboardInterrupt:
				raise
			except:
				print traceback.format_exc()
				print 'Retrying...'
				continue

#Given a list of tags and a user ID, extracts all annotations for each tag 
def getUserAnnotations(tags,username,uid):
	errorType='annotations'
	for t in tags:
		startUrl = 'http://www.last.fm/user/'+username+'/library/tags?tag='+t+'&view=list'
		lastpage = extractAnnotations(startUrl,uid,t,True)
		if lastpage > 1:
			for p in range(2,lastpage+1):
				url = 'http://www.last.fm/user/'+username+'/library/tags?tag='+t+'&view=list&page='+str(p)
				extractAnnotations(url,uid,t,False)

#Record listing of a user's groups
def getUserGroups(username,uid):
	errorType='groups'
	try:
		url = 'http://www.last.fm/user/%s/groups' % username
		page = ul.urlopen(url).read()  
		tree = lxml.html.document_fromstring(page)
		
		pages = tree.find_class('lastpage')
		if pages:
			nPages = int(pages[0].text_content())
		else: 
			nPages = 1
		
		for pageNumber in range(1,nPages+1):
			if pageNumber > 1:
				url = 'http://www.last.fm/user/%s/groups?groupspage=%s' % (username,str(pageNumber))
				page = ul.urlopen(url).read()  
				tree = lxml.html.document_fromstring(page)

			groups = tree.find_class('groupContainer')
			cursor = db.cursor()
			for group in groups:
				groupName = group.cssselect('a')[0].get('href').split('/')[2]
				try: # need to ignore the groups with the stupid heart character... (which seem to be defunct anyway)
					cursor.execute("INSERT IGNORE INTO lastfm_groups (user_id, group_name) VALUES (%s,%s);", (uid,groupName.lower()))
				except UnicodeEncodeError:
					continue
		closeDBConnection(cursor)
	
	# a 404 error means we have a banned/non-existent user, so set retry count to "404" in the error queue 
	except ul.HTTPError as e:
		if str(e)=='HTTP Error 404: Not Found':
			errorRecorded = False
			while not errorRecorded:
				try:
					cursor = db.cursor()                        
					cursor.execute("insert into lastfm_errorqueue (user_id,error_type,retry_count) values (%s,%s,%s);", (uid,errorType,'404'))
					closeDBConnection(cursor)
					errorRecorded = True
				except KeyboardInterrupt:
					raise
				except:
					print traceback.format_exc()
					print 'Retrying...'
					continue

	except KeyboardInterrupt:
		raise
		#closeDBConnection(cursor)
		#cursor=db.cursor()
		#cursor.execute("update lastfm_crawlqueue set groups=0 where user_name=%s",(username))
		#cursor.execute("delete from lastfm_groups where user_id=%s;", (uid))
		#closeDBConnection(cursor)
		#print 'cleanup complete'
		#sys.exit()

	except:
		print traceback.format_exc()
		cursor = db.cursor()
		cursor.execute("insert into lastfm_errorqueue (user_id,error_type,retry_count) values (%s,%s,%s)",(uid,errorType,0))
		closeDBConnection(cursor)

