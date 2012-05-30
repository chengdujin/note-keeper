import httplib, urllib
from time import strftime, gmtime

test = {}
test['op'] = 'sync'
test['uid'] = 'chengdujin'
test['count'] = 7
test['nid'] = "chengdujin20120518062033"
#test['mod_time'] = strftime('%Y%m%d%H%M%S', gmtime())
#test['type'] = 'text'
#test['content'] = test['mod_time']
body = urllib.urlencode(test) 
headers = {'content-type':'application/x-www-form-urlencoded', 'content-length':str(len(body)), "Accept":"text/plain"}
con = httplib.HTTPConnection('176.34.54.120')
con.request('post', '/cgi-bin/note-taker.cgi', body, headers)
response = con.getresponse()

print response.status, response.reason
print response.read()
con.close()

