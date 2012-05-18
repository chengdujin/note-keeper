# -*- coding: utf-8 -*-

##
# License Area
#
# Inlcuding note-writer,
# Copyleft 2012-2013 Yuan JIN
# Released under the GPL Version 3 license
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
#

##
# this script is an agent to: 
# 1. provide interface for the front-ends
# 2. redirect read operations to the caching layer
# 3. redirect write operations to the message queue
#
# @authour Yuan JIN
# @contact jinyuan@baidu.com
# @created May 15, 2012
# @updated May 18, 2012
#
# TODO
# 1. redis persistence
# 2. redis sharding (consistent hashing)
# 3. mongdodb partition
# 4. message queue (nodes.js impl.)
# 5. 4-level storage refactor
# 6. restful api
# 7. fault tolerance
# 8. parameter check (decorator) 
# 9. documentation
#


##
# imports and CONSTANTS
import sys
reload(sys)
sys.setdefaultencoding('UTF-8')

import cgi
import base64
from time import strftime, gmtime
import json

import redis
REDIS_SERVER = '127.0.0.1'
rclient = redis.StrictRedis(REDIS_SERVER)

from pymongo.connection import Connection
MONGO_CON = '127.0.0.1:27017'
mongo_con = Connection(MONGO_CON)
MONGO_DB = 'note-keeper'
MONGO_COL = 'text'

from gearman import GearmanClient
GEARMAN_HOST = '127.0.0.1:4730'
gclient = GearmanClient([GEARMAN_HOST])


def format(result):
    '''wrap data in a client-readable way'''
    if isinstance(result, dict):
        return json.dumps(result)
    raise Exception("Error in formatting");

def read_mongo(parameters, nids_available):
    '''read from mongodb with keys'''
    from pymongo.database import Database
    mongo_db = Database(mongo_con, MONGO_DB)
    from pymongo.collection import Collection
    mongo_col = Collection(mongo_db, MONGO_COL) 
        
    # database find one or nothing
    def query(q_nid):
        # any is a dict or nothing
        any = mongo_col.find_one({'nid':q_nid})
        if any:
            if u'content' in any:
                return any['content']
            else:
                raise Exception('Error in misformat in a databse entry')
        else:
            raise Exception('Error of a missing database entry')

    result = {}
    result['list'] = dict((index, dict([('nid', q_nid), ('mod_time', rclient.hget(parameters['uid'], q_nid)), ('content', query(q_nid))]) ) for index, q_nid in enumerate(nids_available))
    return result

def generate_nid(uid):
    '''base64 -> uid + time stamp'''
    time_stamp = strftime('%Y%m%d%H%M%S', gmtime())
    return '%s%s' % (uid, time_stamp)

def check_parameters(reader):
    '''check the validity of parameters'''
    def checker(data):
        '''wrapper''' 
        unchecked = reader(data)
        if 'op' in unchecked:                                           
            op = unchecked['op']
            if op == 'add':
                if 'uid' in unchecked and unchecked['uid']:
                    if 'mod_time' in unchecked and unchecked['mod_time']:
                        if 'type' in unchecked and unchecked['type']:
                            if 'content' in unchecked:
                                return unchecked
                raise Exception('Error in ill-formatting')
            elif op == 'remove':     
                if 'uid' in unchecked and unchecked['uid']:
                    if 'nid' in unchecked and unchecked['nid']:
                        return unchecked
                raise Exception('Error in ill-formatting')
            elif op == 'edit':
                if 'uid' in unchecked and unchecked['uid']:
                    if 'nid' in unchecked and unchecked['nid']:
                        if 'mod_time' in unchecked and unchecked['mod_time']:
                            if 'type' in unchecked and unchecked['type']:
                                if 'content' in unchecked:
                                    return unchecked
                raise Exception('Error in ill-formatting')
            elif op == 'count':
                if 'uid' in unchecked and unchecked['uid']:
                    return unchecked
                raise Exception('Error in ill-formatting')
            elif op == 'list':
                if 'uid' in unchecked and unchecked['uid']:
                    if 'nid' in unchecked and unchecked['nid']:
                        if 'count' in unchecked and unchecked['count']:
                            return unchecked                            
                raise Exception('Error in ill-formatting')
            elif op == 'sync':
                if 'uid' in unchecked and unchecked['uid']:
                    if 'count' in unchecked and unchecked['count']:
                        return unchecked
                raise Exception('Error in ill-formatting')
        else:               
            raise Exception('Error in providing operation type')
    return checker 

def process(parameters):
    '''read and write operations'''
    result = {}
    # data should be a dict with
    # operation as the key, parameters as the value
    op = parameters['op']

    # cache: uid nid modified time
    # database: nid text
    if op == 'add':
        try:
            # cache
            nid = generate_nid(parameters['uid'])
            rclient.hset(parameters['uid'], nid, parameters['mod_time'])  
            # message queue
            bundle = "%s||%s" % (nid, parameters['content'])
            gclient.submit_job(task='add', data=bundle, background=True, wait_until_complete=False)

            result['value'] = nid
            return result
        except Exception, e:
            raise Exception(str(e))
    elif op == 'remove':
        # cache
        if rclient.hexists(parameters['uid'], parameters['nid']):
            try:
                rclient.hdel(parameters['uid'], parameters['nid'])

                result['value'] = '1'
                return result
            except Exception, e:
                raise Exception(str(e))
        raise Exception('Error in redis')
    elif op == 'edit':
        # cache - modified time
        if rclient.hexists(parameters['uid'], parameters['nid']):
            if int(parameters['mod_time']) > int(rclient.hget(parameters['uid'], parameters['nid'])):
                try:
                    rclient.hset(parameters['uid'], parameters['nid'], parameters['mod_time'])
                    # message queue
                    bundle = "%s||%s" % (parameters['nid'], parameters['content'])
                    gclient.submit_job(task='edit', data=bundle, background=True, wait_until_complete=False)

                    result['value'] = '1'
                    return result
                except Excetion:
                    raise Exception(str(e))
            raise Exception('Error in editing a newer item')
        raise Exception('Error in redis')
    elif op == 'count':
        # cache
        try:
            result['value'] = rclient.hlen(parameters['uid'])
            return result
        except Exception, e:
            raise Exception(str(e))
    elif op == 'list':
        try:
            # cache
            # 1. get a list of fields - nids
            # 2. filter necessary number of fields
            nids = rclient.hkeys(parameters['uid'])
            nids_available = []
            if nids:
                if parameters['nid'] in nids:
                    start = nids.index(parameters['nid'])
                    if (start + 1) > int(parameters['count']):
                        nids_available = nids[(start - int(parameters['count'])): start]
                    else:
                        nids_available = nids[:start]

                    # database
                    # another cache, probably in future
                    if nids_available:
                        return read_mongo(parameters, nids_available)
                    else:
                        raise Exception('Error in finding enough items')
                else:  # cannot find requested nid on the server
                    # find the nearest nid
                    import string
                    nids_simple = [int(string.lstrip(nid, parameters['uid'])) for nid in nids]
                    nids_smaller = filter(lambda x: x<int(parameters['nid']), nids_simple) 

                    # find nids available
                    if len(nids_smaller) > int(parameters['count']):
                        nids_available = ['%s%d' % ('test', nid) for nid in nids_smaller[-int(parameters['count']):]]
                    else:
                        nids_available = ['%s%d' % ('test', nid) for nid in nids_smaller]

                    # find in database
                    if nids_available:
                        return read_mongo(parameters, nids_available)
                    else:
                        raise Exception('Error in finding enough items')
            else:
                raise Exception('Error in getting a non-existing list')
        except Exception, e:
            raise Exception(str(e))
    elif op == 'sync':
        try:
            # find in cache available nids
            # read from database the content
            nids = rclient.hkeys(parameters['uid'])
            nids_available = []
            if nids:
                if int(parameters['count']) < len(nids):
                    nids_available = nids[-int(parameters['count']):]
                else:
                    nids_available = nids

                # database thing
                if nids_available:
                    return read_mongo(parameters, nids_available)
                else:
                    raise Exception('Error in finding enough items')
            else:
                raise Exception('Error in getting the user')
        except Exception, e:
            raise Exception(str(e))

    raise Exception('Unrecognizable operation')

@check_parameters
def read(environ):
    '''parse data sent via http request and parcel into recognizable data'''
    bundle = cgi.parse_qsl(environ['wsgi.input'].read(int(environ['CONTENT_LENGTH'])))
    if bundle:
        return dict(bundle)
    else:
        raise Exception("Error in request formatting")

def application(environ, start_response):
    try:
        data = read(environ)
        result = process(data)
        output = format(result)
        if not output:
            raise Exception("Error in outputing")
        
        header = [('Content-type', 'application/octet-stream'), ('Content-Length', str(len(output)))]
        start_response('200 OK', header)
        return [output]
    except Exception, e:
        header = [('Content-type', 'application/octet-stream'), ('Content-Length', str(len(str(e))))]
        start_response("404 Not Found", header)
        return [str(e)]
