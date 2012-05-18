# -*- coding: utf-8 -*-

##
# this script serves to queue write requests to the database
# in order to reduce waiting time of the sender, and the
# pressure of database writing. later on balance loading
# should be included.
#
# @author Yuan JIN
# @contact jinyuan@baidu.com
# @created May 16, 2012
# @updated May 18, 2012
#

##
# imports and CONSTANTS
import sys
reload(sys)
sys.setdefaultencoding('UTF-8')

from gearman import GearmanWorker
GEARMAN_HOST = '127.0.0.1:4730'
gworker = GearmanWorker([GEARMAN_HOST])

from pymongo.connection import Connection
MONGO_CON = '127.0.0.1:27017'
mongo_con = Connection(MONGO_CON)
from pymongo.database import Database
MONGO_DB = 'note-keeper'
mongo_db = Database(mongo_con, MONGO_DB)
from pymongo.collection import Collection
MONGO_COL = 'text'
mongo_col = Collection(mongo_db, MONGO_COL)


def parse(bundle):
    '''find out information in the bundle'''
    data = bundle.split('||')
    result = {}
    if data:
        if data[0]:
            result['nid'] = data[0]
            result['content'] = data[1]
            return result
        else:
            return None
    return None

def task_add(gearman_worker, gearman_job):
    '''insert valid item to the database'''
    job_data = parse(gearman_job.data)
    mongo_col.save(job_data)
    return 'okay'

def task_edit(gearman_worker, gearman_job):
    '''update valid item with a new value'''
    # TODO add try-except
    job_data = parse(gearman_job.data)
    print str(job_data)
    mongo_col.update({'nid':job_data['nid']}, {'$set':{'content':job_data['content']}})
    return 'okay'

gworker.register_task('add', task_add)
gworker.register_task('edit', task_edit)

gworker.work()
