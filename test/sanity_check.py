#!/usr/bin/env python

"""
A simple sanity checker to verify whether the aggregation 
of the usage records into the several aggreate tables is
done properly. 
This 'script' might be used to quickly validate changes 
of the code.
"""
"""
from datetime import datetime

from sqlalchemy import or_ as OR

from sgas.db import init_model
from sgas.db import sgas_meta as meta
from sgas.db import sgas_schema as schema
from sgas.db import ag_schema as ag_schema


from sgasaggregator.sgascache import ag_schema

"""

import time, sys, calendar
from datetime import datetime
from sqlalchemy import engine_from_config
from sqlalchemy import and_ as AND

from sgasaggregator import dbinit, uraggregator
from sgasaggregator.sgas import sgas_schema
from sgasaggregator.sgascache import ag_schema
from sgasaggregator.sgas import session as sgas_session
from sgasaggregator.sgascache import session as sgascache_session
from sgasaggregator.utils import helpers, init_config, config_parser
    
def _get_vo_name(vo_type, vo_string):
    """ input:  id of original SGAS reccord
        output: name of VO, or ''
    """

    if not vo_string: # no VO info in SGAS record
        return ''
    
    if vo_type == 'voms':
        return vo_string

    if '.' in vo_string: 
        return vo_string.split('.')[0]
    else:
        return ''

def test_raw_inserted(t_start_epoch, t_end_epoch):
    """ count the numbers of records that got *inserted*
        during the specified time.
        Notice, including t_start_epoch, but excluding t_end_epoch records 
    """

    t_start_utc = datetime.utcfromtimestamp(t_start_epoch)
    t_end_utc = datetime.utcfromtimestamp(t_end_epoch)
            
    n = sgas_session.Session.query(sgas_schema.UsageRecords).filter(AND(
                        sgas_schema.UsageRecords.end_time >= t_start_utc,
                        sgas_schema.UsageRecords.end_time < t_end_utc)).count()

    return n


def test_raw2key0_aggregate(t_start_epoch, resolution):
    """ creates the very first aggregate, which will be 
        used afterwards to create the subsequent onces. 
        The input is the SGAS usagedata table. 

        start_t_epoch:  starting time in epoch for database
                        records, which will be read
        resolution: resolution of the aggregate.
    """
    t = t_start_epoch
    now_epoch = int(time.time())

    while t < now_epoch:
        t_start_utc = datetime.utcfromtimestamp(t)
        t += resolution
        t_end_utc = datetime.utcfromtimestamp(t-1)

        print "Raw records aggregation from %s --  %s" % (t_start_utc, t_end_utc)

        org_n_jobs_check_sum = 0
        org_wall_time_check_sum = 0
        ag_recs = dict()
            
        for rec in sgas_session.Session.query(sgas_schema.UsageRecords).filter(AND(
                        sgas_schema.UsageRecords.end_time >= t_start_utc,
                        sgas_schema.UsageRecords.end_time <= t_end_utc)).all():
        
            org_n_jobs_check_sum += 1
            
            if rec.wall_duration:
                org_wall_time_check_sum += rec.wall_duration

            gun = rec.global_user_name
            von = _get_vo_name(rec.vo_type, rec.vo_name)
            mn  = rec.machine_name
            status = rec.status

            key = "%s-%s-%s-%s-%f" % (gun, von, mn, status, t-1)

            if not ag_recs.has_key(key):  # t -1 is the real end time of aggregate
                aggr = ag_schema.UserVoMachineStatus(gun, von, mn, status, resolution, t-1)
                for k, v in uraggregator.AGGREGATE_VALUES.items():
                    exec('aggr.%s = %d' % (k, v))
                ag_recs[key] = aggr
            else:
                aggr = ag_recs[key]

            for k in uraggregator.AGGREGATE_VALUES.keys(): # aggregation of values
                if k == 'n_jobs':
                    aggr.n_jobs += 1
                else:
                    val = eval('rec.%s' % k)
                    if not val:
                        val = 0
                    exec('aggr.%s +=  val' % (k))
        
        ag_n_jobs_check_sum = 0
        ag_wall_time_check_sum = 0
        for rec in ag_recs.values():  # writing to database
            ag_n_jobs_check_sum += rec.n_jobs
            ag_wall_time_check_sum += rec.wall_duration

        print 'got # of keys:', len(ag_recs.keys())
        print 10 * '=', "CHECKSUMS" , 10 * '='
        print 'original data:   %d jobs, %0.2f wall_time' % (org_n_jobs_check_sum, org_wall_time_check_sum)
        print 'aggregated data: %d jobs, %0.2f wall_time' % (ag_n_jobs_check_sum, ag_wall_time_check_sum)


def test_key_aggregation(key_in, key_out, t_start_epoch, resolution):
    """
    Aggregation from parent aggregate (key_in) to child (key_out). 
    Warning: t_start_epoch time MUST match sampling resolution. We don't
             fix the time in this method, if it's shifted.
        key_in: input key, will be mapped to corresponding db
        key_out: output key, will be mapped to corresponding db
        start_t_epoch: starting time from epoch
        resolution:  time steps/resolution of aggregates. must be an integer > 1
    """
    db_obj_in = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_in])
    db_obj_out = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_out])

    db_resolution_in = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_in] + '.resolution')
    db_resolution_out = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_out] + '.resolution')
    db_t_epoch_in = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_in] + '.t_epoch')
    db_t_epoch_out = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_out] + '.t_epoch')
    
    print("Aggregation of %s to %s" % 
            (uraggregator.KEY2ORM_MAP[key_in], uraggregator.KEY2ORM_MAP[key_out]))
        
    ag_recs = dict()
    org_n_jobs_check_sum = 0
    org_wall_time_check_sum = 0

    ag_recs = dict()
    for rec in sgascache_session.Session.query(db_obj_in).filter( AND (
                db_t_epoch_in >= t_start_epoch,
                db_resolution_in == resolution)).all(): # no chunking as in raw2key0_aggregate(...)

        org_n_jobs_check_sum += rec.n_jobs
        if rec.wall_duration:
            org_wall_time_check_sum += rec.wall_duration

        key = '%s' % rec.t_epoch
        kwargs = dict(res=resolution, t_epoch = rec.t_epoch)
        for k in uraggregator.KEY_MAPS[key_out]:  # key out is subset of key in
            kwargs[k] = eval('rec.' + k)
            key  +=  str(kwargs[k])

        if not ag_recs.has_key(key):
            aggr = eval('ag_schema.' + uraggregator.KEY2ORM_MAP[key_out] + '(**kwargs)')
            aggr.resolution = resolution
            aggr.t_epoch = rec.t_epoch
            for k, v in uraggregator.AGGREGATE_VALUES.items(): # init/reset
                exec('aggr.%s = %d' % (k, v))
            ag_recs[key] = aggr
        else:
            aggr = ag_recs[key]

        for k in uraggregator.AGGREGATE_VALUES.keys(): # aggregation of values
            val = eval('rec.%s' % k)
            if not val:
                val = 0
            exec('aggr.%s +=  val' % (k))

        
    ag_n_jobs_check_sum = 0
    ag_wall_time_check_sum = 0
    for rec in ag_recs.values():  # writing to database
        ag_n_jobs_check_sum += rec.n_jobs
        ag_wall_time_check_sum += rec.wall_duration

    print 'got # of keys:', len(ag_recs.keys())
    print 10 * '=', "CHECKSUMS" , 10 * '='
    print 'original data:   %d jobs, %0.2f wall_time' % (org_n_jobs_check_sum, org_wall_time_check_sum)
    print 'aggregated data: %d jobs, %0.2f wall_time' % (ag_n_jobs_check_sum, ag_wall_time_check_sum)



def test_res_aggregation(key_in, t_start_epoch, from_resolution, factor):
    """ Aggregates existing aggregate by specified factor.

        Warning: t_start_epoch time MUST match sampling resolution. We don't
             fix the time in this method, if it's shifted.
        
        key_in: input key, will be mapped to corresponding db
        t_start_epoch: starting time (epoch)
        from_resolution: init resolution
        factor: factor of output  resolution. Must be > 1
    """

    db_obj = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_in])

    resolution = factor * from_resolution

    db_t_epoch = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_in] + '.t_epoch')
    db_resolution = eval( 'ag_schema.' + uraggregator.KEY2ORM_MAP[key_in] + '.resolution')

    print "Aggregation of %s from resolution %d by factor %d" % \
        (uraggregator.KEY2ORM_MAP[key_in], from_resolution, factor)

    org_n_jobs_check_sum = 0
    org_wall_time_check_sum = 0
    ag_recs = dict()

    for rec in sgascache_session.Session.query(db_obj).filter( AND (
                db_t_epoch >= t_start_epoch,
                db_resolution ==  from_resolution)).all(): # no chunking as in raw2key_aggregate(...)
        org_n_jobs_check_sum += rec.n_jobs
        if rec.wall_duration:
            org_wall_time_check_sum += rec.wall_duration

        ag_t_end_epoch = t_start_epoch + int(rec.t_epoch - t_start_epoch) / \
            resolution  * resolution + resolution
        
        key = '%d' % ag_t_end_epoch
        for k in uraggregator.KEY_MAPS[key_in]:
            key  = key + str(eval('rec.' +k))

        if not ag_recs.has_key(key):
            aggr = rec.clone()
            aggr.resolution = resolution
            aggr.t_epoch = ag_t_end_epoch - 1   
            for k, v in uraggregator.AGGREGATE_VALUES.items(): # init/reset
                exec('aggr.%s = %d' % (k, v))
            ag_recs[key] = aggr
        else:
            aggr = ag_recs[key]

        for k in uraggregator.AGGREGATE_VALUES.keys(): # aggregation of values
            val = eval('rec.%s' % k)
            if not val:
                val = 0
            exec('aggr.%s +=  val' % (k))

    if ag_recs:  # writing to database
        ag_n_jobs_check_sum = 0
        ag_wall_time_check_sum = 0
        for rec in ag_recs.values():  # writing to database
            ag_n_jobs_check_sum += rec.n_jobs
            ag_wall_time_check_sum += rec.wall_duration

        print 'got # of keys:', len(ag_recs.keys())
        print 10 * '=', "CHECKSUMS" , 10 * '='
        print 'original data:   %d jobs, %0.2f wall_time' % (org_n_jobs_check_sum, org_wall_time_check_sum)
        print 'aggregated data: %d jobs, %0.2f wall_time' % (ag_n_jobs_check_sum, ag_wall_time_check_sum)


def test_fech_oldest_aggregate(last_aggregation_time_epoch):
    """ fetching aggregated that was inserted since last_aggregation_time
        and which holds the oldest job (ie. job with smallest insert_time)
        returns oldest_job_end_time
    """
    last_aggregation_time = datetime.utcfromtimestamp(last_aggregation_time_epoch)
    print "\n (Assumed) last aggregation time:", last_aggregation_time
    
    recs  = sgas_session.Session.query(sgas_schema.UsageRecords).order_by(sgas_schema.UsageRecords.end_time.asc()).\
        filter(sgas_schema.UsageRecords.insert_time >= last_aggregation_time)[0:2]
    
    if recs:
        end_time_tuple = recs[0].end_time.timetuple()
        start_t_epoch_ = calendar.timegm(end_time_tuple)
        start_t_epoch =  start_t_epoch_ - (start_t_epoch_ % 86400)
        print "Aggregating accouting records starting from %s" %  datetime.utcfromtimestamp(start_t_epoch)
        for rec in recs:
            print 'Oldest job finished at: %s' % rec.end_time
    else:
        print 'no new oldest job'

        
            


if __name__ == '__main__':
    
    init_config('/opt/smscg/sgas/etc/config.ini')
    try:
        sgas_engine = engine_from_config(config_parser.config.get(),'sqlalchemy_sgas.')
        dbinit.init_model(sgas_session, sgas_engine)
        print "Session object to SGAS database created"
    except Exception, ex:
        print "Failed to create session to SGAS database: %r" %  ex

    try:
        sgascache_engine = engine_from_config(config_parser.config.get(),'sqlalchemy_sgascache.')
        dbinit.init_model(sgascache_session, sgascache_engine)
        print "Session object to SGAS cache database created"
    except Exception, ex:
        print "Failed to create session to SGAS cache database: %r" %  ex
    
    now_epoch = int(time.time())
    resolution= 86400
    secs_back = 86400 * 1 

    start_t_epoch = now_epoch - (now_epoch % resolution) - secs_back
    end_t_epoch = now_epoch - (now_epoch % resolution) + resolution
    
    start_t_utc = datetime.utcfromtimestamp(start_t_epoch)
    end_t_utc = datetime.utcfromtimestamp(end_t_epoch)

    print "T1: Statistics on original data:" 
    print "Raw accouting records inserted from %s -- %s" % (start_t_utc, end_t_utc)
    print "Sum:", test_raw_inserted(start_t_epoch, end_t_epoch)

 
    print "\nT2: Generation of first aggregate from original data:" 
    test_raw2key0_aggregate(start_t_epoch, resolution) 
    print '\n\nT3 ... generation of aggregates from first aggregate...: '
    test_key_aggregation('key_0', 'key_01', start_t_epoch, resolution)
    print '\n'
    test_key_aggregation('key_0', 'key_02', start_t_epoch, resolution)
    print '\n'
    test_key_aggregation('key_02', 'key_021', start_t_epoch, resolution)
    print '\n'
    test_key_aggregation('key_021', 'key_0211', start_t_epoch, resolution)
    print '\n'
    test_res_aggregation('key_0', start_t_epoch,  resolution, 2)
    print '\n'
    test_res_aggregation('key_0', start_t_epoch + resolution, resolution, 7)
    print '\n'
    

    print "\nT4: Fetching oldest records of last 7 days:"
    t = now_epoch - 7 * 86400 
    while t < now_epoch: 
        test_fech_oldest_aggregate(t)
        t += resolution



    DN="/DC=com/DC=quovadisglobal/DC=grid/DC=switch/DC=users/C=CH/O=SWITCH/CN=Placi Flury"
    print "\nT5: Fetching records of a user:"
    print 'user:', DN
    print 'Query time intervall', time.ctime(start_t_epoch), '---', time.ctime(end_t_epoch + 86400)

    org_n_jobs_check_sum = 0
    org_wall_time_check_sum = 0
    
    ac_n_jobs_check_sum = 0
    ac_wall_time_check_sum = 0
   

    today_epoch = now_epoch - (now_epoch % resolution)
    start_t_epoch = today_epoch  - 7 * 86400
    end_t_epoch =  today_epoch
    

    start_t_utc = datetime.utcfromtimestamp(start_t_epoch)
    end_t_utc = datetime.utcfromtimestamp(end_t_epoch)
    print "original records" 
    for arec in sgas_session.Session.query(sgas_schema.UsageRecords).filter(AND(
                        sgas_schema.UsageRecords.end_time >= start_t_utc,
                        sgas_schema.UsageRecords.end_time <= end_t_utc, 
                        sgas_schema.UsageRecords.global_user_name == DN)).all():
        org_n_jobs_check_sum +=1
        if arec.wall_duration: 
            org_wall_time_check_sum += arec.wall_duration
        print arec.start_time, arec.end_time, arec.wall_duration
    
    print 'and corresponding aggregates'
    for arec in helpers.get_user_acrecords(DN, start_t_epoch, end_t_epoch, 86400):
        print datetime.utcfromtimestamp(arec.t_epoch),  arec.n_jobs, arec.wall_duration        
        ac_n_jobs_check_sum += arec.n_jobs
        if arec.wall_duration:
            ac_wall_time_check_sum += arec.wall_duration
    
    print 10 * '=', "CHECKSUMS" , 10 * '='
    print 'original data:   %d jobs, %0.2f wall_time' % (org_n_jobs_check_sum, org_wall_time_check_sum)
    print 'aggregated data: %d jobs, %0.2f wall_time' % (ac_n_jobs_check_sum, ac_wall_time_check_sum)

