#!/usr/bin/env python
"""
Module for aggregation of SGAS Usage records
"""
# last modification: bug-fix 12.12.11 PF

import time, logging, calendar
from datetime import datetime

from sqlalchemy import and_ as AND

from sgasaggregator.sgas import sgas_schema
from sgasaggregator.sgas import session as sgas_session
from sgasaggregator.sgascache import ag_schema
from sgasaggregator.sgascache import session as sgascache_session



# mapping from key prefixes to database primary keys
#(resolution and t_epoch are added later)
KEY_MAPS = {
    'raw'      : [],
    'key_0'    : [ 'global_user_name', 'vo_name', 'machine_name', 'status'],
    'key_01'   : [ 'global_user_name', 'vo_name', 'machine_name'],
    'key_02'   : [ 'global_user_name', 'vo_name',  'status'],
    'key_03'   : [ 'vo_name', 'machine_name', 'status'],
    'key_04'   : [ 'global_user_name', 'machine_name', 'status'],
    'key_011'  : [ 'global_user_name', 'vo_name'],
    'key_012'  : [ 'global_user_name', 'machine_name'],
    'key_021'  : [ 'global_user_name', 'status'],
    'key_031'  : [ 'vo_name', 'status'],
    'key_032'  : [ 'vo_name', 'machine_name'],
    'key_041'  : [ 'machine_name', 'status'],
    'key_0211' : [ 'global_user_name'],
    'key_0311' : [ 'status'],
    'key_0321' : [ 'vo_name'],
    'key_0411' : [ 'machine_name']
}

# mapping from key to database object names
KEY2ORM_MAP = {
    'key_0'    : 'UserVoMachineStatus',
    'key_01'   : 'UserVoMachine',
    'key_02'   : 'UserVoStatus',
    'key_03'   : 'VoMachineStatus',
    'key_04'   : 'UserMachineStatus',
    'key_011'  : 'UserVo',
    'key_012'  : 'UserMachine',
    'key_021'  : 'UserStatus',
    'key_031'  : 'VoStatus',
    'key_032'  : 'VoMachine',
    'key_041'  : 'MachineStatus',
    'key_0211' : 'User',
    'key_0311' : 'Status',
    'key_0321' : 'Vo',
    'key_0411' : 'Machine'
}

# the values we aggregate within key_0 - key_0411 databases
AGGREGATE_VALUES = {
    'n_jobs'            : 0,
    'cpu_duration'      : 0,
    'wall_duration'     : 0,
    'user_time'         : 0,
    'kernel_time'       : 0,
    'major_page_faults' : 0
}


class UrAggregator(object):

    """
    Aggregates SGAS Usage Records to 'time-series' samples. Aggregation is
    based on 'keys', i.e. the items specified by the key define the level of
    aggregation. E.g. key_0 samples records at user, vo, machine and status
    level, where key_01, is not distiguishing anymore on the 'status' of the
    records.
    """

    def __init__(self, refresh_days_back):
        self.log = logging.getLogger( __name__)

        now_epoch = int(time.time())
        secs_back = 86400 * refresh_days_back

        self.last_aggregation_time_epoch = now_epoch - (now_epoch % 86400) - secs_back

        self.refresh_days_back = refresh_days_back

    
    def _get_vo_name(self, vo_type, vo_string):
        """ input:  id of original SGAS reccord
            output: name of VO, or ''
        """

        if not vo_string: # no VO info in SGAS record
            return ''
        
        if vo_type in ['voms','grid-vo-map/vomss']:
            return vo_string

        if '.' in vo_string: 
            return vo_string.split('.')[0]
        else:
            return ''


            

    def raw2key0_aggregate(self, t_start_epoch, resolution):
        """
        creates the very first aggregate, which will be used afterwards to
        create the subsequent onces. The input is the SGAS usagedata table.
        Warning: t_start_epoch time MUST match sampling resolution. We don't
                 fix the time in this method, if it's shifted.

        start_t_epoch : starting time in epoch for database records, which will be read
        resolution    : resolution of the aggregate.
        """

        # just to make sure (in case of time shifts)
        n = sgascache_session.Session.query(ag_schema.UserVoMachineStatus).filter(AND(
            ag_schema.UserVoMachineStatus.t_epoch >= t_start_epoch,
            ag_schema.UserVoMachineStatus.resolution == resolution)) \
                .delete(synchronize_session='fetch')

        self.log.info('Removed %d old records from UserVomachineStatus before repopulation.' % n)
            
        t = t_start_epoch
        now_epoch = int(time.time())

        while t < now_epoch:
            t_start_utc = datetime.utcfromtimestamp(t)
            t += resolution
            t_end_utc = datetime.utcfromtimestamp(t-1)
            n_raw=0

            self.log.debug("Raw records aggregation from %s --  %s" % (t_start_utc, t_end_utc))

            ag_recs = dict()
            # XXX maybe we still need to prevent to read too many records at once
            # (e.g. with a loop over some LIMIT query)
            for rec in sgas_session.Session.query(sgas_schema.UsageRecords).filter(AND(
                        sgas_schema.UsageRecords.end_time >= t_start_utc,
                        sgas_schema.UsageRecords.end_time <= t_end_utc)).all():

                n_raw += 1 
                gun = rec.global_user_name
                von = self._get_vo_name(rec.vo_type, rec.vo_name)
                if not von:
                    von = ''
                mn  = rec.machine_name
                status = rec.status

                key = "%s-%s-%s-%s-%f" % (gun, von, mn, status, t-1)

                if not ag_recs.has_key(key):  # t -1 is the real end time of aggregate
                    aggr = ag_schema.UserVoMachineStatus(gun, von, mn, status, resolution, t-1)
                    for k, v in AGGREGATE_VALUES.items():
                        exec('aggr.%s = %d' % (k, v))
                    ag_recs[key] = aggr
                else:
                    aggr = ag_recs[key]

                for k in AGGREGATE_VALUES.keys(): # aggregation of values
                    if k == 'n_jobs':
                        aggr.n_jobs += 1
                    else:
                        val = eval('rec.%s' % k)
                        if not val:
                            val = 0
                        exec('aggr.%s +=  val' % (k))

            
            self.log.debug("Got %d raw records" % n_raw)
            self.log.debug("Got %d new aggregates (resolution = %d)" % \
                    (len(ag_recs.keys()), resolution))

            if ag_recs:  # writing to database
                session  = sgascache_session.Session()
                for nrec in ag_recs.values():
                    session.add(nrec)
                session.commit()

            self.log.debug("Commited them successfully to UserVoMachineStatus db")


    def res_aggregation(self, key_in, t_start_epoch, from_resolution, factor):
        """ Aggregates existing aggregate by specified factor.

            Warning: t_start_epoch time MUST match sampling resolution. We don't
                 fix the time in this method, if it's shifted.
            
            key_in: input key, will be mapped to corresponding db
            t_start_epoch: starting time (epoch)
            from_resolution: init resolution
            factor: factor of output  resolution. Must be > 1
        """

        db_obj = eval( 'ag_schema.' + KEY2ORM_MAP[key_in])

        resolution = factor * from_resolution

        db_t_epoch = eval( 'ag_schema.' + KEY2ORM_MAP[key_in] + '.t_epoch')
        db_resolution = eval( 'ag_schema.' + KEY2ORM_MAP[key_in] + '.resolution')

        self.log.debug("Aggregation of %s from resolution %d by factor %d" %
                (KEY2ORM_MAP[key_in], from_resolution, factor))

        self.log.debug("Removing existing records from %s  aggregate from UTC time  %s on." % \
                (KEY2ORM_MAP[key_in], datetime.utcfromtimestamp(t_start_epoch)))

        # remove exiting records from new resolution
        # which are younger then start_
        n = sgascache_session.Session.query(db_obj).filter(AND(
            db_t_epoch >= t_start_epoch,
            db_resolution == resolution)).delete(synchronize_session='fetch')

        self.log.info( 'Removed %d records from %s db before repopulation.' % \
            (n, KEY2ORM_MAP[key_in]))

        ag_recs = dict()

        for rec in sgascache_session.Session.query(db_obj).filter( AND (
                    db_t_epoch >= t_start_epoch,
                    db_resolution ==  from_resolution)).all(): # no chunking as in raw2key_aggregate(...)

            ag_t_end_epoch = t_start_epoch + int(rec.t_epoch - t_start_epoch) / \
                resolution  * resolution + resolution

            key = '%d' % ag_t_end_epoch
            for k in KEY_MAPS[key_in]:
                key  = key + str(eval('rec.' +k))

            if not ag_recs.has_key(key):
                aggr = rec.clone()
                aggr.resolution = resolution
                aggr.t_epoch = ag_t_end_epoch - 1   
                for k, v in AGGREGATE_VALUES.items(): # init/reset
                    exec('aggr.%s = %d' % (k, v))
                ag_recs[key] = aggr
            else:
                aggr = ag_recs[key]

            for k in AGGREGATE_VALUES.keys(): # aggregation of values
                val = eval('rec.%s' % k)
                if not val:
                    val = 0
                exec('aggr.%s +=  val' % (k))

        if ag_recs:  # writing to database
            session  = sgascache_session.Session()
            for nrec in ag_recs.values():
                session.add(nrec)
            session.commit()
            self.log.info( 'Commited  %d records to %s (resolution: %d).' % \
             (len(ag_recs.keys()), KEY2ORM_MAP[key_in], resolution))


    def key_aggregation(self, key_in, key_out, t_start_epoch, resolution):
        """
        Aggregation from parent aggregate (key_in) to child (key_out). 
        Warning: t_start_epoch time MUST match sampling resolution. We don't
                 fix the time in this method, if it's shifted.
            key_in: input key, will be mapped to corresponding db
            key_out: output key, will be mapped to corresponding db
            start_t_epoch: starting time from epoch
            resolution:  time steps/resolution of aggregates. must be an integer > 1
        """
        db_obj_in = eval( 'ag_schema.' + KEY2ORM_MAP[key_in])
        db_obj_out = eval( 'ag_schema.' + KEY2ORM_MAP[key_out])

        db_resolution_in = eval( 'ag_schema.' + KEY2ORM_MAP[key_in] + '.resolution')
        db_resolution_out = eval( 'ag_schema.' + KEY2ORM_MAP[key_out] + '.resolution')
        db_t_epoch_in = eval( 'ag_schema.' + KEY2ORM_MAP[key_in] + '.t_epoch')
        db_t_epoch_out = eval( 'ag_schema.' + KEY2ORM_MAP[key_out] + '.t_epoch')

        self.log.debug("Aggregation of %s to %s" % (KEY2ORM_MAP[key_in], KEY2ORM_MAP[key_out]))
        self.log.debug("Removing existing records from %s  aggregate from UTC time  %s on." %
                (KEY2ORM_MAP[key_out], datetime.utcfromtimestamp(t_start_epoch)))

        # remove exiting records from out-db (only such from start_t_epoch on
        n = sgascache_session.Session.query(db_obj_out).filter(AND(
            db_t_epoch_out >= t_start_epoch,
            db_resolution_out == resolution)).delete(synchronize_session='fetch')


        ag_recs = dict()
        for rec in sgascache_session.Session.query(db_obj_in).filter( AND (
                    db_t_epoch_in >= t_start_epoch,
                    db_resolution_in == resolution)).all(): # no chunking as in raw2key0_aggregate(...)


            key = '%s' % rec.t_epoch
            kwargs = dict(res=resolution, t_epoch = rec.t_epoch)
            for k in KEY_MAPS[key_out]:  # key out is subset of key in
                kwargs[k] = eval('rec.' + k)
                key  +=  str(kwargs[k])

            if not ag_recs.has_key(key):
                aggr = eval('ag_schema.' + KEY2ORM_MAP[key_out] + '(**kwargs)')
                aggr.resolution = resolution
                aggr.t_epoch = rec.t_epoch
                for k, v in AGGREGATE_VALUES.items(): # init/reset
                    exec('aggr.%s = %d' % (k, v))
                ag_recs[key] = aggr
            else:
                aggr = ag_recs[key]

            for k in AGGREGATE_VALUES.keys(): # aggregation of values
                val = eval('rec.%s' % k)
                if not val:
                    val = 0
                exec('aggr.%s +=  val' % (k))

        if ag_recs:  # writing to database
            session  = sgascache_session.Session()
            for nrec in ag_recs.values():
                session.add(nrec)
            session.commit()
            self.log.info( 'Commited  %d records to %s (resolution: %d).' % \
             (len(ag_recs.keys()), KEY2ORM_MAP[key_out], resolution))


    def main(self, resolution, factors):

        # 1.) fetch records inserted since the last aggregation time
        #     and get record with oldest end_time
        
        last_aggregation_time = datetime.utcfromtimestamp(self.last_aggregation_time_epoch)
        
        self.last_aggregation_time_epoch = int(time.time()) - 300 # now -300 secs (robustness toNTP time drifts)
        
        self.log.debug("last aggreation time:%s" % last_aggregation_time)


        rec = sgas_session.Session.query(sgas_schema.UsageRecords).order_by(sgas_schema.UsageRecords.end_time.asc()).\
            filter(sgas_schema.UsageRecords.insert_time >= last_aggregation_time).first()

        if not rec:
            self.log.debug("No new accouting records to aggregate")

        else:
            self.log.debug("Oldest non-aggregated record's end_time %s" % rec.end_time)

            #2.) aggregation of records starting from oldest end_time on
            end_time_tuple = rec.end_time.timetuple()
            start_t_epoch_ = calendar.timegm(end_time_tuple)
        
            start_t_epoch =  start_t_epoch_ - (start_t_epoch_ % resolution)
            
            self.log.info("Aggregating accouting records starting from %s" % \
                datetime.utcfromtimestamp(start_t_epoch))

            self.raw2key0_aggregate(start_t_epoch, resolution)
            self.key_aggregation('key_0', 'key_01', start_t_epoch, resolution)
            self.key_aggregation('key_0', 'key_02', start_t_epoch, resolution)
            self.key_aggregation('key_0', 'key_03', start_t_epoch, resolution)
            self.key_aggregation('key_0', 'key_04', start_t_epoch, resolution)
            self.key_aggregation('key_01', 'key_011', start_t_epoch, resolution)
            self.key_aggregation('key_01', 'key_012', start_t_epoch, resolution)
            self.key_aggregation('key_02', 'key_021', start_t_epoch, resolution)
            self.key_aggregation('key_03', 'key_031', start_t_epoch, resolution)
            self.key_aggregation('key_03', 'key_032', start_t_epoch, resolution)
            self.key_aggregation('key_04', 'key_041', start_t_epoch, resolution)
            self.key_aggregation('key_021', 'key_0211', start_t_epoch, resolution)
            self.key_aggregation('key_031', 'key_0311', start_t_epoch, resolution)
            self.key_aggregation('key_032', 'key_0321', start_t_epoch, resolution)
            self.key_aggregation('key_041', 'key_0411', start_t_epoch, resolution)
            for factor in factors:
                # callibrate  start_t_epoch time for each resolution, else time may 
                # drift with each run (and start of daemon)
                start_t_epoch = start_t_epoch_ - (start_t_epoch_ % (factor * resolution))
                self.res_aggregation('key_0', start_t_epoch, resolution, factor)
                self.res_aggregation('key_01', start_t_epoch, resolution, factor)
                self.res_aggregation('key_02', start_t_epoch, resolution, factor)
                self.res_aggregation('key_03', start_t_epoch, resolution, factor)
                self.res_aggregation('key_04', start_t_epoch, resolution, factor)
                self.res_aggregation('key_011', start_t_epoch, resolution, factor)
                self.res_aggregation('key_012', start_t_epoch, resolution, factor)
                self.res_aggregation('key_021', start_t_epoch, resolution, factor)
                self.res_aggregation('key_031', start_t_epoch, resolution, factor)
                self.res_aggregation('key_032', start_t_epoch, resolution, factor)
                self.res_aggregation('key_041', start_t_epoch, resolution, factor)
                self.res_aggregation('key_0211', start_t_epoch, resolution, factor)
                self.res_aggregation('key_0311', start_t_epoch, resolution, factor)
                self.res_aggregation('key_0321', start_t_epoch, resolution, factor)
                self.res_aggregation('key_0411', start_t_epoch, resolution, factor)


if __name__ == '__main__':

    import logging.config
    logging.config.fileConfig("../config/logging.conf")

    ur = UrAggregator(28)
    ur.main(86400, None)
