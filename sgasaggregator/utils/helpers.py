"""
Some helper functions to query aggregated information from the SGAS cache.

When querying for aggregated accouting records, the time interval of the query
must be adjusted so it falls within the sampling interval of the aggregates. 

Assume the orignal accounting records, arrived in at the following times (the 
asterisk '*' represents the end times of the job) 

original data: 
             ------*--****-*---***---------**------> t_c 

sampling at inteval 'resolution':
             ---|-----|-----|-----|-----|-----|----> t_d
             ------*--****-*---***---------**------> t_c 
gives us following discrete aggregates:

              0 |  2  |  4  |   3 |  0  |   2 | 0  > t_d


If we query for the accouting records from t_1 - t_2 (contiuous time, which
does not fall within sampling boundaries)

            ------|-----------------|--------------->t_c
                  t_1              t_2

we need to transfrom the continuous times to their coresponding discrete times:

             ---|-----|-----|-----|-----|-----|----> t_d
            ------|-----------------|--------------->t_c
                  t_1              t_2
             ---|-----------------------|----------> t_d
                t_1_new                 t_2_new

hence, t_1_new =  t_1 - (t_1 modulo resolution)
       t_2_new =  t_2 - (t_2 modulo resolution) + resolution

"""

from sqlalchemy import and_
import logging

from sgasaggregator.sgascache import ag_schema
from sgasaggregator.sgascache import session as sgascache_session

log = logging.getLogger(__name__)

def get_user_acrecords(DN, start_t_epoch, end_t_epoch, resolution):
    """ returns a query object of the jobs or None, upon which 
        one can iterate.
        DN: user's certificate DN
        start_t_epoch: starting time in epoch
        end_t_epoch: endint time in epoch
        resolution: resolution of (aggregated) jobs. 
    """
    
    if resolution == 0:
        log.warn("Got 0 resolution, can't fulfill request.")
        return None

    s_start, s_end = get_sampling_interval(start_t_epoch, end_t_epoch, resolution)

    return sgascache_session.Session.query(ag_schema.User).filter(and_(
        ag_schema.User.t_epoch >= s_start,
        ag_schema.User.t_epoch <= s_end,
        ag_schema.User.global_user_name == DN,
        ag_schema.User.resolution == resolution)).order_by(ag_schema.User.t_epoch)

def get_cluster_user_acrecords(hostname, DN, start_t_epoch, end_t_epoch, resolution):
    """ returns a query object of the jobs or None, upon which 
        one can iterate.
        hostname: hostname of the cluster frontend
        DN: user's certificate DN
        start_t_epoch: starting time in epoch
        end_t_epoch: endint time in epoch
        resolution: resolution of (aggregated) jobs. If set to 0 
                    the original (non-aggregated) data will be returned
    """
    
 
    if resolution == 0:
        log.warn("Got 0 resolution, can't fulfill request.")
        return None
    
    s_start, s_end = get_sampling_interval(start_t_epoch, end_t_epoch, resolution)

    return sgascache_session.Session.query(ag_schema.UserMachine).filter(and_(
        ag_schema.UserMachine.t_epoch >= s_start,
        ag_schema.UserMachine.t_epoch <=  s_end,
        ag_schema.UserMachine.machine_name == hostname,
        ag_schema.UserMachine.global_user_name == DN,
        ag_schema.UserMachine.resolution == resolution))


def get_cluster_acrecords(hostname, start_t_epoch, end_t_epoch, resolution):
    """ returns a query object of the jobs or None, upon which 
        one can iterate.
        hostname: hostname of the cluster frontend
        start_t_epoch: starting time in epoch
        end_t_epoch: endint time in epoch
        resolution: resolution of (aggregated) jobs. If set to 0 
                    the original (non-aggregated) data will be returned
    """
    
 
    if resolution == 0:
        log.warn("Got 0 resolution, can't fulfill request.")
        return None
    
    s_start, s_end = get_sampling_interval(start_t_epoch, end_t_epoch, resolution)

    return sgascache_session.Session.query(ag_schema.Machine).filter(and_(
        ag_schema.Machine.t_epoch >= s_start,
        ag_schema.Machine.t_epoch <=  s_end,
        ag_schema.Machine.machine_name == hostname,
        ag_schema.Machine.resolution == resolution))

def get_cluster_names():
    """ returns list of distinct clusters """
    clusters = []
    for arec in sgascache_session.Session.query(ag_schema.Machine.machine_name).distinct():
        cluster_name  = arec.machine_name
        if not cluster_name:
            continue
        clusters.append(cluster_name)
    return clusters

def get_vo_acrecords(vo_name, start_t_epoch, end_t_epoch, resolution):
    """ returns a query object of the jobs or None, upon which 
        one can iterate.
        vo_name:  VO name of the 
        start_t_epoch: starting time in epoch
        end_t_epoch: endint time in epoch
        resolution: resolution of (aggregated) jobs. If set to 0 
                    the original (non-aggregated) data will be returned
    """
    if resolution == 0:
        log.warn("Got 0 resolution, can't fulfill request.")
        return None
    
    s_start, s_end = get_sampling_interval(start_t_epoch, end_t_epoch, resolution)

    return sgascache_session.Session.query(ag_schema.Vo).filter(and_(
        ag_schema.Vo.t_epoch >= s_start,
        ag_schema.Vo.t_epoch <=  s_end,
        ag_schema.Vo.vo_name == vo_name,
        ag_schema.Vo.resolution == resolution)).order_by(ag_schema.Vo.t_epoch)

def get_cluster_vo_acrecords(cluster_name, vo_name, start_t_epoch, end_t_epoch, resolution):
    """ returns a query object of the jobs or None, upon which 
        one can iterate.
        vo_name:  VO name of the 
        start_t_epoch: starting time in epoch
        end_t_epoch: endint time in epoch
        resolution: resolution of (aggregated) jobs. If set to 0 
                    the original (non-aggregated) data will be returned
    """
    if resolution == 0:
        log.warn("Got 0 resolution, can't fulfill request.")
        return None
    
    s_start, s_end = get_sampling_interval(start_t_epoch, end_t_epoch, resolution)

    return sgascache_session.Session.query(ag_schema.VoMachine).filter(and_(
        ag_schema.VoMachine.t_epoch >= s_start,
        ag_schema.VoMachine.t_epoch <=  s_end,
        ag_schema.VoMachine.vo_name == vo_name,
        ag_schema.VoMachine.machine_name == cluster_name,
        ag_schema.VoMachine.resolution == resolution)).order_by(ag_schema.VoMachine.t_epoch)


def get_vo_names():
    """ returns list of distinct VOs """
    vos = []
    for arec in sgascache_session.Session.query(ag_schema.Vo.vo_name).distinct():
        vo = arec.vo_name
        vos.append(vo)   # i.e. vo can be  NULL/None

    return vos



def get_sampling_interval(start_t_epoch, end_t_epoch, resolution):
    """ The time interval of a *continuous* query must be adapted, so 
        it matches the *discrete* time boundaries (or sampling time 
        boundaries) of the aggregates.
        This function returns the start and end times of the sampling 
        interval for any specifies continuous time interval.

        Input:  start_t_epoch: starting time (continuous), in epoch time
                end_t_epoch:   end time (continuous), in epoch time
                resolution: resolution, aka sampling size/rate 

        Returns: sampling_start_t, sampling_end_t
    """
    samp_start_t = start_t_epoch - (start_t_epoch % resolution) 
    samp_end_t=  end_t_epoch - (end_t_epoch % resolution) + resolution - 1

    return samp_start_t, samp_end_t    
