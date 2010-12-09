"""
Some helper functions to query aggregated information from the SGAS cache.

The semantics of the accouting record epoch times in the SGAS cache and 
the epoch times of a query are different. Accouting records
times refer to the aggregation of accounting data *up to* the 
specified point in time, while the time of a query wants to 
fetch the accounting records *starting* from a specified point
in time. We need to compensate this in our queries, by 
adding the 'resolution/sampling time -1' to our query times.


accouting records time as in db:

                --->|---->|----->|----->|----->|---->|--
                   t_0   t_1    t_2    t_3    t_4

query time:  a query for the interval t'_1 to t'_3 
             would return the db records t_1, t_2, t_3

                ---|>----|>-----|>-----|>-----|>----|>--
                  t'_0  t'_1   t'_2   t'_3   t'_4

the correction factor is thus:

    t'_start -> t'_start + resolution -1
    t'_end -> t'_end + resolution -1

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
    start_t_epoch = start_t_epoch - (start_t_epoch % resolution) 
    end_t_epoch =  end_t_epoch - (end_t_epoch % resolution) + resolution - 1
    
    if resolution == 0:
        log.warn("Got 0 resolution, can't fulfill request.")
        return None

    return sgascache_session.Session.query(ag_schema.User).filter(and_(
        ag_schema.User.t_epoch >= start_t_epoch,
        ag_schema.User.t_epoch < end_t_epoch,
        ag_schema.User.global_user_name == DN,
        ag_schema.User.resolution == resolution))


def get_cluster_acrecords(hostname, start_t_epoch, end_t_epoch, resolution):
    """ returns a query object of the jobs or None, upon which 
        one can iterate.
        hostname: hostname of the cluster frontend
        start_t_epoch: starting time in epoch
        end_t_epoch: endint time in epoch
        resolution: resolution of (aggregated) jobs. If set to 0 
                    the original (non-aggregated) data will be returned
    """
    
    start_t_epoch = start_t_epoch - (start_t_epoch % resolution) 
    end_t_epoch =  end_t_epoch - (end_t_epoch % resolution) + resolution - 1
    
    if resolution == 0:
        log.warn("Got 0 resolution, can't fulfill request.")
        return None


    return sgascache_session.Session.query(ag_schema.Machine).filter(and_(
        ag_schema.Machine.t_epoch >= start_t_epoch,
        ag_schema.Machine.t_epoch < end_t_epoch,
        ag_schema.Machine.machine_name == hostname,
        ag_schema.Machine.resolution == resolution))

