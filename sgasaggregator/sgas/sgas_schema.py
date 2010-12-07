"""
Metadata tables (sqlalchemy) for SGAS 
"""
__author__ = "Placi Flury grid@switch.ch"
__date__ = "02.08.2010"
__version__ = "0.0.1"

import sqlalchemy as sa
from sqlalchemy.orm import mapper
import sqlalchemy.databases as sadb

from sgasaggregator.sgas import session



t_usagerecords = sa.Table("usagerecords", session.metadata,
    sa.Column('record_id',          sa.types.VARCHAR(1000), nullable = False, primary_key = True),
    sa.Column('create_time',        sa.types.TIMESTAMP,     nullable = False),
    sa.Column('global_user_name',   sa.types.VARCHAR(1000),  nullable = False),
    sa.Column('vo_type',            sa.types.VARCHAR(100)),
    sa.Column('vo_issuer',          sa.types.VARCHAR(1000)),
    sa.Column('vo_name',            sa.types.VARCHAR(1000), nullable = False),
    sa.Column("vo_attributes",      sadb.postgresql.PGArray(sa.types.VARCHAR(100))),
    sa.Column('machine_name',       sa.types.VARCHAR(200), nullable = False),
    sa.Column('global_job_id',      sa.types.VARCHAR(1000)),
    sa.Column('local_job_id',       sa.types.VARCHAR(500)),
    sa.Column('local_user_id',      sa.types.VARCHAR(100)),
    sa.Column('job_name',           sa.types.VARCHAR(1000)),
    sa.Column('charge',             sa.types.NUMERIC(12,2)),
    sa.Column('status',             sa.types.VARCHAR(100)),
    sa.Column('queue',              sa.types.VARCHAR(200)),
    sa.Column('host',               sa.types.VARCHAR(1500)),
    sa.Column('node_count',         sa.types.INTEGER),
    sa.Column('project_name',       sa.types.VARCHAR(200)),
    sa.Column('submit_host',        sa.types.VARCHAR(200)),
    sa.Column("start_time",         sa.types.TIMESTAMP),
    sa.Column("end_time",           sa.types.TIMESTAMP),
    sa.Column("submit_time",        sa.types.TIMESTAMP),
    sa.Column('cpu_duration',       sa.types.NUMERIC(12,2)),
    sa.Column('wall_duration',      sa.types.NUMERIC(12,2)),
    sa.Column('ksi2k_cpu_duration', sa.types.NUMERIC(12,2)),
    sa.Column('ksi2k_wall_duration', sa.types.NUMERIC(12,2)),
    sa.Column('user_time',          sa.types.NUMERIC(12,2)),
    sa.Column('kernel_time',        sa.types.NUMERIC(12,2)),
    sa.Column('major_page_faults',  sa.types.INTEGER),
    sa.Column("runtime_environments", sadb.postgresql.PGArray(sa.types.VARCHAR(512))),
    sa.Column('exit_code',          sa.types.INTEGER),
    sa.Column('insert_hostname',    sa.types.VARCHAR(1024)),
    sa.Column("insert_identity",    sa.types.VARCHAR(1024), nullable = False),
    sa.Column('insert_time',        sa.types.TIMESTAMP)
)


class UsageRecords(object):
    pass



mapper(UsageRecords, t_usagerecords)


