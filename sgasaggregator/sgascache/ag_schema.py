""" Schema of aggregated SGAS records.

They will be 15 different key types, all resulting from the
'logical key' we consider being the most granular:

key_0:   global_user_name + vo_name + machine_name + status

from which are deduced:

key_01: global_user_name + vo_name + machine_name
key_02: global_user_name + vo_name + status
key_03: vo_name + machine_name + status
key_04: global_user_name + machine_name + status

from which are deduced records with key:

key_011: global_user_name  + vo_name
key_012: global_user_name + machine_name

key_021: global_user_name + status

key_031: vo_name + status
key_032: vo_name + machine_name

key_041: machine_name + status

from which we deduce:

key_0211: global_user_name

key_0311: status

key_0321: vo_name

key_0411: machine_name

Each of this keys will have its own table where the aggregated values are stored. The tables may hold
aggregates at different resolutions.

We have avoided using a table schema that requires 'joins'. Each table keeps therefore its own copy of
the variables that get aggregated.

"""

import sqlalchemy as sa
from sqlalchemy.orm import mapper

from sgasaggregator.sgascache import session as sgascache_session


"""
t_value = sa.Table("values", sgascache_session.metadata,
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0
)
"""

# key_0
t_user_vo_machine_status = sa.Table("user_vo_machine_status", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('vo_name',            sa.types.VARCHAR(50), primary_key = True),
    sa.Column('machine_name',       sa.types.VARCHAR(50), primary_key = True),
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution',         sa.types.INTEGER, autoincrement = False, primary_key = True),
    sa.Column('t_epoch',            sa.types.INTEGER, autoincrement = False, primary_key = True),
    sa.Column('n_jobs',             sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration',       sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration',      sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time',          sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time',        sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults',  sa.types.INTEGER, default = 0)
)

# key_01
t_user_vo_machine = sa.Table("user_vo_machine", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('vo_name',            sa.types.VARCHAR(50), primary_key = True),
    sa.Column('machine_name',       sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)
#key_02
t_user_vo_status = sa.Table("user_vo_status", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('vo_name',            sa.types.VARCHAR(50), primary_key = True),
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_03
t_vo_machine_status = sa.Table("vo_machine_status", sgascache_session.metadata,
    sa.Column('vo_name',            sa.types.VARCHAR(50), primary_key = True),
    sa.Column('machine_name',       sa.types.VARCHAR(50), primary_key = True),
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_04
t_user_machine_status = sa.Table("user_machine_status", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('machine_name',       sa.types.VARCHAR(50), primary_key = True),
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('status', sa.types.VARCHAR(100), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

# key_011
t_user_vo = sa.Table("user_vo", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('vo_name',            sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

# key_012
t_user_machine = sa.Table("user_machine", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('machine_name',       sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_021
t_user_status = sa.Table("user_status", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_031
t_vo_status = sa.Table("vo_status", sgascache_session.metadata,
    sa.Column('vo_name',            sa.types.VARCHAR(200), primary_key = True),
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_032
t_vo_machine = sa.Table("vo_machine", sgascache_session.metadata,
    sa.Column('vo_name',            sa.types.VARCHAR(200), primary_key = True),
    sa.Column('machine_name',       sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_041
t_machine_status = sa.Table("machine_status", sgascache_session.metadata,
    sa.Column('machine_name',       sa.types.VARCHAR(200), primary_key = True),
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_0211
t_user = sa.Table("user", sgascache_session.metadata,
    sa.Column('global_user_name',   sa.types.VARCHAR(200), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_0311
t_status = sa.Table("status", sgascache_session.metadata,
    sa.Column('status',             sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_0321
t_vo = sa.Table("vo", sgascache_session.metadata,
    sa.Column('vo_name',            sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)

#key_0411
t_machine = sa.Table("machine", sgascache_session.metadata,
    sa.Column('machine_name',       sa.types.VARCHAR(50), primary_key = True),
    sa.Column('resolution', sa.types.INTEGER, primary_key = True),
    sa.Column('t_epoch', sa.types.INTEGER, primary_key = True),
    sa.Column('n_jobs', sa.types.INTEGER, default= 0),
    sa.Column('cpu_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('wall_duration', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('user_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('kernel_time', sa.types.NUMERIC(12,2), default = 0.0),
    sa.Column('major_page_faults', sa.types.INTEGER, default = 0)
)


RES_DEFAULT = 86400  # seconds per day


class UserVoMachineStatus(object):

    def __init__(self, global_user_name, vo_name, machine_name, status, res, t_epoch):

        self.global_user_name = global_user_name
        self.vo_name = vo_name
        self.machine_name = machine_name
        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return UserVoMachineStatus(self.global_user_name, self.vo_name, self.machine_name,
                                   self.status, self.resolution, self.t_epoch)



class UserVoMachine(object): #key01

    def __init__(self, global_user_name, vo_name, machine_name, res, t_epoch):
        self.global_user_name = global_user_name
        self.vo_name = vo_name
        self.machine_name = machine_name
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return UserVoMachine(self.global_user_name, self.vo_name, self.machine_name,
                             self.resolution, self.t_epoch)


class UserVoStatus(object): # key02

    def __init__(self, global_user_name, vo_name, status, res, t_epoch):

        self.global_user_name = global_user_name
        self.vo_name = vo_name
        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return UserVoStatus(self.global_user_name, self.vo_name, self.status,
                            self.resolution, self.t_epoch)


class VoMachineStatus(object): # key03

    def __init__(self, vo_name, machine_name, status, res, t_epoch):

        self.vo_name = vo_name
        self.machine_name = machine_name
        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return VoMachineStatus(self.vo_name, self.machine_name, self.status,
                               self.resolution, self.t_epoch)


class UserMachineStatus(object): #key04

    def __init__(self, global_user_name, machine_name, status, res, t_epoch):
        self.global_user_name = global_user_name
        self.machine_name = machine_name
        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return UserMachineStatus(self.global_user_name, self.machine_name,
                                 self.status, self.resolution, self.t_epoch)


class UserVo(object): #key011

    def __init__(self, global_user_name, vo_name, res, t_epoch):

        self.global_user_name = global_user_name
        self.vo_name = vo_name
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return UserVo(self.global_user_name, self.vo_name, self.resolution, self.t_epoch)


class UserMachine(object): #key012

    def __init__(self, global_user_name,  machine_name, res, t_epoch):

        self.global_user_name = global_user_name
        self.machine_name = machine_name
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return UserMachine(self.global_user_name, self.machine_name, self.resolution, self.t_epoch)


class UserStatus(object): # key021

    def __init__(self, global_user_name, status, res, t_epoch):

        self.global_user_name = global_user_name
        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return UserStatus(self.global_user_name, self.status, self.resolution, self.t_epoch)


class VoStatus(object): # key031

    def __init__(self, vo_name, status, res, t_epoch):

        self.vo_name = vo_name
        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return VoStatus(self.vo_name, self.status, self.resolution, self.t_epoch)


class VoMachine(object): # key032

    def __init__(self, vo_name, machine_name, res, t_epoch):

        self.vo_name = vo_name
        self.machine_name = machine_name
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return VoMachine(self.vo_name, self.machine_name, self.resolution, self.t_epoch)


class MachineStatus(object): #key041

    def __init__(self, machine_name, status, res, t_epoch):

        self.machine_name = machine_name
        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return MachineStatus(self.machine_name, self.status, self.resolution, self.t_epoch)


class User(object): # key0211

    def __init__(self, global_user_name, res, t_epoch):

        self.global_user_name = global_user_name
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return User(self.global_user_name, self.resolution, self.t_epoch)


class Status(object): # key0311

    def __init__(self, status, res, t_epoch):

        self.status = status
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return Status(self.status, self.resolution, self.t_epoch)


class Vo(object): # key032

    def __init__(self, vo_name, res, t_epoch):

        self.vo_name = vo_name
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return Vo(self.vo_name, self.resolution, self.t_epoch)


class Machine(object): #key0411

    def __init__(self, machine_name, res, t_epoch):

        self.machine_name = machine_name
        self.t_epoch = t_epoch

        self.resolution = res or RES_DEFAULT

    def clone(self):
        return Machine(self.machine_name, self.resolution, self.t_epoch)



mapper(UserVoMachineStatus, t_user_vo_machine_status) # key_0

mapper(UserVoMachine, t_user_vo_machine) # key_01
mapper(UserVoStatus, t_user_vo_status) # key_02
mapper(VoMachineStatus, t_vo_machine_status) # key_03
mapper(UserMachineStatus, t_user_machine_status) # key_04

mapper(UserVo, t_user_vo) # key_011
mapper(UserMachine, t_user_machine) # key_012

mapper(UserStatus, t_user_status) # key_021

mapper(VoStatus, t_vo_status) # key_031
mapper(VoMachine, t_vo_machine) # key_032

mapper(MachineStatus, t_machine_status) # key_041

mapper(User, t_user) # key_0211

mapper(Status, t_status) # key_0311

mapper(Vo, t_vo) # key_0321

mapper(Machine, t_machine) # key_0411

