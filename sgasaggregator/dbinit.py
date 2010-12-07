from sqlalchemy import orm

def init_model(session, engine):
    """ Call me before using any of the tables or classes in the model """

    session.engine = engine
    session.metadata.bind = engine

    session.metadata.create_all(checkfirst=True)

    ses = orm.sessionmaker(bind=engine)
    session.Session = orm.scoped_session(ses)

