from celery import Celery
from crudcreate import Crud
from sqlalchemy import text
# from celeryapp import session
#from queue_changes import app1
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()
engine = create_engine('postgres+psycopg2://postgres:root@192.168.1.101/test', echo=True)
#engine = create_engine('postgres+psycopg2://edcloud:essentio@192.168.1.10/erpdatacloud')
Session = sessionmaker(bind=engine)
session = Session()
app1 = Celery('celerybasic', broker='redis://localhost:6379', backend='redis://localhost:6379')


@app1.task
def add(x,y):
    z = x + y
    user1 = Crud(x, y)
    session.add(user1)
    z = str(x) + str(y)
    session.flush()
    session.commit()

    return z + "added to database"

@app1.task
def dell(x,y):
    usr1 = session.query(Crud).filter(Crud.name == str(x)).first()
    try:
        session.delete(usr1)
    except:
        z = "Given name {} does not exist....".format(x)
        return z
    z = str(x) + "'s role changed to  " +str(y)
    print(usr1.name,usr1.role)
    session.flush()
    session.commit()
    return z  + ":Deleted from to database"
@app1.task
def upp(x,y):
    usr1 = session.query(Crud).filter(Crud.name == str(x)).first()
    # if not usr1:
    #     return "Nothing to be updated"
    try:
        usr1.role = y
    except:
        return "{} name does not exists in table crud".format(x)
    session.flush()
    session.commit()


    return str(x)  + "'s:role updated in database"
@app1.task
def show():
    data=""
    row = session.query(Crud).all()
    if not row:
        data = "Data Not found"
    for row1 in row:
        data += " " +str(row1.name) + " is " + str(row1.role)+"\n"
    return data
@app1.task
def select(x,y):
    data=""
    # row = session.query(Crud).filter(text("name =:value ")).params(value="Ronak").all()
    row = session.query(Crud).filter(text("{} ='{}' ".format(x,y))).all()
    #row = session.query(Crud).filter(Crud.name == x).all()
    #row = session.execute("select * from crud where {} = '{}'".format(x,y))
    if not row:
        data = "Data Not found"
    for row1 in row:
        data += " " + str(row1.name) + " is " + str(row1.role) + "\n"

    session.flush()
    session.commit()
    return data
#
# @app1.task
# def insert_data(j,k):
#     user1 = Crud(j,k)
#     session.add(user1)
#     session.commit()
#     return str(j)+ str(k)
# # add.apply_async((2,2))

# b = celery.send_task(add,(4,4))
# b = add.apply_async((2,6), countdown = 3)
# b = add.delay(4,2)
# b = b.get()
# print(b)

