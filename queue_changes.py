from celery import Celery
from celery.result import AsyncResult

app1 = Celery('celerybasic', broker='redis://localhost:6379', backend='redis://localhost:6379')


def addition(x,y):
    print("DDDDDDDDDDDDDDDDDDDDDDD")
    celery_result = app1.send_task('celerybasic.add', queue='app_queue', kwargs={"x": x, "y": y})
    while not AsyncResult(celery_result.id).ready():
        pass
    result = AsyncResult(celery_result.id).result
    print("DDDDDDDDDDDDDDDDDDDDDDD", str(result))
    return str(result)


def insertion(x,y):
    print("XXXXXXXXXX")
    celery_results = app1.send_task('celerybasic.add', queue='app_queue', kwargs={"x": x, "y": y})
    while not AsyncResult(celery_results.id).ready():
        pass
    result = AsyncResult(celery_results.id).result
    print("DDDDDDDDDDDDDDDDDDDDDDD", str(result))
    return str(result) + "added to database"

def deletion(x,y):
    print("XXXXXXXXXX")
    celery_results = app1.send_task('celerybasic.dell', queue='app_queue', kwargs={"x": x, "y": y})
    while not AsyncResult(celery_results.id).ready():
        pass
    result = AsyncResult(celery_results.id).result
    print("DDDDDDDDDDDDDDDDDDDDDDD", str(result))
    return str(result) + ":Deleted from to database"

def updation(x,y):
    print("XXXXXXXXXX")
    celery_results = app1.send_task('celerybasic.upp', queue='app_queue', kwargs={"x": x, "y": y})
    while not AsyncResult(celery_results.id).ready():
        pass
    result = AsyncResult(celery_results.id).result
    print("DDDDDDDDDDDDDDDDDDDDDDD", str(result))
    return str(result) + "'s:role updated in database"
def showdata():
    print("XXXXX")
    celery_results = app1.send_task('celerybasic.show', queue = 'app_queue')
    while not AsyncResult(celery_results.id).ready():
        pass
    result = AsyncResult(celery_results.id).result
    print("DDDDDDDD", str(result))
    return str(result)

def selection(x,y):
    print("XXXXXXXXXXXXX")
    celery_results = app1.send_task('celerybasic.select', queue = 'app_queue',kwargs={"x":x, "y":y})
    while not AsyncResult(celery_results.id).ready():
        pass
    result = AsyncResult(celery_results.id).result
    print("DDDDDDDD",str(result))
    return str(result)