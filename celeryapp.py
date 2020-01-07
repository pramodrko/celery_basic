import json,falcon
from gevent.pywsgi import WSGIServer
# from celerybasic import add
from queue_changes import insertion,deletion,updation,showdata,selection


class Cel:

    def on_get(self,req,resp):
        data = req.stream.read().decode('UTF-8')
        doc = json.loads(data)
        if 'name' in doc:
            data1 = list(doc["name"])
        else:
            pass
        if 'role' in doc:
            data2 = list(doc["role"])
        else:
            pass
        if 'column' in doc:
            data3 = list(doc["column"])
        else:
            pass
        if 'value' in doc:
            data4 = list(doc["value"])
        else:
            pass
        #celery_result =  app1.send_task('celerydata.insert_data', queue='app_queue', kwargs={"j":str(data1[0]), "k":str(data2[0])})
        #results = addition(data1[0],data2[0])
        if "add" in doc:
            results = insertion(str(data1[0]),str(data2[0]))
        elif "dell" in doc:
            results = deletion(str(data1[0]), str(data2[0]))
        elif "up" in doc:
            results = updation(str(data1[0]), str(data2[0]))
        elif "show" in doc:
            results = showdata()
        elif "select" in doc:
            results = selection(str(data3[0]),str(data4[0]))
        else:
            print("Check input")
        # while not AsyncResult(celery_result.id).ready():
        #     pass
        # result = AsyncResult(celery_result.id).result

        resp.status =falcon.HTTP_200
        #result = {'taks_id':task.id}
        # resp.body = json.dumps(result)
        resp.body = str(results)
        # resp.body = json.dumps(ddoc)
        return resp


app = falcon.API()
app.add_route('/cel',Cel())
WSGIServer(('127.0.0.1', 8088), app).serve_forever()