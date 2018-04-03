from celery import Celery

app = Celery('tasks',
             broker='pyamqp://guest:hb_10086@localhost',
             backend='db+postgresql://postgres:root@localhost/postgres')

@app.task
def add(x, y):
    return x + y
