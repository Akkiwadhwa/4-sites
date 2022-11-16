import time
from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from products import *

executors = {
    'default': ThreadPoolExecutor(20),
}
scheduler = BackgroundScheduler(timezone="asia/kolkata", executors=executors)
scheduler.start()
wong = scheduler.add_job(products, 'cron', hour="0", minute="17")
while True:
    scheduler.print_jobs()
    time.sleep(60 * 60 * 2)
