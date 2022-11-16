import time
from apscheduler.schedulers.background import BackgroundScheduler
from products import *


scheduler = BackgroundScheduler(timezone="gmt")
scheduler.start()
wong = scheduler.add_job(track, 'cron', hour="1", minute="00")
products()
while True:
    scheduler.print_jobs()
    time.sleep(60 * 60 * 2)
