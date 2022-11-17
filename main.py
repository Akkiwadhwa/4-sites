import time
from multiprocessing import Process
from apscheduler.schedulers.background import BackgroundScheduler
from products import *
from track import *
from websites import websites

websites()
scheduler = BackgroundScheduler(timezone="gmt")
scheduler.start()
track = scheduler.add_job(track, 'cron', hour="1", minute="00")
m = Process(target=main_metro)
v = Process(target=main_vivanda)
w = Process(target=main_wong)
p = Process(target=main_Plazavea)
m.start()
v.start()
w.start()
p.start()
while True:
    scheduler.print_jobs()
    time.sleep(60 * 60 * 2)
