import warnings
warnings.filterwarnings("ignore")

# from datetime import datetime
# from apscheduler.schedulers.background import BackgroundScheduler

from findy.interface import Region
from findy.interface.fetch import fetching

# sched = BackgroundScheduler()


# @sched.scheduled_job('interval', days=1)
def main():
    # multiprocessing.set_start_method('fork')

    fetching(Region.CHN)


if __name__ == '__main__':
    main()

    # print("scheduling in processing...")
    # print("next triggering time will be at: {}".format(next_date(datetime.now(), days=1)))
    # print("good day...")
    # print("")
    # sched.start()
    # sched._thread.join()
