import warnings
warnings.filterwarnings("ignore")

import multiprocessing
# from datetime import datetime
# from apscheduler.schedulers.background import BackgroundScheduler

from zvt.api.data_type import Region
from zvt.api.fetch import fetch_data
# from zvt.utils.time_utils import next_date

# sched = BackgroundScheduler()


# @sched.scheduled_job('interval', days=1)
def main():
    multiprocessing.set_start_method('spawn')

    fetch_data(Region.CHN)
    # fetch_data(Region.US)


if __name__ == '__main__':
    main()

    # print("scheduling in processing...")
    # print("next triggering time will be at: {}".format(next_date(datetime.now(), days=1)))
    # print("good day...")
    # print("")
    # sched.start()
    # sched._thread.join()
