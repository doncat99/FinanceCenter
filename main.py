import platform
import multiprocessing
import warnings
warnings.filterwarnings("ignore")

# from datetime import datetime
# from apscheduler.schedulers.background import BackgroundScheduler

from findy import findy_config
from findy.interface import ContentType, Region
from findy.interface.fetch import fetching

# sched = BackgroundScheduler()


def arg_parsing():
    import argparse

    # parse cli args
    parser = argparse.ArgumentParser()

    parser.add_argument("-debug",
                        action='store_true',
                        help="Debug message output")

    parser.add_argument("-fetch",
                        choices=[e.value for e in ContentType],
                        help=f"fetch data type. support: {[e.value for e in ContentType]}")

    parser.add_argument("-region",
                        choices=[e.value for e in Region],
                        help=f"fetch data region. support: {[e.value for e in Region]}")

    parser.add_argument("-v", action="version",
                        version="Financial-Dynamics v%s" % findy_config['version'],
                        help="prints version and exits")

    return parser.parse_args()


# @sched.scheduled_job('interval', days=1)
def fetch(args):
    if args.fetch is not None and args.region is not None:
        fetching([{"content_type": args.fetch, "region": [args.region]}])


if __name__ == '__main__':
    current_os = platform.system().lower()
    if current_os == 'darwin':
        pass
    elif current_os == "windows":
        multiprocessing.set_start_method("spawn")
    elif current_os == "linux":
        multiprocessing.set_start_method("spawn")
    else:
        print("system not support.")
        exit()

    args = arg_parsing()
    print(args)

    findy_config['debug'] = args.debug

    fetch(args)

    # print("scheduling in processing...")
    # print("next triggering time will be at: {}".format(next_date(datetime.now(), days=1)))
    # print("good day...")
    # print("")
    # sched.start()
    # sched._thread.join()
