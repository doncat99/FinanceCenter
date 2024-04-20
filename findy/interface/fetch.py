import warnings
warnings.filterwarnings("ignore")

import logging

from findy.interface import ContentType, Region
from findy.interface.fetch_task import task_news_chn, task_news_us, task_stock_chn, task_stock_us
from findy.task.execution import task_execution

logger = logging.getLogger(__name__)


def fetching(fetchList):
    task_set = []

    for item in fetchList:
        if len(list(filter(lambda x: x in ["content_type", "region"], item.keys()))) != 2:
            logger.warning(f'wrong fetching parameters! please check fetching parameters again. {item}')
            continue

        if item["content_type"] == ContentType.News.value:
            
            for region in item["region"]:
                if region == Region.CHN.value:
                    task_set.extend(task_news_chn)
                elif region == Region.US.value:
                    task_set.extend(task_news_us)

        if item["content_type"] == ContentType.Stock.value:

            for region in item["region"]:
                if region == Region.CHN.value:
                    task_set.extend(task_stock_chn)
                elif region == Region.US.value:
                    task_set.extend(task_stock_us)

    if len(task_set) > 0:
        task_execution(task_set)


if __name__ == '__main__':
    fetchList = [{"content_type": 'news', "region": ['us', 'chn']}, {"content_type": 'stocks', "region": ['us']}]

    fetching(fetchList)
