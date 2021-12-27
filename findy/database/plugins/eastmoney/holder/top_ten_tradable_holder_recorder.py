# -*- coding: utf-8 -*-
from findy.interface import Region, Provider
from findy.database.schema.misc.holder import TopTenTradableHolder
from findy.database.plugins.eastmoney.holder.top_ten_holder_recorder import TopTenHolderRecorder


class TopTenTradableHolderRecorder(TopTenHolderRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = TopTenTradableHolder

    url = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetShiDaLiuTongGuDong'
    path_fields = ['ShiDaLiuTongGuDongList']
    timestamps_fetching_url = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetFirstRequest2Data'
    timestamp_list_path_fields = ['SDLTGDBGQ', 'ShiDaLiuTongGuDongBaoGaoQiList']
    timestamp_path_fields = ['BaoGaoQi']
