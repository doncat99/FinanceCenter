from multiprocessing import current_process, Pool

import pandas as pd
from tqdm import tqdm


class MultiProcess():
    def __init__(self, processes_cnt):
        self.__cpus = processes_cnt

    def cpus(self):
        return self.__cpus

    @staticmethod
    def getCurrentProcessID():
        return current_process()._identity[0]

    def imap(self, func, args, leave=True, ncols=80, pos=None, desc=''):
        with Pool(self.__cpus) as pool:
            with tqdm(total=len(args), leave=leave, ncols=ncols, position=pos) as pbar:
                pbar.set_description(desc)
                for _ in pool.imap_unordered(func, args):
                    pbar.update()

    def imapList(self, func, args, leave=True, ncols=80, pos=None, desc=''):
        res_list = []
        with Pool(self.__cpus) as pool:
            with tqdm(total=len(args), leave=leave, ncols=ncols, position=pos) as pbar:
                pbar.set_description(desc)
                for res in pool.imap_unordered(func, args):
                    res_list.append(res)
                    pbar.update()
        return res_list

    def imapDict(self, func, args, leave=True, ncols=80, pos=None, desc=''):
        res_dict = {}
        with Pool(self.__cpus) as pool:
            with tqdm(total=len(args), leave=leave, ncols=ncols, position=pos) as pbar:
                pbar.set_description(desc)
                for res in pool.imap_unordered(func, args):
                    res_dict.update(res)
                    pbar.update()
        return res_dict

    def imapDataFrame(self, func, args, leave=True, ncols=80, pos=None, desc=''):
        res_df = pd.DataFrame()
        with Pool(self.__cpus) as pool:
            with tqdm(total=len(args), leave=leave, ncols=ncols, position=pos) as pbar:
                pbar.set_description(desc)
                for res in pool.imap_unordered(func, args):
                    # res_df = pd.concat([res_df, res], sort=True)
                    res_df = pd.concat([res_df, res])
                    pbar.update()
        return res_df

    def imapCallback(self, func, callback, args, leave=True, ncols=80, pos=None, desc=''):
        global position_holders
        # position_holders = self.m.Array('i', [0] * self.__cpus)
        with Pool(self.__cpus) as pool:
            with tqdm(total=len(args), leave=leave, ncols=ncols, position=pos) as pbar:
                pbar.set_description(desc)
                for res in pool.imap_unordered(func, args):
                    callback(res)
                    pbar.update()

    def imapCallbackWithIndex(self, func, callback, args, leave=True, ncols=80, pos=None, desc=''):
        # global position_holders
        # position_holders = self.m.Array('i', [0] * self.__cpus)
        with Pool(self.__cpus) as pool:
            with tqdm(total=len(args), leave=leave, ncols=ncols, position=pos) as pbar:
                pbar.set_description(desc)
                for i, res in enumerate(pool.imap_unordered(func, args)):
                    callback([i, res])
                    pbar.update()
