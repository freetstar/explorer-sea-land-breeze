#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from datetime import datetime

from pandas import DataFrame,Series
import pandas as pd
import numpy as np

for year in range(2005,2019):
    df = pd.read_csv("54623/54623-%s.text" % year ,delimiter=" ",header=None)
    df = df.iloc[:,[2,3,4,5,14,15]]
    df.columns = ["Year","Month","Day","Hour","Wind_dir","Wind_speed"]
    df['Datetime']  = (pd.to_datetime(df['Year'].astype(str) + '-' +
                                    df['Month'].astype(str) + '-' +
                                    df['Day'].astype(str) + ' ' +
                                    df['Hour'].astype(str) + ":00:00"))
    df['Datetime']  = df['Datetime'] +  pd.to_timedelta(8,unit='h')


    df = df.drop(['Day','Month','Year', 'Hour'], axis=1)


    #留下全天最大风力小于8.0的时间
    filter = df["Wind_speed"].groupby(df.Datetime.dt.date).max() <= 8.0
    df = df[~df["Datetime"].dt.date.isin(filter[filter.values != True].index)]

    #return x["Wind_dir"] < 180 and x["Wind_dir"] > 45  and df["Wind_speed"] >= 1.0

    grouped = df.groupby(df.Datetime.dt.date)

    from itertools import groupby
    def ranges(lst):
        pos = (j - i for i, j in enumerate(lst))
        t = 0
        for i, els in groupby(pos):
            l = len(list(els))
            el = lst[t]
            t += l
            yield range(el, el+l)



    with open("land_wind_results/land_wind_%s_result.txt" % year,"w") as f:
        for name, group in grouped:
            #筛选出12小时到20小时的
            group_1321 = group[ (group.Datetime.dt.hour >= 1 ) & (group.Datetime.dt.hour <= 8)]
            #满足海陆凤的index
            s_index = group_1321[(225 <= group_1321["Wind_dir"] ) & (group_1321["Wind_dir"] <= 360 ) & (group_1321["Wind_speed"] >= 0.5 )].index
            if len(s_index) >= 3:
                for i_range in (ranges(s_index)):
                    if len(i_range) >=  3:
                        #print(list(i_range))
                        f.write("\r\n")
                        for r in (i_range):
                            f.write(group_1321.loc[r]["Datetime"].strftime("%Y-%m-%d %H:%M:%S") + "\r\n")
