#!/usr/bin/python
# -*- coding:utf-8 -*-
# @Time    :  3:33 PM
# @Author  : zengymf
# @File    : main.py
# @Describe:

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

def task1(_pd):
    """
    筛选出app_name包含fe的记录，找出每个流水线的构建次数，取前10并可视化
    :return:
    """

    # app_name lowercase
    app_name_series = _pd["app_name"].str.lower()
    _pd["app_name"] = app_name_series

    # 筛选fe条目
    fe_pd = _pd[_pd["app_name"].str.find("fe") > -1]
    grouped_fe_pd = fe_pd.groupby("app_name").max(numeric_only=True)
    top10_pd = grouped_fe_pd.sort_values(by="build_version", ascending=False).head(10)

    top10_pd["build_version"].plot(kind="bar")
    plt.xlabel("app_name")
    plt.ylabel("build_version")
    plt.show()
    print(top10_pd)


def task2(_pd):
    """
    生成流水线各节点是否开启的数据透视图
    :return:
    """

    indexs = _pd.groupby("app_name")["build_version"].idxmax()
    filtered_pd = _pd.loc[indexs].sample(frac=0.2)

    new_pd = pd.DataFrame()

    for row_index, row in filtered_pd.iterrows():
        steps = row["step_status"].split("--")

        for tmp in steps:
            step = tmp.split(":")[0]
            row["s"+step] = True

        new_pd = new_pd.append(row)

    step_status = new_pd["step_status"]
    new_pd = new_pd.drop(columns=["step_status", "start_time", "end_time", "build_version", "env", "user_name"])
    new_pd.insert(1, "step_status", step_status)
    print(new_pd)

def task3(_pd):
    """
    统计各年月执行流水线的次数，可视化
    :return:
    """

    start_time = pd.to_datetime(_pd["start_time"], format="%Y-%m-%d")
    create_month = start_time.map(lambda x: "%s-%s" % (x.year, x.month))
    _pd["create_month"] = create_month

    grouped_pd = _pd.groupby("create_month").count()["app_name"]
    grouped_pd.T.plot(kind="barh")
    plt.xlabel("assembly_count")
    plt.ylabel("app_name")
    plt.show()
    print(grouped_pd)

def task4(_pd):
    """
    计算各流水线记录的执行时间，作为一列添加
    :return:
    """

    new_pd = _pd.dropna()
    new_pd["spend_time"] = new_pd.apply(lambda x: str_to_datetime(x["end_time"]) - str_to_datetime(x["start_time"]), axis=1)
    new_pd = new_pd.sort_values(by="spend_time")
    print(new_pd)

def task5(_pd):
    """
    随机取10个应用，生成以app_name为index，env为columns，build_version为值的数据透视图
    :param _pd:
    :return:
    """

    groupd_pd = _pd.groupby(["app_name", "env"]).max(numeric_only=True)
    sample_pd = groupd_pd.sample(frac=0.1)
    pivot_pd = pd.pivot_table(sample_pd, values="build_version", index="app_name", columns="env")

    print(pivot_pd)

def str_to_datetime(datetime_str):
    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    return dt


if __name__ == '__main__':
    assembly_pd = pd.read_excel("app_data1.xlsx")
    task2(assembly_pd)

