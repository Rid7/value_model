# -*- coding: utf-8 -*-
# @File : get_data.py
# @Author : Zongxi.Li
# @Email : lizongxi1995@gmail.com
# @Time : 2022/05/12 14:37:03
import logging
import os
import time

import pandas as pd
from retry import retry
from tqdm import tqdm
from WindPy import w

logging.basicConfig(level=logging.DEBUG)

w.start()


def get_part_data(code, fields, begin_time, end_time, options=None, usedf=True):
    """获取分组数据

    Args:
        code (str): 股票代码
        fields (list): 因子列表
        begin_time (str): 开始时间
        end_time (str): 结束时间
        options (str, optional): wind接口的补充参数. Defaults to None.
        usedf (bool, optional): 数据是否按df格式返回. Defaults to True.

    Returns:
        DataFrame or str: 请求接口异常返回错误码, 正常返回wind接口数据, 未请求接口默认返回空df
    """
    @retry(TimeoutError, tries=3, delay=15)
    def get_wind_data():
        """获取wind因子数据

        Returns:
            tuple: [0]返回码，[1]因子数据df
        """
        time.sleep(3)   # 防止wind接口被封
        return w.wsd(code, fields, begin_time, end_time, options=options, usedf=usedf)

    if len(fields) > 0:
        logging.info(f'正在请求{code}的{fields}数据')

        errorcode, data = get_wind_data(
            code, fields, begin_time, end_time, options=part2_options, usedf=True)

        if errorcode != 0:
            logging.error(f'{code}的{fields}数据请求失败：错误码{errorcode}')
            return str(errorcode)   # 返回错误码, 错误码可能带有字符串，统一转换为str
        else:
            data.index = pd.to_datetime(data.index)
            return data
    return pd.DataFrame()


# 读取样本股票
sample_df = pd.read_excel(r'data/样本股票.xlsx')
codes = sample_df['证券代码'].tolist()

fields = ['pe_ttm',  # 市盈率PE(TTM)
          #   'pb',  # 市净率PB    不可回测
          'pb_mrq',  # 市净率PB(MRQ)
          #   'ps',  # 市销率PS    不可回测
          'ps_lyr',  # 市销率PS(LYR)
          'qfa_yoysales',  # 单季度.营业收入同比增长率
          'fa_orgr_ttm',  # 增长率-营业收入(TTM)_PIT
          #   'wgsd_growth_sales_3y',  # 总营业收入(近3年增长率)_GSD
          'growth_or',  # 营业收入(N年，增长率)
          #   'qfa_roe_deducted',  # 单季度.净资产收益率(扣除非经常损益)
          'deductedprofit_yoy',  # 单季度.扣除非经常性损益后的净利润同比增长率
          'fa_npgr_ttm',  # 增长率-净利润(TTM)_PIT
          #   'wgsd_growth_np_3y',  # 净利润(近3年增长率)_GSD
          'growth_profit',  # 净利润(N年，增长率)
          'qfa_yoyocf',  # 单季度.经营性现金净流量(同比增长率)
          'fa_cfogr_ttm',  # 增长率-经营活动产生的现金流量净额
          'wgsd_growth_ocf',  # 经营活动产生的现金流量净额(N年，增长率)
          'qfa_roe',  # 单季度.净资产收益率ROE
          'fa_roenp_ttm',  # 净资产收益率(TTM)_PIT
          'qfa_roa',  # 单季度.总资产净利率ROA
          'fa_netprofittoassets_ttm',  # 总资产净利率-不含少数股东损益(TTM)_PIT
          'fa_grossprofitmargin_ttm',  # 销售毛利率(TTM)_PIT
          'turnover_ttm',  # 总资产周转率(TTM)
          'operatecashflowtoop_ttm2',  # 经营活动产生的现金流量净额/营业利润(TTM)_GSD
          'fa_mlev',  # 市场杠杆PIT
          'fa_blev',  # 账面杠杆PIT
          'cashtocurrentdebt',  # 现金比率
          'current',  # 流动比率
          'longdebttoequity',  # 非流动负债权益比率
          'mkt_cap_ard',  # 总市值
          'wrating_avg_data',  # 综合评级(数值)
          'wgsd_assets',  # 总资产
          'wgsd_com_eq_paholder',  # 普通股权益总额_GSD
          #   'wgsd_pfd_stk',  # 优先股_GSD
          'wgsd_liabs_lt',  # 非流动负债合计_GSD
          'fcfe'    # 股权自由现金流
          ]

# 由于接口参数不同 将因子拆分为两组
fields_part1 = ['qfa_yoysales', 'pe_ttm']
fields_part2 = list(set(fields) - set(fields_part1))

begin_time = '2010-01-01'
end_time = '2020-12-31'
if not os.path.exists(r'data/factor'):
    os.mkdir(r'data/factor')

logging.info('开始获取因子数据')
for code in tqdm(codes):
    logging.info(f'正在获取{code}数据')

    # 读取本地数据 防止重复请求
    local_df = pd.DataFrame()
    unsaved_fields_part1 = fields_part1.copy()
    unsaved_fields_part2 = fields_part2.copy()
    if os.path.exists(rf'data/factor/{code}.csv'):
        local_df = pd.read_csv(rf'data/factor/{code}.csv', index_col=0)
        local_df.index = pd.to_datetime(local_df.index)
        local_factors = local_df.columns.str.lower()
        unsaved_fields_part1 = list(set(fields_part1) - set(local_factors))
        unsaved_fields_part2 = list(set(fields_part2) - set(local_factors))

        if unsaved_fields_part1 == [] and unsaved_fields_part2 == []:
            continue

    # 获取第一组因子
    part1_options = "ruleType=2;Period=Q;Days=Alldays"
    part1_data = get_part_data(
        code, fields_part1, begin_time, end_time, part1_options)
    if isinstance(part1_data, str):
        continue

    # 获取第二组因子
    part2_options = "ruleType=3;N=3;unit=1;rptType=1;currencyType=;westPeriod=180;Period=Q;Days=Alldays"
    part2_data = get_part_data(
        code, fields_part2, begin_time, end_time, part2_options)
    if isinstance(part2_data, str):
        continue

    # 拼接两组因子和本地数据
    data = pd.concat([local_df, part1_data, part2_data], axis=1)

    # 储存数据文件
    data.to_csv(rf'data/factor/{code}.csv')
    logging.info(f'{code}数据已保存\n')

logging.info('保存完成')
