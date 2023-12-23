# data_processing.py
import pandas as pd
import datetime
import os

def process_data(source_order_path, adjust_order_path, name_list_path, b2b_order_path):
    # 获取脚本开始执行的时间戳
    timestamp = datetime.datetime.now().strftime("%y%m%d%H%M%S")

    # 设置要处理的年月
    year = 2023
    month = 11

    # 数据处理逻辑
    # 例如：读取'source_order_path'文件所需的列
    source_order_cols = ['订单类型', '订单号', '下单时间', '发货时间', '退单时间', '是否退款', '商品编码', '数量', '单品折后总金额', '订单归属店员', '订单归属店员工号', '订单归属门店编码', '订单归属门店名称']
    source_order = pd.read_excel(source_order_path, usecols=source_order_cols)
    # 创建复制列
    source_order['订单归属店员-调整后'] = source_order['订单归属店员']
    source_order['订单归属店员工号-调整后'] = source_order['订单归属店员工号']
    source_order['订单归属门店名称-花名册'] = source_order['订单归属门店名称']
    # 创建'备注','储值卡交易金额','是否对公','是否加盟-1/0'列
    source_order['备注'] = ''
    source_order['储值卡交易金额'] = ''
    source_order['是否对公'] = ''
    source_order['是否加盟-1/0'] = ''
    # 去除订单号中的下划线，并确保数字格式正确
    source_order['订单号'] = source_order['订单号'].str.replace('_', '').astype('int64')
    # 将 '订单号' 转换为字符串，避免科学记数法
    source_order['订单号'] = source_order['订单号'].apply(lambda x: '{:0.0f}'.format(x))
    # 转换'下单时间'、'发货时间'和'退单时间'为仅包含年月日
    source_order['下单时间'] = pd.to_datetime(source_order['下单时间']).dt.date
    source_order['发货时间'] = pd.to_datetime(source_order['发货时间']).dt.date
    source_order['退单时间'] = pd.to_datetime(source_order['退单时间']).dt.date

    # 开始做是否【隔月退款】【有效销售】【无效销售】的检查
    # 为比较创建年月格式的临时列
    source_order['发货时间_年月'] = pd.to_datetime(source_order['发货时间']).dt.to_period('M')
    source_order['退单时间_年月'] = pd.to_datetime(source_order['退单时间']).dt.to_period('M')

    # 将检查日期转换为 Pandas 的 Period 对象，以便进行年和月的比较
    check_date = pd.Period(year=year, month=month, freq='M')

    # 情况1
    condition_1 = source_order['发货时间_年月'].isna()
    source_order.loc[condition_1, ['数量', '单品折后总金额']] = 0

    # 情况2
    condition_2 = source_order['发货时间_年月'].notna() & (source_order['发货时间_年月'] < check_date)
    condition_2_refund = condition_2 & (source_order['退单时间_年月'].notna() & (source_order['退单时间_年月'] >= check_date))
    condition_2_no_refund = condition_2 & (source_order['退单时间_年月'].isna() | (source_order['退单时间_年月'] < check_date))
    source_order.loc[condition_2_refund, ['数量', '单品折后总金额']] *= -1
    source_order.loc[condition_2_no_refund, ['数量', '单品折后总金额']] = 0

    # 情况3
    condition_3 = source_order['发货时间_年月'].notna() & (source_order['发货时间_年月'] == check_date)
    condition_3_refund = condition_3 & source_order['退单时间_年月'].notna()
    source_order.loc[condition_3_refund, ['数量', '单品折后总金额']] = 0

    # 情况4
    condition_out = source_order['发货时间_年月'].notna() & (source_order['发货时间_年月'] > check_date)
    source_order.loc[condition_out, ['数量', '单品折后总金额']] = 0

    # 删除临时年月列
    source_order.drop(['发货时间_年月', '退单时间_年月'], axis=1, inplace=True)

    # 读取'adjust_order_path'文件所需的列，表头保持一致
    adjust_order_cols = ['微商城订单号', '新订单归属人', '新订单归属人工号']
    adjust_order = pd.read_excel(adjust_order_path, usecols=adjust_order_cols)
    # 将 '微商城订单号' 转换为字符串，避免科学记数法
    adjust_order['微商城订单号'] = adjust_order['微商城订单号'].apply(lambda x: '{:0.0f}'.format(x))
    # 确保 '新订单归属人工号' 是字符串类型
    adjust_order['新订单归属人工号'] = adjust_order['新订单归属人工号'].astype(str)
    # 创建一个映射关系，用于更新 '订单归属店员-调整后'
    name_map = adjust_order.set_index('微商城订单号')['新订单归属人']
    source_order['订单归属店员-调整后'] = source_order['订单号'].map(name_map).fillna(source_order['订单归属店员-调整后'])
    # 创建一个映射关系，用于更新 '订单归属店员工号-调整后'
    employee_id_map = adjust_order.set_index('微商城订单号')['新订单归属人工号']
    source_order['订单归属店员工号-调整后'] = source_order['订单号'].map(employee_id_map).fillna(source_order['订单归属店员工号-调整后'])

    # 读取'name_list_path'文件中子表为'202311'所需的列，表头保持一致，并使得'员工工号'为'字符串'
    name_list_cols = ['员工工号', 'NC部门']
    name_list = pd.read_excel(name_list_path, sheet_name='202311', usecols=name_list_cols)
    name_list['员工工号'] = name_list['员工工号'].astype(str)

    # 创建一个映射关系，用于更新 '订单归属门店名称-花名册'
    employee_id_map2 = name_list.set_index('员工工号')['NC部门']
    source_order['订单归属门店名称-花名册'] = source_order['订单归属店员工号-调整后'].map(employee_id_map2).fillna(source_order['订单归属门店名称-花名册'])

    # 读取'b2b_order_path'文件所需的列，表头保持一致
    B2B_order_cols = ['微商城订单号']
    B2B_order = pd.read_excel(b2b_order_path, usecols=B2B_order_cols)
    # 在B2B_order中添加一个标记列
    B2B_order['匹配标记'] = 1
    # 删除B2B_order中微商城订单号的重复值
    B2B_order = B2B_order.drop_duplicates(subset='微商城订单号', keep='first')
    # 确保 '微商城订单号' 是字符串类型
    B2B_order['微商城订单号'] = B2B_order['微商城订单号'].astype(str)
    # 创建映射关系
    order_match_map = B2B_order.set_index('微商城订单号')['匹配标记']
    # 更新source_order
    source_order['是否对公'] = source_order['订单号'].map(order_match_map).fillna(source_order['是否对公'])
    # 保存处理后的数据
    output_filename = os.path.expanduser(f'~/Desktop/work_{timestamp}.xlsx')
    source_order.to_excel(output_filename, index=False)

    # 返回生成的文件名
    return output_filename
