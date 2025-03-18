import pandas as pd
import akshare as ak
import time
import json
import os
import numpy as np

def check_data_quality(df, column):
    """检查数据列的质量"""
    if column not in df.columns:
        return False
    null_count = df[column].isnull().sum()
    total_count = len(df)
    return bool(null_count == 0)  # 转换为 Python 原生布尔值

def process_financial_data(stock_codes):
    # 创建空列表存储所有公司的数据
    all_companies_data = []
    
    for code in stock_codes:
        if code.startswith('6'):
            symbol = f"SH{code}"
        else:
            symbol = f"SZ{code}"
            
        try:
            # 获取财务数据
            profit_yearly = ak.stock_profit_sheet_by_yearly_em(symbol=symbol)
            balance_yearly = ak.stock_balance_sheet_by_yearly_em(symbol=symbol)
            cashflow_yearly = ak.stock_cash_flow_sheet_by_yearly_em(symbol=symbol)
            
            # 定义要提取的指标及其中文名称
            metrics = {
                'profit_yearly': {
                    'TOTAL_OPERATE_INCOME': '营业收入',
                    'TOTAL_OPERATE_COST': '营业成本',
                    'OPERATE_COST': '主营业务成本',
                    'RESEARCH_EXPENSE': '研发费用',
                    'OPERATE_TAX_ADD': '附加税',
                    'SALE_EXPENSE': '销售费用',
                    'MANAGE_EXPENSE': '管理费用',
                    'FINANCE_EXPENSE': '财务费用',
                    'FAIRVALUE_CHANGE_INCOME': '公允价值变动损益',
                    'INVEST_INCOME': '投资收益',
                    'ASSET_IMPAIRMENT_INCOME': '资产减值损益',
                    'CREDIT_IMPAIRMENT_INCOME': '信用减值损益',
                    'OTHER_INCOME': '其他收入',
                    'OPERATE_PROFIT': '营业利润',
                    'NONBUSINESS_INCOME': '营业外收入',
                    'NONBUSINESS_EXPENSE': '营业外支出',
                    'TOTAL_PROFIT': '税前利润',
                    'INCOME_TAX': '所得税',
                    'NETPROFIT': '净利润',
                    'CONTINUED_NETPROFIT': '经常性净利润',
                    'PARENT_NETPROFIT': '归属母公司净利润',
                    'MINORITY_INTEREST': '少数股东损益'
                },
                'balance_yearly': {
                    'NOTE_ACCOUNTS_RECE': '应收票据及应收账款',
                    'ACCOUNTS_RECE': '应收账款',
                    'NOTE_RECE': '应收票据',
                    'PREPAYMENT': '预付账款',
                    'INVENTORY': '存货',
                    'TOTAL_OTHER_RECE': '其他应收款',
                    'TOTAL_CURRENT_ASSETS': '流动资产',
                    'FIXED_ASSET': '固定资产',
                    'INTANGIBLE_ASSET': '无形资产',
                    'LONG_PREPAID_EXPENSE': '长期待摊费用',
                    'TOTAL_ASSETS': '总资产',                    
                    'SHORT_LOAN': '短期借款',
                    'NOTE_ACCOUNTS_PAYABLE': '应付票据及应付账款',
                    'ACCOUNTS_PAYABLE': '应付账款',
                    'NOTE_PAYABLE': '应付票据',
                    'STAFF_SALARY_PAYABLE': '应付职工薪酬',
                    'TAX_PAYABLE': '应付税费',
                    'CONTRACT_LIAB': '合同负债',
                    'ADVANCE_RECEIVABLES': '预收账款',
                    'TOTAL_OTHER_PAYABLE': '其他应付款',
                    'TOTAL_CURRENT_LIAB': '流动负债',
                    'LEASE_LIAB': '租赁负债',
                    'LONG_LOAN': '长期借款',
                    'LONG_PAYABLE': '长期应付款',
                    'TOTAL_LIABILITIES': '总负债',
                    'SHARE_CAPITAL': '实收资本',
                    'MINORITY_EQUITY': '少数股东权益',
                    'TOTAL_EQUITY': '所有者权益'
                },
                'cashflow_yearly': {
                    'SALES_SERVICES': '销售收款',
                    'RECEIVE_OTHER_OPERATE': '其他经营性流入',
                    'TOTAL_OPERATE_INFLOW': '经营活动现金流入总额',
                    'BUY_SERVICES': '采购支出',
                    'PAY_STAFF_CASH': '人工支出',
                    'PAY_ALL_TAX': '税费支出',
                    'PAY_OTHER_OPERATE': '其他经营性流出',
                    'TOTAL_OPERATE_OUTFLOW': '经营活动现金流出总额',
                    'NETCASH_OPERATE': '经营活动现金流量净额',
                    'CONSTRUCT_LONG_ASSET': '固定资产支出',
                    'INVEST_PAY_CASH': '投资支出',
                    'TOTAL_INVEST_OUTFLOW': '投资活动现金流出总额',
                    'ACCEPT_INVEST_CASH': '吸收投资流入',
                    'NETCASH_INVEST': '投资活动现金流量净额',
                    'RECEIVE_LOAN_CASH': '借款流入',
                    'TOTAL_FINANCE_INFLOW': '筹资活动现金流入总额',
                    'PAY_DEBT_CASH': '还款流出',
                    'ASSIGN_DIVIDEND_PORFIT': '分红支出',
                    'TOTAL_FINANCE_OUTFLOW': '筹资活动现金流出总额',
                    'NETCASH_FINANCE': '筹资活动现金流量净额',
                    'CCE_ADD': '现金增加',
                    'END_CCE': '期末现金',
                    'INVENTORY_REDUCE': '存货余额减少',
                    'OPERATE_RECE_REDUCE': '应收余额减少',
                    'OPERATE_PAYABLE_ADD': '应付余额增加'
                }
            }
            
            # 处理利润表数据
            for _, row in profit_yearly.iterrows():
                company_name = row['SECURITY_NAME_ABBR']
                report_date = row['REPORT_DATE']
                for en_name, cn_name in metrics['profit_yearly'].items():
                    if pd.notna(row[en_name]):  # 只添加非空值
                        all_companies_data.append({
                            '股票代码': code,
                            '公司名称': company_name,
                            '报表': 'profit_yearly',
                            '项目': cn_name,
                            '报告期': report_date,
                            '金额': row[en_name]
                        })
            
            # 处理资产负债表数据
            for _, row in balance_yearly.iterrows():
                company_name = row['SECURITY_NAME_ABBR']
                report_date = row['REPORT_DATE']
                
                # 特殊处理合同负债和预收账款的合并
                contract_liab = row['CONTRACT_LIAB'] if pd.notna(row['CONTRACT_LIAB']) else 0
                advance_rec = row['ADVANCE_RECEIVABLES'] if pd.notna(row['ADVANCE_RECEIVABLES']) else 0
                combined_value = contract_liab + advance_rec
                
                # 如果合并后的值不为0，添加到数据中
                if combined_value != 0:
                    all_companies_data.append({
                        '股票代码': code,
                        '公司名称': company_name,
                        '报表': 'balance_yearly',
                        '项目': '合同负债',
                        '报告期': report_date,
                        '金额': combined_value
                    })
                
                # 处理其他资产负债表项目
                for en_name, cn_name in metrics['balance_yearly'].items():
                    # 跳过合同负债和预收账款的单独处理
                    if en_name in ['CONTRACT_LIAB', 'ADVANCE_RECEIVABLES']:
                        continue
                    if pd.notna(row[en_name]):  # 只添加非空值
                        all_companies_data.append({
                            '股票代码': code,
                            '公司名称': company_name,
                            '报表': 'balance_yearly',
                            '项目': cn_name,
                            '报告期': report_date,
                            '金额': row[en_name]
                        })
            
            # 处理现金流量表数据
            for _, row in cashflow_yearly.iterrows():
                company_name = row['SECURITY_NAME_ABBR']
                report_date = row['REPORT_DATE']
                for en_name, cn_name in metrics['cashflow_yearly'].items():
                    if pd.notna(row[en_name]):  # 只添加非空值
                        all_companies_data.append({
                            '股票代码': code,
                            '公司名称': company_name,
                            '报表': 'cashflow_yearly',
                            '项目': cn_name,
                            '报告期': report_date,
                            '金额': row[en_name]
                        })
            
            print(f"已处理：{company_name}")
            
        except Exception as e:
            print(f"处理出错：{code}")
        
        time.sleep(2)
    
    # 将所有公司数据转换为DataFrame
    df = pd.DataFrame(all_companies_data)
    
    # 按公司名称、报表类型和报告期排序（报告期改为升序）
    df = df.sort_values(['公司名称', '报表', '报告期'], ascending=[True, True, True])
    
    # 保存合并后的CSV文件
    merged_csv_path = 'data/merged_financial_data.csv'
    df.to_csv(merged_csv_path, index=False, encoding='utf-8-sig')
    
    # 生成用于网页图表的JSON数据
    json_data = {
        "companies": {}
    }
    
    # 按公司分组处理数据
    for company_code in df['股票代码'].unique():
        company_df = df[df['股票代码'] == company_code]
        company_name = company_df['公司名称'].iloc[0]
        
        json_data["companies"][company_code] = {
            "company_name": company_name,
            "stock_code": company_code,
            "profit_sheet": {
                "years": [],
                "metrics": {}
            },
            "balance_sheet": {
                "years": [],
                "metrics": {}
            },
            "cash_flow": {
                "years": [],
                "metrics": {}
            }
        }
        
        # 获取该公司所有报告期并按时间顺序排序（从早到晚）
        report_dates = sorted(company_df['报告期'].unique())
        years = [str(date).split()[0] for date in report_dates]
        
        # 为所有报表类型设置相同的年份数据
        json_data["companies"][company_code]["profit_sheet"]["years"] = years
        json_data["companies"][company_code]["balance_sheet"]["years"] = years
        json_data["companies"][company_code]["cash_flow"]["years"] = years
        
        # 处理每种报表类型的数据
        report_type_map = {
            'profit_yearly': 'profit_sheet',
            'balance_yearly': 'balance_sheet',
            'cashflow_yearly': 'cash_flow'
        }
        
        for report_type, json_key in report_type_map.items():
            report_df = company_df[company_df['报表'] == report_type]
            
            # 处理每个指标的数据
            for metric in report_df['项目'].unique():
                metric_data = []
                for date in report_dates:  # 使用按时间顺序排序的日期
                    value = report_df[(report_df['项目'] == metric) & 
                           (report_df['报告期'] == date)]['金额'].values
                    if len(value) > 0:
                        metric_data.append(float(value[0]))
                    else:
                        metric_data.append(None)
                
                json_data["companies"][company_code][json_key]["metrics"][metric] = metric_data
        
        # 计算毛利润和毛利率
        if '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '营业成本' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            cost = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业成本']
            gross_profit = []
            gross_margin = []
            for i in range(len(revenue)):
                if revenue[i] is not None and cost[i] is not None:
                    profit = revenue[i] - cost[i]
                    gross_profit.append(profit)
                    if revenue[i] != 0:
                        margin = (profit / revenue[i]) * 100
                        gross_margin.append(round(margin, 2))
                    else:
                        gross_margin.append(None)
                else:
                    gross_profit.append(None)
                    gross_margin.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['毛利润'] = gross_profit
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['毛利率'] = gross_margin
        
        # 计算净利润率
        if '净利润' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            net_profit = json_data["companies"][company_code]["profit_sheet"]["metrics"]['净利润']
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            net_margin = []
            for i in range(len(net_profit)):
                if net_profit[i] is not None and revenue[i] is not None and revenue[i] != 0:
                    margin = (net_profit[i] / revenue[i]) * 100
                    net_margin.append(round(margin, 2))
                else:
                    net_margin.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['净利润率'] = net_margin
        
        # 计算净利润增长率
        if '净利润' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            net_profit = json_data["companies"][company_code]["profit_sheet"]["metrics"]['净利润']
            growth_rates = []
            for i in range(len(net_profit)):
                if i > 0 and net_profit[i] is not None and net_profit[i-1] is not None and net_profit[i-1] != 0:
                    growth_rate = (net_profit[i] - net_profit[i-1]) / net_profit[i-1] * 100
                    growth_rates.append(round(growth_rate, 2))
                else:
                    growth_rates.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['净利润增长率'] = growth_rates
        
        # 计算营业收入增长率
        if '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            revenue_growth_rates = []
            for i in range(len(revenue)):
                if i > 0 and revenue[i] is not None and revenue[i-1] is not None and revenue[i-1] != 0:
                    growth_rate = (revenue[i] - revenue[i-1]) / revenue[i-1] * 100
                    revenue_growth_rates.append(round(growth_rate, 2))
                else:
                    revenue_growth_rates.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入增长率'] = revenue_growth_rates
        
        # 计算销售费用占营业收入比例
        if '销售费用' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            sales_expense = json_data["companies"][company_code]["profit_sheet"]["metrics"]['销售费用']
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            sales_expense_ratio = []
            for i in range(len(sales_expense)):
                if sales_expense[i] is not None and revenue[i] is not None and revenue[i] != 0:
                    ratio = (sales_expense[i] / revenue[i]) * 100
                    sales_expense_ratio.append(round(ratio, 2))
                else:
                    sales_expense_ratio.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['销售费用占比'] = sales_expense_ratio
        
        # 计算管理费用占营业收入比例
        if '管理费用' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            admin_expense = json_data["companies"][company_code]["profit_sheet"]["metrics"]['管理费用']
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            admin_expense_ratio = []
            for i in range(len(admin_expense)):
                if admin_expense[i] is not None and revenue[i] is not None and revenue[i] != 0:
                    ratio = (admin_expense[i] / revenue[i]) * 100
                    admin_expense_ratio.append(round(ratio, 2))
                else:
                    admin_expense_ratio.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['管理费用占比'] = admin_expense_ratio
        
        # 计算研发费用占营业收入比例
        if '研发费用' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            rd_expense = json_data["companies"][company_code]["profit_sheet"]["metrics"]['研发费用']
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            rd_expense_ratio = []
            for i in range(len(rd_expense)):
                if rd_expense[i] is not None and revenue[i] is not None and revenue[i] != 0:
                    ratio = (rd_expense[i] / revenue[i]) * 100
                    rd_expense_ratio.append(round(ratio, 2))
                else:
                    rd_expense_ratio.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['研发费用占比'] = rd_expense_ratio
        
        # 计算资产负债率
        if '总资产' in json_data["companies"][company_code]["balance_sheet"]["metrics"] and \
           '总负债' in json_data["companies"][company_code]["balance_sheet"]["metrics"]:
            total_assets = json_data["companies"][company_code]["balance_sheet"]["metrics"]['总资产']
            total_liabilities = json_data["companies"][company_code]["balance_sheet"]["metrics"]['总负债']
            debt_to_assets_ratio = []
            for i in range(len(total_assets)):
                if total_assets[i] is not None and total_liabilities[i] is not None and total_assets[i] != 0:
                    ratio = (total_liabilities[i] / total_assets[i]) * 100
                    debt_to_assets_ratio.append(round(ratio, 2))
                else:
                    debt_to_assets_ratio.append(None)
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['资产负债率'] = debt_to_assets_ratio
        
        # 计算应收账款及应收票据占营业收入比例
        if ('应收票据及应收账款' in json_data["companies"][company_code]["balance_sheet"]["metrics"] or \
            ('应收账款' in json_data["companies"][company_code]["balance_sheet"]["metrics"] and \
             '应收票据' in json_data["companies"][company_code]["balance_sheet"]["metrics"])) and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            
            # 获取应收账款及应收票据总额
            if '应收票据及应收账款' in json_data["companies"][company_code]["balance_sheet"]["metrics"]:
                accounts_receivable = json_data["companies"][company_code]["balance_sheet"]["metrics"]['应收票据及应收账款']
            else:
                accounts = json_data["companies"][company_code]["balance_sheet"]["metrics"]['应收账款']
                notes = json_data["companies"][company_code]["balance_sheet"]["metrics"]['应收票据']
                accounts_receivable = []
                for i in range(len(accounts)):
                    acc = accounts[i] if accounts[i] is not None else 0
                    note = notes[i] if i < len(notes) and notes[i] is not None else 0
                    accounts_receivable.append(acc + note)
            
            # 如果没有应收账款及应收票据数据，则跳过计算
            if len(accounts_receivable) == 0:
                continue
                
            # 计算应收账款及应收票据占营业收入比例
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            receivables_ratio = []
            for i in range(len(accounts_receivable)):
                if i < len(revenue) and accounts_receivable[i] is not None and revenue[i] is not None and revenue[i] != 0:
                    ratio = (accounts_receivable[i] / revenue[i]) * 100
                    receivables_ratio.append(round(ratio, 2))
                else:
                    receivables_ratio.append(None)
            
            # 保存计算结果
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['应收账款及应收票据'] = accounts_receivable
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['应收账款及应收票据占比'] = receivables_ratio
        
        # 计算存货占营业成本比例
        if '存货' in json_data["companies"][company_code]["balance_sheet"]["metrics"] and \
           '营业成本' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            inventory = json_data["companies"][company_code]["balance_sheet"]["metrics"]['存货']
            cost = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业成本']
            inventory_ratio = []
            for i in range(len(inventory)):
                if i < len(cost) and inventory[i] is not None and cost[i] is not None and cost[i] != 0:
                    ratio = (inventory[i] / cost[i]) * 100
                    inventory_ratio.append(round(ratio, 2))
                else:
                    inventory_ratio.append(None)
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['存货占营业成本比'] = inventory_ratio
        
        # 计算应付账款及应付票据占营业成本比例
        if ('应付票据及应付账款' in json_data["companies"][company_code]["balance_sheet"]["metrics"] or \
            ('应付账款' in json_data["companies"][company_code]["balance_sheet"]["metrics"] and \
             '应付票据' in json_data["companies"][company_code]["balance_sheet"]["metrics"])) and \
           '营业成本' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            
            # 获取应付账款及应付票据总额
            if '应付票据及应付账款' in json_data["companies"][company_code]["balance_sheet"]["metrics"]:
                accounts_payable = json_data["companies"][company_code]["balance_sheet"]["metrics"]['应付票据及应付账款']
            else:
                accounts = json_data["companies"][company_code]["balance_sheet"]["metrics"]['应付账款']
                notes = json_data["companies"][company_code]["balance_sheet"]["metrics"]['应付票据']
                accounts_payable = []
                for i in range(len(accounts)):
                    acc = accounts[i] if accounts[i] is not None else 0
                    note = notes[i] if i < len(notes) and notes[i] is not None else 0
                    accounts_payable.append(acc + note)
            
            # 如果没有应付账款及应付票据数据，则跳过计算
            if len(accounts_payable) == 0:
                continue
                
            # 计算应付账款及应付票据占营业成本比例
            cost = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业成本']
            payables_ratio = []
            for i in range(len(accounts_payable)):
                if i < len(cost) and accounts_payable[i] is not None and cost[i] is not None and cost[i] != 0:
                    ratio = (accounts_payable[i] / cost[i]) * 100
                    payables_ratio.append(round(ratio, 2))
                else:
                    payables_ratio.append(None)
            
            # 保存计算结果
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['应付账款及应付票据'] = accounts_payable
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['应付账款及应付票据占比'] = payables_ratio
        
        # 计算并添加合同负债占营业收入的比例
        if '合同负债' in json_data["companies"][company_code]["balance_sheet"]["metrics"] and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            liabilities = json_data["companies"][company_code]["balance_sheet"]["metrics"]['合同负债']
            income = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            liability_ratios = []
            for i in range(len(liabilities)):
                if liabilities[i] is not None and income[i] is not None and income[i] != 0:
                    ratio = (liabilities[i] / income[i] * 100)
                    liability_ratios.append(round(ratio, 2))
                else:
                    liability_ratios.append(None)
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['合同负债占比'] = liability_ratios
        
        # 计算应付账款及应付票据占(营业成本+存货)比
        if '应付账款及应付票据' in json_data["companies"][company_code]["balance_sheet"]["metrics"] and \
           '营业成本' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '存货' in json_data["companies"][company_code]["balance_sheet"]["metrics"]:
            
            payables = json_data["companies"][company_code]["balance_sheet"]["metrics"]['应付账款及应付票据']
            cost = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业成本']
            inventory = json_data["companies"][company_code]["balance_sheet"]["metrics"]['存货']
            
            payables_to_cost_inventory_ratio = []
            for i in range(len(payables)):
                if payables[i] is not None and cost[i] is not None and inventory[i] is not None:
                    denominator = cost[i] + inventory[i]
                    if denominator != 0:
                        ratio = (payables[i] / denominator) * 100
                        payables_to_cost_inventory_ratio.append(round(ratio, 2))
                    else:
                        payables_to_cost_inventory_ratio.append(None)
                else:
                    payables_to_cost_inventory_ratio.append(None)
            
            json_data["companies"][company_code]["balance_sheet"]["metrics"]['应付账款占营业成本与存货比'] = payables_to_cost_inventory_ratio
        
        # 计算经营活动现金流量净额增长率
        if '经营活动现金流量净额' in json_data["companies"][company_code]["cash_flow"]["metrics"]:
            operating_cash_flow = json_data["companies"][company_code]["cash_flow"]["metrics"]['经营活动现金流量净额']
            growth_rates = []
            for i in range(len(operating_cash_flow)):
                if i > 0 and operating_cash_flow[i] is not None and operating_cash_flow[i-1] is not None and operating_cash_flow[i-1] != 0:
                    growth_rate = (operating_cash_flow[i] - operating_cash_flow[i-1]) / abs(operating_cash_flow[i-1]) * 100
                    growth_rates.append(round(growth_rate, 2))
                else:
                    growth_rates.append(None)
            json_data["companies"][company_code]["cash_flow"]["metrics"]['经营活动现金流量净额增长率'] = growth_rates

        # 计算销售现金率（销售收款/营业收入）
        if '销售收款' in json_data["companies"][company_code]["cash_flow"]["metrics"] and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            sales_cash = json_data["companies"][company_code]["cash_flow"]["metrics"]['销售收款']
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            sales_cash_ratio = []
            for i in range(len(sales_cash)):
                if sales_cash[i] is not None and revenue[i] is not None and revenue[i] != 0:
                    ratio = (sales_cash[i] / revenue[i]) * 100
                    sales_cash_ratio.append(round(ratio, 2))
                else:
                    sales_cash_ratio.append(None)
            json_data["companies"][company_code]["cash_flow"]["metrics"]['销售现金率'] = sales_cash_ratio

        # 计算经营现金收入比（经营活动现金流量净额/营业收入）
        if '经营活动现金流量净额' in json_data["companies"][company_code]["cash_flow"]["metrics"] and \
           '营业收入' in json_data["companies"][company_code]["profit_sheet"]["metrics"]:
            operating_cash_flow = json_data["companies"][company_code]["cash_flow"]["metrics"]['经营活动现金流量净额']
            revenue = json_data["companies"][company_code]["profit_sheet"]["metrics"]['营业收入']
            operating_cash_ratio = []
            for i in range(len(operating_cash_flow)):
                if operating_cash_flow[i] is not None and revenue[i] is not None and revenue[i] != 0:
                    ratio = (operating_cash_flow[i] / revenue[i]) * 100
                    operating_cash_ratio.append(round(ratio, 2))
                else:
                    operating_cash_ratio.append(None)
            json_data["companies"][company_code]["cash_flow"]["metrics"]['经营现金收入比'] = operating_cash_ratio

        # 计算净资产收益率（ROE）
        if '净利润' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '所有者权益' in json_data["companies"][company_code]["balance_sheet"]["metrics"]:
            net_profit = json_data["companies"][company_code]["profit_sheet"]["metrics"]['净利润']
            equity = json_data["companies"][company_code]["balance_sheet"]["metrics"]['所有者权益']
            roe = []
            for i in range(len(net_profit)):
                if net_profit[i] is not None and equity[i] is not None and equity[i] != 0:
                    ratio = (net_profit[i] / equity[i]) * 100
                    roe.append(round(ratio, 2))
                else:
                    roe.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['净资产收益率'] = roe

        # 计算总资产收益率（ROA）
        if '净利润' in json_data["companies"][company_code]["profit_sheet"]["metrics"] and \
           '总资产' in json_data["companies"][company_code]["balance_sheet"]["metrics"]:
            net_profit = json_data["companies"][company_code]["profit_sheet"]["metrics"]['净利润']
            total_assets = json_data["companies"][company_code]["balance_sheet"]["metrics"]['总资产']
            roa = []
            for i in range(len(net_profit)):
                if net_profit[i] is not None and total_assets[i] is not None and total_assets[i] != 0:
                    ratio = (net_profit[i] / total_assets[i]) * 100
                    roa.append(round(ratio, 2))
                else:
                    roa.append(None)
            json_data["companies"][company_code]["profit_sheet"]["metrics"]['总资产收益率'] = roa

    # 保存JSON文件
    merged_json_path = 'data/merged_financial_data.json'
    with open(merged_json_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)
    
    print(f"\n数据文件已生成完成")
    
    return df  # 返回DataFrame以便进行后续分析

if __name__ == "__main__":
    # 确保data目录存在
    if not os.path.exists('data'):
        os.makedirs('data')
        
    stock_codes = ['688596', '603690']  # 正帆科技在前，至纯科技在后
    process_financial_data(stock_codes)



