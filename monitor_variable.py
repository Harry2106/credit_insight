# %%
import sys
sys.path.append("c:\\Users\\truongtanhoa\\Desktop\\utils")
from credit_insight import *
import csv
import logging
import cx_Oracle
import pandas as pd
import numpy as np
from datetime import date
import xlsxwriter

fi = FeatureInsight()
mi = ModelInsight()
# %%
loan_feat = ['CR_DPDLOAN_PAYSUMC6', 'NOT_CONTACT_MANUAL_C2', 'CR_DPDC7C9_PREPAYC6', 'DPD_MAX_C1', 'N_PAY_MIN_C3', 'CR_CLOSE_MAXDELAY', 'DPD_MAX_C10_C12', 'TERM', 'OUTSTANDING_AMT', 'MAX_DPD_CURR', 'N_APP_C12', 'N_REJECT_C3', 'NUM_NOTGRANT_LOAN']
card_feat = [ 'C_DELIQ_5C6_PAYMIN9', 'C_NMANUAL3_PAYNO3', 'OVD3_COUNT_C1', 'TOTAL_PAYMENT_C1', 'MIN_MOB_OBS', 'DPD_MAX_C12', 'MAX_DEBT_CIC_C6', 'MAX_DELAY_ALL', 'NUM_NOTGRANT_LOAN', 'CARD_LIMIT', 'COOPERATIONFE_TIME', 'N_REJECT_C9', 'N_APP_C3', 'NO_UPL']
# %%
# Init dataframe-to-excel object
writer = pd.ExcelWriter('test.xlsx', engine= 'xlsxwriter')
# Init workbook object
workbook = writer.book
# Init worksheet object named var_monitoring
sheet_target = 'VAR_MONITORING'
worksheet = workbook.add_worksheet(sheet_target)

origin = 0
hrz_interval = 10
vtc_interval = 3

# %%
# START
for feat in loan_feat:

    connection = cx_Oracle.connect(user = "hoatruong4[RISK_MDL]", password = "Chuheobietdi2001@",  dsn = "EDW_X9")
    cursor = connection.cursor()
    sql_command = f'''
        select  /*+parallel(128)*/ 
                OBS_MONTH
                ,{feat}
                ,COUNT(DISTINCT appl_id) as N_APPS
                ,SUM(base_fe_dpd3) as DPD3_BASE
                ,SUM(target_fe_dpd3) as DPD3
            from PREDUE_HT_MONITORING_VAR
            where segment = 'LOAN'
            group by OBS_MONTH, {feat}
            order by 1,2
        '''  
    temp = pd.read_sql_query(sql_command, con = connection)

    dist = fi.distribution_over_time(temp, feat, 'N_APPS', 'OBS_MONTH')

    var_dist_no = dist.pivot(columns = feat, index = 'OBS_MONTH',values ='N_APPS')
    var_dist_pct = dist.pivot(columns = feat, index = 'OBS_MONTH',values ='%N_APPS')

    default_no = fi.default_rate_over_time(temp, feat, 'DPD3', 'DPD3_BASE', 'OBS_MONTH').\
                pivot(columns = feat, index = 'OBS_MONTH',values ='DPD3')
    default_pct = fi.default_rate_over_time(temp, feat, 'DPD3', 'DPD3_BASE', 'OBS_MONTH').\
                pivot(columns = feat, index = 'OBS_MONTH',values ='%DEFAULT')
    
    woe = fi.woe_iv_over_time(temp, feat, 'DPD3', 'DPD3_BASE', 'OBS_MONTH', None, False).\
                pivot(columns = feat, index = 'OBS_MONTH', values = 'WOE')

    iv = fi.woe_iv_over_time(temp, feat, 'DPD3', 'DPD3_BASE', 'OBS_MONTH', None, False).\
                pivot(columns = feat, index = 'OBS_MONTH', values = 'IV')
    
    iv_per_month = fi.woe_iv_over_time(temp, feat, 'DPD3', 'DPD3_BASE', 'OBS_MONTH', None, True).\
                    set_index('OBS_MONTH')


    max_row, max_col = var_dist_no.shape
    ################ VAR DIST %
    
    x,y = (origin, 0)
    var_dist_pct.to_excel(writer, sheet_name=sheet_target, startrow=x, startcol=y)
    chart = workbook.add_chart({'type' : 'column', 'subtype' : 'percent_stacked'})

    for i in range(y+1, y+max_col+1):
        chart.add_series({
                    'name': [sheet_target, x, i], 
                    'categories': [sheet_target, x+1, y, x+max_row, y],
                    'values' : [sheet_target, x+1, i, x+max_row, i],
                    'gap' : 2
                    })
        
    chart.set_x_axis({'name': 'OBS_MONTH'})
    chart.set_y_axis({'name': '%'})
    chart.set_title({'name': '% ' + feat, 'name_font': {'size': 14}})

    worksheet.insert_chart(x, y + 7, chart)

    ################ VAR DIST NO
    x,y = (origin + max_row + vtc_interval, 0)
    var_dist_no.to_excel(writer, sheet_name=sheet_target, startrow=x, startcol=y)

    ################ DEFAULT %
    x,y = (origin, 6+hrz_interval) #origin + max_col + hrz_interval
    default_pct.to_excel(writer, sheet_name=sheet_target, startrow=x, startcol=y)

    chart = workbook.add_chart({'type' : 'line'})

    for i in range(y+1, y+max_col+1):
        chart.add_series({
                    'name': [sheet_target, x, i], 
                    'categories': [sheet_target, x+1, y, x+max_row, y],
                    'values' : [sheet_target, x+1, i, x+max_row, i],
                    'gap' : 2
                    })
    chart.set_x_axis({'name': 'OBS_MONTH'})
    chart.set_y_axis({'name': '%DPD3'})
    chart.set_title({'name': '%DPD3 ' + feat, 'name_font': {'size': 14}})

    worksheet.insert_chart(x, y + 7, chart)

    ################ DEFAULT #
    x,y = (origin + max_row + vtc_interval, 6+hrz_interval)
    default_no.to_excel(writer, sheet_name=sheet_target, startrow=x, startcol=y)


    ################# WOE % 6 is the default column of first table
    x,y = (origin, 2*(6+hrz_interval))
    woe.to_excel(writer, sheet_name=sheet_target, startrow = x, startcol=y)

    chart = workbook.add_chart({'type' : 'line'})
    for i in range(1, max_row+1):
        chart.add_series({
                            'name': [sheet_target, x+i, y],
                            'categories': [sheet_target, x, y+1 , x, y+max_col],
                            'values' : [sheet_target, x+i, y+1, x+i, y+max_col],
                            'gap' : 2
                            })
    
    chart.set_x_axis({'name': 'OBS_MONTH'})
    chart.set_y_axis({'name': 'WOE'})
    chart.set_title({'name': 'WOE of ' + feat, 'name_font': {'size': 14}})

    worksheet.insert_chart(x, y + 7, chart)

    ################# IV per bin
    x,y = (origin, 3*(6+hrz_interval))
    iv.to_excel(writer, sheet_name=sheet_target, startrow = x, startcol=y)
    
    chart = workbook.add_chart({'type' : 'line'})
    
    for i in range(y+1, y+max_col+1):
        chart.add_series({
                    'name': [sheet_target, x, i], 
                    'categories': [sheet_target, x+1, y, x+max_row, y],
                    'values' : [sheet_target, x+1, i, x+max_row, i],
                    'gap' : 2
                    })

    chart.set_x_axis({'name': 'OBS_MONTH'})
    chart.set_y_axis({'name': 'IV'})
    chart.set_title({'name': 'IV of ' + feat, 'name_font': {'size': 14}})

    worksheet.insert_chart(x, y + 7, chart)

    ################# IV per month
    x,y = (origin, 4*(6+hrz_interval))
    iv_per_month.to_excel(writer, sheet_name=sheet_target, startrow = x, startcol=y)
    
    chart = workbook.add_chart({'type' : 'line'})
    
    for i in range(y+1, y+2):
        chart.add_series({
                    'name': [sheet_target, x, i], 
                    'categories': [sheet_target, x+1, y, x+max_row, y],
                    'values' : [sheet_target, x+1, i, x+max_row, i],
                    'gap' : 2
                    })

    chart.set_x_axis({'name': 'OBS_MONTH'})
    chart.set_y_axis({'name': 'IV TOTAL'})
    chart.set_title({'name': 'IV TOTAL of ' + feat, 'name_font': {'size': 14}})

    worksheet.insert_chart(x, y + 7, chart)

    ################# PSI        
    origin += 20
writer.save()    
workbook.close()


# %%
# Code them IV 
# Code them PSI

# %%
# for var in list_var_loan:  
#     # sql_command = '''
#     #     select 
#     #             CREATE_MONTH AS APP_MONTH, {var}, count(distinct t.APP_ID) n_apps
#     #             ,sum(t.fpd30) as fpd30, sum(t.fpd30_base) as fpd30_base
#     #             ,SUM(DEL30_MOB4_APP) AS DEL30_MOB4_APP,SUM(MOB4_BASE) AS MOB4_BASE
#     #     from HD_PCB_SCORE_MONITOR_BIN_ALL_FN_2 t
#     #     WHERE CREATE_MONTH <'2024/01'
#     #     group by CREATE_MONTH, {var}
#     #     ORDER BY CREATE_MONTH, {var}
#     # '''
    
#     sql_command = f'''
#         select  /*+parallel(128)*/ 
#               OBS_MONTH
#               ,{var}
#               ,COUNT(DISTINCT appl_id) as N_APPS
#               ,SUM(base_fe_dpd3) as DPD3_BASE
#               ,SUM(target_fe_dpd3) as DPD3
#         from PREDUE_HT_MONITORING_VAR
#         where segment = 'LOAN'
#         group by OBS_MONTH, {var}
#         order by 1,2
#     '''  
#     temp = pd.read_sql_query(sql_command, con = connection)
#     temp["VAR_NAME"] = var
#     print(var)
    
#     temp = temp.rename(columns = {1:'VAR_BIN'})
    
#     var_dist = dist_over_time(temp,var,'OBS_MONTH','N_APPS')
#     #var_dist_loan = dist_over_time(temp,var,'APP_MONTH','FPD30_BASE')
#     var_default = default_over_time(temp,var, 'OBS_MONTH','DPD3','DPD3_BASE')
    
#     var_dist.to_excel(writer, sheet_name='ALL', startrow=r)
#     var_default.to_excel(writer, sheet_name='ALL', startrow=r, startcol=startcol)
#     #var_default.to_excel(writer, sheet_name='ALL', startrow=r, startcol=startcol*2)

#     wks1 = writer.sheets['ALL']    
#     chart = workbook.add_chart({'type': 'column', 'subtype': 'percent_stacked'})
#     chart.set_title({'name': 'Lead Dist of ' + var.replace('_BIN', ''), 'name_font': {'size': 14}})
    
#     for i in range(1, len(var_dist.columns) + 1):
#         chart.add_series({
#             'name':       ['ALL', r, i],
#             'categories': ['ALL', r+1, 0, len(var_dist)+r, 0],
#             'values':     ['ALL', r+1, i, len(var_dist)+r, i],
#             'gap':        2,
#         })
    
#     chart.set_x_axis({'name': 'Month'})
#     chart.set_y_axis({'name': '%Sample'})
#     wks1.insert_chart(r, 0, chart)

#     chart3 = workbook.add_chart({'type': 'line'})
#     chart3.set_title({'name': 'DPD3 by ' +  var.replace('_BIN', ''), 'name_font': {'size': 14}})
#     for i in range(1, len(var_default.columns) + 1):
#         chart3.add_series({
#             'name':       ['ALL', r, i+startcol],
#             'categories': ['ALL', r+1, startcol, len(var_default)+r, startcol],
#             'values':     ['ALL', r+1, i+startcol, len(var_default)+r, i+startcol],
#             'gap':        2,
#         })
#     chart3.set_x_axis({'name': 'Month'})
#     chart3.set_y_axis({'name': '%Sample'})
#     wks1.insert_chart(r, 9, chart3)
    
#     r = r+ var_dist.shape[0]+10

# writer.save()    
# workbook.close()
# %%
# chart.add_series({
#                 'name': [sheet_target, start, start+1], 
#                 'categories': [sheet_target, start+1, 0, start+max_row, 0],
#                 'values' : [sheet_target, start+1, 1, start+max_row, 1],
#                 'gap' : 2
#                 })

# chart.add_series({
#                 'name': [sheet_target, start, start+2],
#                 'categories': [sheet_target, start+1, 0, start+max_row, 0],
#                 'values' : [sheet_target, start+1, 2, start+max_row, 2],
#                 'gap' : 2
#                 })

# chart.add_series({
#                 'name': [sheet_target, start, start+3],
#                 'categories': [sheet_target, start+1, 0, start+max_row, 0],
#                 'values' : [sheet_target, start+1, 3, start+max_row, 3],
#                 'gap' : 2
#                 })

# chart.add_series({
#                 'name': [sheet_target, start, start+4],
#                 'categories': [sheet_target, start+1, 0, start+max_row, 0],
#                 'values' : [sheet_target, start+1,4,start+max_row,4],
#                 'gap' : 2
#                 })