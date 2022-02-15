# %%
import sys
sys.path.append("C:\\Users\\ejohbea\\Documents\\Python Scripts\\jtb-adv-python")

#import sys
import import_ipynb


import pandas as pd
import numpy as np
import ipywidgets as widgets
import time
import csv
import math
import xlsxwriter

from xlsxwriter.utility import xl_col_to_name, xl_range
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFilter, ImageFont
#from timezonefinder import TimezoneFinder
from dateutil.parser import parse
from astral import LocationInfo
from astral.sun import sun
from pytz import timezone, utc

#from DopQueries import DopQueries



# %%
# Declare Output path
path_output = './output/'
path_input = './input/'
path_process ='./process/'
path_logs = './logs/'

# Declare key path
path_keys = 'C:/Users/ejohbea/OneDrive - Ericsson AB/Documents/Technical/EPP/DOP API/keys/'

default_report_days = 1
process_start_time = datetime.now()
process_datestamp = process_start_time.strftime('%Y%m%d')
process_start_date_stamp = process_start_time.strftime("%Y-%m-%d'%Y-%m-%d %H:%M:%S'")

default_start_date = process_start_time - timedelta(days = 0 + default_report_days)
default_end_date   = process_start_time - timedelta(days = 0)

#Test values
start_date = default_start_date.strftime("%Y/%m/%d")
end_date   = default_end_date.strftime("%Y/%m/%d")
eNodeB = "M7BAN188A"
Cell = "E7BAN188A11"

# Set 1 - Single  eNodeB
#eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]
#Cell_list = ["D7BAN063A11","D7BAN063A21","D7BAN063A31"]

# Set 2 - Different sites, same carrier
#eNodeB_list = ["L7WAC502A","L7WAC239B","L7WAC356D"]
#Cell_list = ["B7WAC502A11","B7WAC239B41","B7WAC356D21"]

#Set 3
#eNodeB_list = ["M7BAN188A"]
#Cell_list = ["E7BAN188A11"]

# Set 4 from text file
cell_list = ['node','blank']
#cell_df_lte = pd.read_csv(path_input+'TMO_NR_Philly.csv', names=cell_list)
#cell_df_lte = pd.read_csv(path_input+'ATT_NR_multi_hi_band_b.csv', names=cell_list)
#cell_df_lte = pd.read_csv(path_input+'TMO_LTE_single.csv', names=cell_list)
cell_df_lte = pd.read_csv(path_input+'TMO_LTE_multi.csv', names=cell_list)


cell_df_lte[["eNodeB","Cell"]] = cell_df_lte["node"].str.split(".",expand=True)
eNodeB_list=cell_df_lte["eNodeB"].tolist()
Cell_list = cell_df_lte["Cell"].tolist()

#cell_df_nr =  pd.read_csv(path_input+'TMO_NR_Philly.csv', names=cell_list)
#cell_df_nr[["eNodeB","Cell"]] = cell_df_nr["node"].str.split(".",expand=True)


print(f'{start_date} to {end_date}')
print(f'eNodeB list: {eNodeB_list}')
print(f'Cell list: {Cell_list}')


cell_df_lte.head()


# %%
from dop_query import DopQueries
print(DopQueries)

customer = "TMO"
technology = "LTE"
vendor = "ericsson"
#time_resolution = "15mins"
time_resolution = "Hourly"

dq = DopQueries(path_keys,customer,technology,vendor,time_resolution,start_date,end_date)
foo = dq.test()
bar = dq.loopback("Rar")
test_path = dq.key_path()
print(test_path)

# %%
# Create the time filtered PM cell query

if False:
    
    start_time = datetime.now()  
    foo =  dq.pm_query_cell(eNodeB_list,Cell_list)
    end_time = datetime.now()
    

# %%
#dq.df_pm_cell.head()
#dq.unique_object_ids

#dq.df_time_template.head()

# %%
#dq.df_time_template.tail()

# %%
# Test the PM Cell level table query by market

if True:

    print(f"{eNodeB_list}") 
    print(f"{Cell_list}")    
    
    #Cell_list = ["D7BAN063A11","D7BAN063A21","D7BAN063A31"]    
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
    #Cell_list = [""]
    #time_resolution = "Hourly"
    start_time = datetime.now()
    prb_to_dbm = True
    foo =  dq.pm_query_cell(eNodeB_list,Cell_list,prb_to_dbm)
    #self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    
    end_time = datetime.now()

    status =        foo['resp_status_code']
    headers =       foo['resp_headers']
    df_pm_cell =    foo['result']
    result_status = foo['result_status']
    query_status =  foo['resp_status_code']
    raw_json =      foo['JSON']
    columns =       foo['columns']
    rows =          foo['rows']

    
    time_diff = (end_time - start_time).total_seconds()
    #process_time['1.0 CM CELL QUERY'] = time_diff
    print('2.0 PM CELL QUERY '+str(time_diff)+' seconds')
    print(f"JSON Status:{query_status}, Process Status:{result_status}")
    print(f"--------------------")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")

    df_pm_cell.to_excel(r"Query_check_pm_cell_eNodeB_LTE.xlsx", index=True, header=True)      
    
#df_test.head()
df_pm_cell.head()
#df_pm_cell.tail(30)

# %%
#print(f"raw: {len(dq.df_pm_cell_raw)}, temp: {len(dq.df_pm_cell_temp)}, combined: {len(dq.df_pm_cell)}, template: {len(dq.df_time_template)}")
#print(f"Su-34len_raw}, clipped: {len_clipped}, merged: {len_merged}, combined:{len_out}, time temp: {len_template}")
#df_pm_cell.head()
#df_pm_cell.tail(30)
#dq.df_time_template(r"Query_check_pm_time_template.xlsx", index=True, header=True)      

#raw file
#dq.df_pm_cell_raw.head()
#dq.df_pm_cell_raw.to_excel(r"Query_check_debug_pm_cell_raw.xlsx", index=True, header=True)  

#Temp file
#dq.df_pm_cell_temp.head()
#dq.df_pm_cell_temp.to_excel(r"Query_check_debug_pm_cell_temp.xlsx", index=True, header=True)  

#dq.df_time_template.tail()
#dq.df_pm_cell_empty.head()
#dq.df_pm_cell = pd.DataFrame(columns=dq.df_pm_cell_raw.columns.values, index=None)
#dq.df_pm_cell = pd.DataFrame(columns=dq.df_pm_cell_raw.columns.values)
#dq.df_pm_cell.head()

#df_pm_cell['pusch_rssi_count'] = df_pm_cell[dq.pusch_rssi_list].count(axis=1, numeric_only = True)  
#df_pm_cell['pusch_rssi'] = 777
#df_pm_cell['pusch_rssi_numerator'] = 888
#df_pm_cell['pusch_rssi_denominator'] = 999
#df_pm_cell['pusch_rssi_numerator'] = df_pm_cell[dq.pusch_rssi_list].apply(dq.weighted_avg_rssi_pusch_numerator, axis=1)
#df_pm_cell['pusch_rssi_denominator'] = df_pm_cell[dq.pusch_rssi_list].apply(dq.weighted_avg_rssi_pusch_denominator, axis=1)
#df_pm_cell['pusch_rssi'] = df_pm_cell[['pusch_rssi_numerator','pusch_rssi_denominator']].apply(dq.weighted_avg_rssi_pusch_formula, axis=1)

#print(dq.pusch_rssi_list) 

#f_pm_cell.to_excel(r"Query_check_pm_cell_eNodeB_LTE.xlsx", index=True, header=True)  

#df_pm_cell['populated_PRB_100_type'] = df_pm_cell['prb_100'].apply(lambda x: type(int(x)))
#df_pm_cell['populated_PRBs_new']   = df_pm_cell[dq.prb_list].(column!=0).count(axis=1)
#df_pm_cell['populated_PRBs_new']   = 999
#f_pm_cell['populated_PRBs_new']   = df_pm_cell.loc[df_pm_cell['prb_1'] >0].count(axis=1)
#df_pm_cell['populated_PRBs_new']   = df_pm_cell[dq.prb_list].count(axis=1).astype(bool)
#df_pm_cell['populated_PRBs_new']   = np.count_nonzero(df_pm_cell[dq.prb_list])

#df_pm_cell['populated_PRBs_new']   = (df_pm_cell[dq.prb_list] != '0' ).sum(axis=1)

#self.df_pm_cell['populated_PRBs']   = (self.df_pm_cell['PRB_1'] != '0' ).sum(axis=1)  
#df_pm_cell['populated_PRBs_new']   = (type(df_pm_cell['prb_1']) == np.nan).sum(axis=1)


df_pm_cell[['ObjectId',
            'datetime_t',
            'PRBs',
            'populated_PRBs',
            #'populated_PRBs_new',            
            'sum_prb_rssi',
            'avg_prb_rssi',
            "prb_1",
            "prb_25",
            "prb_50",
            "prb_75",
            "prb_76",            
            "prb_100",    
           ]].head(48)



# %%
if False:    
    df_test['value'] = (df_pm_cell[dq.prb_list] != True).sum(axis=1) 
    df_test[[
                "prb_1",
                "prb_25",
                "prb_50",
                "prb_75",
                "prb_76",          
                "prb_100",      
    ]].head()
    
    

df_test = (df_pm_cell[dq.prb_list] != True).sum(axis=1) 

# %%
df_test  = dq.df_pm_cell_raw.loc[dq.df_pm_cell_raw['ObjectId']== "L7BAC002A.L7BAC002A21"]
df_test.to_excel(r"Query_check_debug_df_temp.xlsx", index=True, header=True)  

#df_pm_cell_test = dq.df_time_template.merge(df_test, how='left', on='datetime') 
#df_pm_cell_test = df_pm_cell_test.assign(ObjectId="L7BAC002A.L7BAC002A21")
#df_pm_cell_test.to_excel(r"Query_check_debug_pm_merged_test.xlsx", index=True, header=True)  
#df_test.head(10)
#len(df_test)
#len(df_pm_cell_test)
#df_pm_cell_test.tail(10)
#df_test.describe()

# %%
dq.unique_object_ids

# %%
# Test the Schema attributes for Precanned report 

if True:
    
    time_resolution = "rop"    
    report_name = "XIC Inference Report Daily"
    start_time = datetime.now()  
    foo = dq.precanned_report_schema_attributes(technology,report_name)
    #self,customer,vendor,tech,time_resolution,table_name):
    #(self,customer,vendor,tech,data_type,time_resolution):    
    
    end_time = datetime.now()  
    #print(foo)

    query_status =  foo['resp_status_code']
    headers =       foo['resp_headers']
    result_status = foo['result_status']
    pc_rpt_schema  = foo['result']
    raw_json =      foo['JSON']
    raw_url =       foo['url']
    raw_body =      foo['body']
    columns =       foo['columns']
    rows =          foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('Schema attributes QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}")
    print(f"Result:{result_status}")
    print(f"--------------------")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    print(f"Headers:{headers}")
    print(f"--------------------")
    print(f"Head:{raw_url}")
    print(f"--------------------") 
    print(f"Body:{raw_body}")
    print(f"--------------------")  
    print(f"JSON:{raw_json}")
    #print(f"--------------------")    
    #print(type(df_schema))
    #print(f"--------------------")
    #print(df_cm)
    #print(f"--------------------")
    #print(df_cm.head())

    #xlsx_file_name = 
    pc_rpt_schema.to_excel(rf"Query_check_PreCanned_schema_{report_name}.xlsx", index=True, header=True)
    
pc_rpt_schema.head()

# %%
dq.lte_bw_to_prbs

# %%
# Test the PreCanned Report List for NR

df_schema = {}

if True:
    
    #time_resolution = "rop"
    start_time = datetime.now()  
    technology = "NR"
    foo = dq.precanned_report_list(customer,"ericsson",technology)
    #foo = dq.schema_list(customer,"ericsson",technology,"PM",time_resolution)
    #(self,customer,vendor,tech,data_type,time_resolution):    
    
    end_time = datetime.now()  
    #print(foo)

    query_status =   foo['resp_status_code']
    result_status = foo['result_status']    
    headers =        foo['resp_headers']
    #result_status = foo['result_status']
    df_pc_rpt_list = foo['result']
    raw_json =       foo['JSON']
    raw_url =        foo['url']
    raw_body =       foo['body']
    #columns =       foo['columns']
    #rows =          foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('Precanned Report List QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}")
    print(f"Result:{result_status}")
    #print(f"--------------------")
    #print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    print(f"Headers:{headers}")
    #print(f"--------------------")
    #rint(f"URL:{raw_url}")
    #rint(f"--------------------") 
    #rint(f"Body:{raw_body}")
    #rint(f"--------------------")  
    #rint(f"JSON:{raw_json}")
    #print(f"--------------------")    
    #print(type(df_schema))
    #print(f"--------------------")
    #print(df_schema)
    #print(f"--------------------")
    #print(df_schema.head())

    df_pc_rpt_list.to_excel(r"Query_check_PreCanned_report_list_NR.xlsx", index=True, header=True)
    
df_pc_rpt_list.head()

# %%
# Test the PreCanned Report List for LTE

df_schema = {}

if True:
    
    #time_resolution = "rop"
    start_time = datetime.now()  
    technology = "LTE"
    foo = dq.precanned_report_list(customer,"ericsson",technology)
    #foo = dq.schema_list(customer,"ericsson",technology,"PM",time_resolution)
    #(self,customer,vendor,tech,data_type,time_resolution):    
    
    end_time = datetime.now()  
    #print(foo)

    query_status =   foo['resp_status_code']
    result_status = foo['result_status']    
    headers =        foo['resp_headers']
    #result_status = foo['result_status']
    df_pc_rpt_list = foo['result']
    raw_json =       foo['JSON']
    raw_url =        foo['url']
    raw_body =       foo['body']
    #columns =       foo['columns']
    #rows =          foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('Precanned Report List QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}")
    print(f"Result:{result_status}")
    #print(f"--------------------")
    #print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    print(f"Headers:{headers}")
    #print(f"--------------------")
    #rint(f"URL:{raw_url}")
    #rint(f"--------------------") 
    #rint(f"Body:{raw_body}")
    #rint(f"--------------------")  
    #rint(f"JSON:{raw_json}")
    #print(f"--------------------")    
    #print(type(df_schema))
    #print(f"--------------------")
    #print(df_schema)
    #print(f"--------------------")
    #print(df_schema.head())

    df_pc_rpt_list.to_excel(r"Query_check_PreCanned_report_list_LTE.xlsx", index=True, header=True)
    
df_pc_rpt_list.head()

# %%
# Test the NR PM Cell level table query by market

if True:
    
    print(f"{Cell_list}")    

    #Cell_list = ["D7BAN063A11","D7BAN063A21","D7BAN063A31"]    
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
    #Cell_list = [""]
    #time_resolution = "Hourly"
    start_time = datetime.now()    
    foo =  dq.pm_query_cell_NR(eNodeB_list,Cell_list)
    #self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    
    end_time = datetime.now()

    status =        foo['resp_status_code']
    headers =       foo['resp_headers']
    df_pm_cell_NR = foo['result']
    result_status = foo['result_status']
    query_status =  foo['resp_status_code']
    raw_json =      foo['JSON']
    columns =       foo['columns']
    rows =          foo['rows']

    
    time_diff = (end_time - start_time).total_seconds()
    #process_time['2.1 NR PM CELL QUERY'] = time_diff
    print('2.0 PM CELL QUERY '+str(time_diff)+' seconds')
    print(f'{start_date} to {end_date}')
    print(f"Result:{result_status}")
    #print(f"Status:{query_status}")"
    print(f"--------------------")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")

    df_pm_cell_NR.to_excel(r"Query_check_pm_cell_eNodeB_NR.xlsx", index=True, header=True)      
    
df_pm_cell_NR.head()

# %%
# Test the CM Master table query for whole gNodeB (eNR)

if True:
    
    print(f"{Cell_list}")
    
    #time_resolution = "Daily"    
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]
    start_time = datetime.now()  
    foo = dq.cm_masterquery_gnodeb(eNodeB_list,Cell_list)
    end_time = datetime.now()  
    #print(foo)

    query_status =        foo['resp_status_code']
    headers =             foo['resp_headers']
    result_status =       foo['result_status']
    df_cm_master_gNodeB = foo['result']
    raw_json =            foo['JSON']
    columns =             foo['columns']
    rows =                foo['rows']
    tech =                foo['technology']

    time_diff = (end_time - start_time).total_seconds()
    print('1.2 NR CM QUERY MASTER '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    #print(f"eNodeB list:{eNodeB_list}")
    #print(f"Cell list:{Cell_list}")
    print(f"--------------------")
    #print(f"URL:{foo['url']}")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(type(df_cm_master_gNodeB))
    #print(f"--------------------")
    #print(df_cm_master_gNodeB)
    #print(f"--------------------")
    #print(df_cm_master_gNodeB.head())

    df_cm_master_gNodeB.to_excel(r"Query_check_cm_gNodeB_list.xlsx", index=True, header=True)       

df_cm_master_gNodeB.head(30)

# %%
# Test the CM Master table query for whole eNodeB (eLTE)

if True:
    
    #time_resolution = "Daily"    
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]
    start_time = datetime.now()  
    foo = dq.cm_masterquery_enodeb(eNodeB_list,Cell)
    end_time = datetime.now()  
    #print(foo)

    query_status =        foo['resp_status_code']
    headers =             foo['resp_headers']
    result_status =       foo['result_status']
    df_cm_master_eNodeB = foo['result']
    raw_json =            foo['JSON']
    columns =             foo['columns']
    rows =                foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('1.1 LTE CM QUERY MASTER'+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(type(df_cm))
    #print(f"--------------------")
    #print(df_cm)
    #print(f"--------------------")
    #print(df_cm.head())

    df_cm_master_eNodeB.to_excel(r"Query_check_cm_eNodeB_list.xlsx", index=True, header=True)       

df_cm_master_eNodeB.head(30)

# %%
# Test the CM ulAttenuation (DataRFBranch) query

if True:

    table_name = "SubNetwork_MeContext_ManagedElement_vsDataEquipment_vsDataAntennaUnitGroup_vsDataRfBranch"
    
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
    time_resolution = "Daily"    
    start_time = datetime.now()  
    foo = dq.cm_query_data_rf_branch(eNodeB_list,end_date) 
    end_time = datetime.now()  

    query_status =    foo['resp_status_code']
    headers =         foo['resp_headers']
    df_cm_rf_branch = foo['result']
    result_status = foo['result_status']    
    #result_status =   ""  
    raw_json =        foo['JSON']
    columns =         foo['columns']
    rows =            foo['rows']

    
    time_diff = (end_time - start_time).total_seconds()
    print('CM ulAttn QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"--------------------")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")
    #print(f"Columns: {columns}")

    if "ManagedElement" in columns:
        print("Yay!")
    else:    
        print("Foo")
    
    df_cm_rf_branch.to_excel(r"Query_check_pm_ulAttenuation.xlsx", index=True, header=True)    
    
df_cm_rf_branch.head()    
#df_ulatt["auPortRef"].head()
    

# %%
# Test the PM Branch level table query

if True:

    #Cell_list = ["D7BAN063A11","D7BAN063A21","D7BAN063A31"]    
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
    #Cell_list = [""]
    
    print(f"{start_date}, {end_date}")

    start_time = datetime.now()    
    #foo =  dq.pm_query_branch(time_resolution,eNodeB_list,Cell_list,start_date,end_date)
    foo =  dq.pm_query_branch(eNodeB_list,Cell_list)
    #foo =  dq.pm_query_branch()
    end_time = datetime.now()

    #status =        foo['resp_status_code']
    headers =       foo['resp_headers']
    df_pm_branch =  foo['result']
    result_status = foo['result_status']    
    query_status =  foo['resp_status_code']
    raw_json =      foo['JSON']
    columns =       foo['columns']
    rows =          foo['rows']

    
    time_diff = (end_time - start_time).total_seconds()
    #process_time['1.0 CM CELL QUERY'] = time_diff
    print('2.0 PM BRANCH QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"--------------------")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")

    df_pm_branch.to_excel(r"Query_check_pm_branch_eNodeB X.xlsx", index=True, header=True)      
    
df_pm_branch.head()

# %%
# Test the PM Cell level table query

if True:

    #Cell_list = ["D7BAN063A11","D7BAN063A21","D7BAN063A31"]    
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
    #Cell_list = [""]
    #time_resolution = "Hourly"
    start_time = datetime.now()    
    #foo =  dq.pm_query_cell(time_resolution,eNodeB_list,Cell_list,start_date,end_date)
    foo =  dq.pm_query_cell(eNodeB_list,Cell_list)
    #self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    
    end_time = datetime.now()

    status =        foo['resp_status_code']
    headers =       foo['resp_headers']
    df_pm_cell =    foo['result']
    result_status = foo['result_status']
    query_status =  foo['resp_status_code']
    raw_json =      foo['JSON']
    columns =       foo['columns']
    rows =          foo['rows']

    
    time_diff = (end_time - start_time).total_seconds()
    #process_time['1.0 CM CELL QUERY'] = time_diff
    print('2.0 PM CELL QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"--------------------")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")

    df_pm_cell.to_excel(r"Query_check_pm_cell_eNodeB.xlsx", index=True, header=True)      
    
df_pm_cell.head()

# %%
# Create the daily and time templates

if True:
    
    print(f"Time templates: {time_resolution}- {start_date} to {end_date}")
    print("Time resolution template")
    #df_time_template =  dq.create_time_template(time_resolution,start_date,end_date)
    df_time_template =  dq.create_time_template()    
    df_time_template.to_excel(r"Query_check_time_template.xlsx", index=True, header=True)    
    
    print("Daily template")
    #df_day_template = dq.create_daily_template(start_date,end_date)
    df_day_template = dq.create_daily_template()
    df_day_template.to_excel(r"Query_check_day_template.xlsx", index=True, header=True)
    
#df_time_template.head(7)
df_day_template.head(7)


# %%
# Test the CM eUtranCellFDD query

df_eutrancellFDD={}

if True:

    #table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataEUtranCellFDD"
    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
        
    time_resolution = "Daily"    
    start_time = datetime.now()  
    foo = dq.cm_query_eUtranCellFDD(eNodeB_list,end_date)
    end_time = datetime.now()  

    query_status =     foo['resp_status_code']
    headers =          foo['resp_headers']
    df_eutrancellFDD = foo['result']
    result_status =    foo['result_status']    
    raw_json =         foo['JSON']
    columns =          foo['columns']
    rows =             foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('CM ulAttn QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")

    df_eutrancellFDD.to_excel(r"Query_check_cm_eUtranCellFDD_eNodeB.xlsx", index=True, header=True)
    
df_eutrancellFDD.head()    
    

# %%
# Test the CM DataSectorCarrier query

if True:

    #eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
    #table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataSectorCarrier"        
      
    time_resolution = "Daily"    
    start_time = datetime.now()  
    foo = dq.cm_query_DataSectorCarrier(eNodeB_list,end_date)
    end_time = datetime.now()  

    query_status =  foo['resp_status_code']
    headers =       foo['resp_headers']
    df_cm_dsc =     foo['result']
    #result_status = foo['result_status']    
    result_status = ""  
    raw_json =      foo['JSON']
    columns =       foo['columns']
    rows =          foo['rows']

    
    time_diff = (end_time - start_time).total_seconds()
    print('CM DataSectorCarrier QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")

    df_cm_dsc.to_excel(r"Query_check_cm_DataSectorCarrier_eNodeB.xlsx", index=True, header=True)    
    
df_cm_dsc.head()    
    

# %%
# Test the CM Master table query for a single cell

if True:
    
    time_resolution = "Daily"    
    start_time = datetime.now()  
    foo = dq.cm_masterquery_cell(eNodeB,Cell)
    end_time = datetime.now()  
    #print(foo)

    query_status =      foo['resp_status_code']
    headers =           foo['resp_headers']
    result_status =     foo['result_status']
    df_cm_master_cell = foo['result']
    raw_json =          foo['JSON']
    columns =           foo['columns']
    rows =              foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('CM ulAttn QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    #print(f"--------------------")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(type(df_cm))
    #print(f"--------------------")
    #print(df_cm)
    #print(f"--------------------")
    #print(df_cm.head())

    df_cm_master_cell.to_excel(r"Query_check_cm_single_cell.xlsx", index=True, header=True)       

df_cm_master_cell.head()

# %%
# Test the CM PmUlInterferenceReport query

if True:

    #table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataEUtranCellFDD"
    eNodeB_list = ["L7BAC007A","L7BAC007A2","L7BAC007A3","M7BAC007A"]    
    #eNodeB_list = ["L7BAC007A"]
    
    test_str = ','.join(eNodeB_list)
    print(test_str)
   
    start_time = datetime.now()  
    foo = dq.cm_query_PmUlInterferenceReport(eNodeB_list,end_date)
    end_time = datetime.now()  

    query_status =  foo['resp_status_code']
    headers =       foo['resp_headers']
    df_cm_pmulir =  foo['result']
    #result_status = foo['result_status']    
    result_status = ""  
    raw_json =      foo['JSON']
    columns =       foo['columns']
    rows =          foo['rows']

    
    time_diff = (end_time - start_time).total_seconds()
    print('CM ulAttn QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}, Result:{result_status}")
    print(f"DF Cols:{len(columns)}, rows:{rows}")
    print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")

    df_cm_pmulir.to_excel(r"Query_check_cm_PmUlInterferenceReport_eNodeB.xlsx", index=True, header=True)    
    
df_cm_pmulir.head()    
#df_ulatt["auPortRef"].head()
    

# %%
# Save outputs to Excel

if True:

    filename_xlsx_report = f' Query Check Combined Report.xlsx'   
    writer = pd.ExcelWriter(filename_xlsx_report, engine='xlsxwriter')
    workbook = writer.book
    fmt_header = workbook.add_format({'bold':True})
    fmt_rssi   = workbook.add_format({'num_format':'####.#'})

    df_day_template.to_excel(writer,sheet_name="Day Template", index=False)
    ws_df_day_template = writer.sheets['Day Template']
    ws_df_day_template.set_row(0, None, fmt_header)      
    
    df_time_template.to_excel(writer,sheet_name="Time Template", index=False)
    ws_df_time_template = writer.sheets['Time Template']
    ws_df_time_template.set_row(0, None, fmt_header)      
    
    df_cm_master_eNodeB.to_excel(writer,sheet_name="CM Cell Master", index=False)
    ws_df_cm_master_eNodeB = writer.sheets['CM Cell Master']
    ws_df_cm_master_eNodeB.set_row(0, None, fmt_header)    
    
    df_eutrancellFDD.to_excel(writer,sheet_name="CM eUtranCellFDD", index=False)
    ws_df_eutrancellFDD = writer.sheets['CM eUtranCellFDD']
    ws_df_eutrancellFDD.set_row(0, None, fmt_header)        
    
    df_cm_dsc.to_excel(writer,sheet_name="CM DataSectorCarrier", index=False)
    ws_df_cm_dsc = writer.sheets['CM DataSectorCarrier']
    ws_df_cm_dsc.set_row(0, None, fmt_header)
    
    df_cm_rf_branch.to_excel(writer,sheet_name="CM DataRFBranch", index=False)
    ws_df_cm_rf_branch = writer.sheets['CM DataRFBranch']
    ws_df_cm_rf_branch.set_row(0, None, fmt_header)
    
    df_cm_rf_branch.to_excel(writer,sheet_name="PM Cell", index=False)
    ws_df_cm_rf_branch = writer.sheets['PM Cell']
    ws_df_cm_rf_branch.set_row(0, None, fmt_header)    

    df_pm_cell.to_excel(writer,sheet_name="PM Cell", index=False)
    ws_df_pm_cell = writer.sheets['PM Cell']
    ws_df_pm_cell.set_row(0, None, fmt_header)    
    
    df_pm_branch.to_excel(writer,sheet_name="PM Branch", index=False)
    ws_df_pm_branch = writer.sheets['PM Branch']
    ws_df_pm_branch.set_row(0, None, fmt_header)       
    
    writer.save()
    

# %%
#
#
# Utility functions
#
#

# %%
# Test the SchemaList

df_schema = {}

if True:
    
    time_resolution = "rop"    
    start_time = datetime.now()  
    technology = "NR"
    foo = dq.schema_list(customer,"ericsson",technology,"CM","rop")
    #foo = dq.schema_list(customer,"ericsson",technology,"PM",time_resolution)
    #(self,customer,vendor,tech,data_type,time_resolution):    
    
    end_time = datetime.now()  
    #print(foo)

    query_status =  foo['resp_status_code']
    headers =       foo['resp_headers']
    #result_status = foo['result_status']
    df_schema =     foo['result']
    raw_json =      foo['JSON']
    raw_url =       foo['url']
    raw_body =      foo['body']
    #columns =       foo['columns']
    #rows =          foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('Schema List QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}")
    #print(f"Status:{query_status}, Result:{result_status}")
    #print(f"--------------------")
    #print(f"DF Cols:{len(columns)}, rows:{rows}")
    #print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"Head:{raw_url}")
    #print(f"--------------------") 
    #print(f"Body:{raw_body}")
    #print(f"--------------------")  
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")    
    #print(type(df_schema))
    #print(f"--------------------")
    #print(df_schema)
    #print(f"--------------------")
    #print(df_schema.head())

    df_schema.to_excel(r"Query_check_SchemaList.xlsx", index=True, header=True)
    
df_schema.head()

# %%
# Test the Schema attributes for vsDataRfBranch

df_schema_att = {}

if False:
    
    time_resolution = "rop"    
    table_name = "SubNetwork_MeContext_ManagedElement_vsDataEquipment_vsDataAntennaUnitGroup_vsDataRfBranch"
    start_time = datetime.now()  
    foo = dq.schema_attributes(customer,"ericsson",technology,time_resolution,"CM",table_name)
    #self,customer,vendor,tech,time_resolution,table_name):
    #(self,customer,vendor,tech,data_type,time_resolution):    
    
    end_time = datetime.now()  
    #print(foo)

    query_status =  foo['resp_status_code']
    headers =       foo['resp_headers']
    #result_status = foo['result_status']
    df_schema_att = foo['result']
    raw_json =      foo['JSON']
    #raw_url =       foo['url']
    raw_body =      foo['body']
    #columns =       foo['columns']
    #rows =          foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('Schema attributes QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}")
    #print(f"Status:{query_status}, Result:{result_status}")
    print(f"--------------------")
    #print(f"DF Cols:{len(columns)}, rows:{rows}")
    #print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"Head:{raw_url}")
    #print(f"--------------------") 
    #print(f"Body:{raw_body}")
    #print(f"--------------------")  
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")    
    #print(type(df_schema))
    #print(f"--------------------")
    #print(df_cm)
    #print(f"--------------------")
    #print(df_cm.head())

    #df_schema.to_excel(r"Query_check_SchemaList.xlsx", index=True, header=True)    
    df_schema_att.to_excel(r"Query_check_Schema_attributes_vsDataRfBranch.xlsx", index=True, header=True)
    
#df_schema_att.head()

# %%
# Test the Schema attributes for vsDataEUtranCellFDD

df_schema_eUtranCellFDD = {}

if False:
    
    time_resolution = "rop"    
    table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataEUtranCellFDD"
    start_time = datetime.now()  
    foo = dq.schema_attributes(customer,"ericsson",technology,"rop","CM",table_name)
    #self,customer,vendor,tech,time_resolution,table_name):
    #(self,customer,vendor,tech,data_type,time_resolution):    
    
    end_time = datetime.now()  
    #print(foo)

    query_status =            foo['resp_status_code']
    headers =                 foo['resp_headers']
    #result_status =           foo['result_status']
    df_schema_eUtranCellFDD = foo['result']
    raw_json =                foo['JSON']
    #raw_url =                 foo['url']
    raw_body =                foo['body']
    #columns =                 foo['columns']
    #rows =                    foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('Schema attributes QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}")
    #print(f"Status:{query_status}, Result:{result_status}")
    print(f"--------------------")
    #print(f"DF Cols:{len(columns)}, rows:{rows}")
    #print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"Head:{raw_url}")
    #print(f"--------------------") 
    #print(f"Body:{raw_body}")
    #print(f"--------------------")  
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")    
    #print(type(df_schema))
    #print(f"--------------------")
    #print(df_cm)
    #print(f"--------------------")
    #print(df_cm.head())

    df_schema.to_excel(r"Query_check_Schema_attributes_vsDataEUtranCellFDD.xlsx", index=True, header=True)
    
df_schema_eUtranCellFDD.head()

# %%


# %%
# Test the Schema attributes for vsDataSectorCarrier

if True:
    
    time_resolution = "rop"    
    table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataSectorCarrier"
    start_time = datetime.now()  
    foo = dq.schema_attributes("rop","CM",table_name)
    #self,customer,vendor,tech,time_resolution,table_name):
    #(self,customer,vendor,tech,data_type,time_resolution):    
    
    end_time = datetime.now()  
    #print(foo)

    query_status =  foo['resp_status_code']
    headers =       foo['resp_headers']
    #result_status = foo['result_status']
    df_schema_dsc = foo['result']
    raw_json =      foo['JSON']
    #raw_url =       foo['url']
    raw_body =      foo['body']
    #columns =       foo['columns']
    #rows =          foo['rows']

    time_diff = (end_time - start_time).total_seconds()
    print('Schema attributes QUERY '+str(time_diff)+' seconds')
    print(f"Status:{query_status}")
    #print(f"Status:{query_status}, Result:{result_status}")
    print(f"--------------------")
    #print(f"DF Cols:{len(columns)}, rows:{rows}")
    #print(f"--------------------")
    #print(f"Headers:{headers}")
    #print(f"--------------------")
    #print(f"Head:{raw_url}")
    #print(f"--------------------") 
    #print(f"Body:{raw_body}")
    #print(f"--------------------")  
    #print(f"JSON:{raw_json}")
    #print(f"--------------------")    
    #print(type(df_schema))
    #print(f"--------------------")
    #print(df_cm)
    #print(f"--------------------")
    #print(df_cm.head())

    df_schema_dsc.to_excel(r"Query_check_Schema_attributes_vsDataSectorCarrier.xlsx", index=True, header=True)
    
df_schema_dsc.head()

# %%


# %%
