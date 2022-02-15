# %%
class DopQueries():
    
    def __init__(self,path_keys,customer,technology,vendor, time_resolution, start_date, end_date):
              
        # These are test values to test basic class method functionality
        # I did this because class methods have to be implemented oddly in Jupyter Notebooks
        # and I wanted to verify the class was working at all
        self.test_val = "Howdy!"
        self.test_val_b = "Blank"
        
        # This is the path to the API keys
        self.path_keys = path_keys
               
        # Build a dictionary of API keys with index of operator
        self.dict_keys ={}
        self.dict_customer_freq_regions_lte = {}        
        self.dict_customer_freq_regions_nr  = {}  
        self.dict_customer_target_rssi  = {}         
        with open(self.path_keys+'customer_list.csv', mode='r') as infile:
            
            csvreader = csv.reader(infile)
            fields = next(csvreader)           
            for row in csvreader:
                self.dict_keys[row[0]] = row[1]
                self.dict_customer_freq_regions_lte[row[0]] = row[2]
                self.dict_customer_freq_regions_nr[row[0]] = row[3]
                self.dict_customer_target_rssi[row[0]] = row[5]
        
        # Initialize query arguments
        self.customer = customer
        self.key = self.dict_keys[self.customer]   
        self.vendor = vendor
        self.tech = technology
        self.key = self.dict_keys[self.customer]
        self.time_resolution = time_resolution
        self.start_date = start_date
        self.end_date = end_date
        
        self.freq_region_lte = self.dict_customer_freq_regions_lte[self.customer]
        self.freq_region_nr = self.dict_customer_freq_regions_nr[self.customer]
        self.target_rssi = self.dict_customer_target_rssi[self.customer]      
        
        # Initialize blank values
        self.type_of_query = ""
        self.eNodeB_list = []
        self.eNodeB_str = ""
        self.eNodeB = ""
        self.Cell = ""
        self.market_list = []
        self.market_str = ""
        #self.start_date = ""
        #self.end_date = ""
        self.record_date = ""
        self.table_name = ""
        self.result_status = ""
        self.rops = 0
        
        self.body =""
        self.json =""
        self.resp_status_code =""
        self.resp_headers =""
        self.cols = ""
        self.rows = ""   
        
        # Initialize blank dataframes
        self.df                        = pd.DataFrame()
        self.df_cm_enodeb              = pd.DataFrame() 
        self.df_ulAtt                  = pd.DataFrame() 
        self.df_eUtranCellFDD          = pd.DataFrame() 
        self.df_DataSectorCarrier      = pd.DataFrame() 
        self.df_PmUlInterferenceReport = pd.DataFrame() 
        self.df_pm_cell                = pd.DataFrame()
        self.df_pm_branch              = pd.DataFrame()        
        
        
        self.result_status
        
        # Load LTE arfcn to frequency conversion lookup
        self.lookupLTE = pd.read_csv('PROJECT_arfcn_LTE.csv')  
        
        # Load LTE arfcn to frequency conversion lookup
        self.lookup_NR = pd.read_csv('PROJECT_arfcn_NR.csv')          
        
        # Define Lookup dict - LTE chBandwidth to PRBs
        #self.lte_bw_to_prbs = {'1400':'6','3000':'15','5000':'25','10000':'50' ,'15000':'75' ,'20000':'100'  }
        self.lte_bw_to_prbs = {'1400':6,'3000':15,'5000':25,'10000':50 ,'15000':75 ,'20000':100  }        
        
        # Define lookup dict - Time resolution to ROP count
        self.time_resolution_to_rops = {'ROP':1,'Hourly':4,'Daily':96}
        
        # Define lookup dict - Day of Week abbreviations
        self.day_of_week_abbreviations = {'Monday':'Mon','Tuesday':'Tues','Wednesday':'Wed','Thursday':'Thur','Friday':'Fri','Saturday':'Sat','Sunday':'Sun'}            
        
        # Create a day template dataframe
        self.df_day_template = self.create_daily_template()        
        
        # Create an hourly template dataframe
        self.df_time_template =  self.create_time_template()          
           
        # Define PM Cell level info columns - don't remember why this is important
        self.cell_prb_cols_init = [
            'ObjectId',
            #'RecordDate',
            'datetime',
            'Date','Hour',
            'eNodeB','Cell',
            'freqBand',
            'earfcndl',
             #'earfcnul',
             #'fcDlCtrFreq',
             'ulFreq',
             'dlChannelBandwidth',
             #'ulChannelBandwidth',
        ]

        # Define PM Cell level columns for final output
        self.cell_prb_col_order = [
            'Node',
            #'RecordDate',
            'datetime',
            'Date','Hour',
            'eNodeB','Cell',
            'freqBand',
            'earfcndl',
            #'fcDlCtrFreq',
            'dlFreq',
            'ulFreq',
            'BandRegion',
            'dlChannelBandwidth',
            #'ulChannelBandwidth',
            'PRBs','pucch_rssi','pusch_rssi','avg_PRB_RSSI','used_PRBs',
            'prb_1','prb_2','prb_3','prb_4','prb_5','prb_6','prb_7','prb_8','prb_9','prb_10',
            'prb_11','prb_12','prb_13','prb_14','prb_15','prb_16','prb_17','prb_18','prb_19','prb_20',
            'prb_21','prb_22','prb_23','prb_24','prb_25','prb_26','prb_27','prb_28','prb_29','prb_30',
            'prb_31','prb_32','prb_33','prb_34','prb_35','prb_36','prb_37','prb_38','prb_39','prb_40',
            'prb_41','prb_42','prb_43','prb_44','prb_45','prb_46','prb_47','prb_48','prb_49','prb_50',
            'prb_51','prb_52','prb_53','prb_54','prb_55','prb_56','prb_57','prb_58','prb_59','prb_60',
            'prb_61','prb_62','prb_63','prb_64','prb_65','prb_66','prb_67','prb_68','prb_69','prb_70',
            'prb_71','prb_72','prb_73','prb_74','prb_75','prb_76','prb_77','prb_78','prb_79','prb_80',
            'prb_81','prb_82','prb_83','prb_84','prb_85','prb_86','prb_87','prb_88','prb_89','prb_90',
            'prb_91','prb_92','prb_93','prb_94','prb_95','prb_96','prb_97','prb_98','prb_99','prb_100',
            'ObjectId',
        ]        
        
        # Define PRB RSSI columns.  Needed for RSSI calculations
        self.prb_list = [
            'prb_1','prb_2','prb_3','prb_4','prb_5','prb_6','prb_7','prb_8','prb_9','prb_10',
            'prb_11','prb_12','prb_13','prb_14','prb_15','prb_16','prb_17','prb_18','prb_19','prb_20',
            'prb_21','prb_22','prb_23','prb_24','prb_25','prb_26','prb_27','prb_28','prb_29','prb_30',
            'prb_31','prb_32','prb_33','prb_34','prb_35','prb_36','prb_37','prb_38','prb_39','prb_40',
            'prb_41','prb_42','prb_43','prb_44','prb_45','prb_46','prb_47','prb_48','prb_49','prb_50',
            'prb_51','prb_52','prb_53','prb_54','prb_55','prb_56','prb_57','prb_58','prb_59','prb_60',
            'prb_61','prb_62','prb_63','prb_64','prb_65','prb_66','prb_67','prb_68','prb_69','prb_70',
            'prb_71','prb_72','prb_73','prb_74','prb_75','prb_76','prb_77','prb_78','prb_79','prb_80',
            'prb_81','prb_82','prb_83','prb_84','prb_85','prb_86','prb_87','prb_88','prb_89','prb_90',
            'prb_91','prb_92','prb_93','prb_94','prb_95','prb_96','prb_97','prb_98','prb_99','prb_100',
        ]  
        
        # Define PRB RSSI clolumns plus additional values needed for validation
        self.prb_list_and_validation = self.prb_list.copy()
        self.prb_list_and_validation = self.prb_list_and_validation.append(['PRBs','populated_PRBs'])        
        
              
        # Define binned RSSI columns for PUSCH used in weighted avg RSSI calculations
        self.pusch_rssi_list = [
            'pmRadioRecInterferencePwr_0','pmRadioRecInterferencePwr_1',
            'pmRadioRecInterferencePwr_2','pmRadioRecInterferencePwr_3',
            'pmRadioRecInterferencePwr_4','pmRadioRecInterferencePwr_5',
            'pmRadioRecInterferencePwr_6','pmRadioRecInterferencePwr_7',
            'pmRadioRecInterferencePwr_8','pmRadioRecInterferencePwr_9',
            'pmRadioRecInterferencePwr_10','pmRadioRecInterferencePwr_11',
            'pmRadioRecInterferencePwr_12','pmRadioRecInterferencePwr_13',
            'pmRadioRecInterferencePwr_14','pmRadioRecInterferencePwr_15',
        ]              
        
        # Define binned RSSI columns for PUCCH used in weighted avg RSSI calculations        
        self.pucch_rssi_list = [
            'pmRadioRecInterferencePwrPucch_0','pmRadioRecInterferencePwrPucch_1',
            'pmRadioRecInterferencePwrPucch_2','pmRadioRecInterferencePwrPucch_3',
            'pmRadioRecInterferencePwrPucch_4','pmRadioRecInterferencePwrPucch_5',
            'pmRadioRecInterferencePwrPucch_6','pmRadioRecInterferencePwrPucch_7',
            'pmRadioRecInterferencePwrPucch_8','pmRadioRecInterferencePwrPucch_9',
            'pmRadioRecInterferencePwrPucch_10','pmRadioRecInterferencePwrPucch_11',
            'pmRadioRecInterferencePwrPucch_12','pmRadioRecInterferencePwrPucch_13',
            'pmRadioRecInterferencePwrPucch_14','pmRadioRecInterferencePwrPucch_15',
        ]
        
        # Define columns used for LTE DL PRB Utilization % calculation        
        self.dl_prb_util_list = [
            'pmPrbUtilDl_0','pmPrbUtilDl_1','pmPrbUtilDl_2','pmPrbUtilDl_3','pmPrbUtilDl_4',
            'pmPrbUtilDl_5','pmPrbUtilDl_6','pmPrbUtilDl_7','pmPrbUtilDl_8','pmPrbUtilDl_9',
        ]
        
        # Define columns used for NR UL Pwr Sum PRB Distribution
        self.rssi_sum_prb_distr_nr = [
            'pmRadioRecInterferencePwrSumPrbDistr_0','pmRadioRecInterferencePwrSumPrbDistr_1',
            'pmRadioRecInterferencePwrSumPrbDistr_2','pmRadioRecInterferencePwrSumPrbDistr_3',
            'pmRadioRecInterferencePwrSumPrbDistr_4','pmRadioRecInterferencePwrSumPrbDistr_5',
            'pmRadioRecInterferencePwrSumPrbDistr_6','pmRadioRecInterferencePwrSumPrbDistr_7',
            'pmRadioRecInterferencePwrSumPrbDistr_8','pmRadioRecInterferencePwrSumPrbDistr_9',
            'pmRadioRecInterferencePwrSumPrbDistr_10','pmRadioRecInterferencePwrSumPrbDistr_11',
            'pmRadioRecInterferencePwrSumPrbDistr_12','pmRadioRecInterferencePwrSumPrbDistr_13',
            'pmRadioRecInterferencePwrSumPrbDistr_14','pmRadioRecInterferencePwrSumPrbDistr_15',
            'pmRadioRecInterferencePwrSumPrbDistr_16','pmRadioRecInterferencePwrSumPrbDistr_17',
            'pmRadioRecInterferencePwrSumPrbDistr_18','pmRadioRecInterferencePwrSumPrbDistr_19',
            'pmRadioRecInterferencePwrSumPrbDistr_20',
        ]         

        
    def test(self):
        return self.test_val
    
    def loopback(self,value):
        self.test_val_b = value
        return self.test_val_b
    
    def key_path(self):
        return self.path_keys   
    
    def weighted_avg_rssi_pusch_formula_new(self,row):
            
            
        denominator = ( 
                float(row['pmRadioRecInterferencePwr_0']) + 
                float(row['pmRadioRecInterferencePwr_1']) + 
                float(row['pmRadioRecInterferencePwr_2']) + 
                float(row['pmRadioRecInterferencePwr_3']) + 
                float(row['pmRadioRecInterferencePwr_4']) + 
                float(row['pmRadioRecInterferencePwr_5']) + 
                float(row['pmRadioRecInterferencePwr_6']) + 
                float(row['pmRadioRecInterferencePwr_7']) + 
                float(row['pmRadioRecInterferencePwr_8']) + 
                float(row['pmRadioRecInterferencePwr_9']) + 
                float(row['pmRadioRecInterferencePwr_10']) + 
                float(row['pmRadioRecInterferencePwr_11']) + 
                float(row['pmRadioRecInterferencePwr_12']) + 
                float(row['pmRadioRecInterferencePwr_13']) + 
                float(row['pmRadioRecInterferencePwr_14']) + 
                float(row['pmRadioRecInterferencePwr_15']) 
        )   
        
        numerator = ( 
                float(row['pmRadioRecInterferencePwr_0']) * 0.794328235 + 
                float(row['pmRadioRecInterferencePwr_1']) * 1 + 
                float(row['pmRadioRecInterferencePwr_2']) * 1.258925412 + 
                float(row['pmRadioRecInterferencePwr_3']) * 1.584893192 + 
                float(row['pmRadioRecInterferencePwr_4']) * 1.995262315 + 
                float(row['pmRadioRecInterferencePwr_5']) * 2.511886432 + 
                float(row['pmRadioRecInterferencePwr_6']) * 3.16227766 + 
                float(row['pmRadioRecInterferencePwr_7']) * 3.981071706 + 
                float(row['pmRadioRecInterferencePwr_8']) * 5.011872336 + 
                float(row['pmRadioRecInterferencePwr_9']) * 6.309573445 + 
                float(row['pmRadioRecInterferencePwr_10']) * 10 + 
                float(row['pmRadioRecInterferencePwr_11']) * 25.11886432 + 
                float(row['pmRadioRecInterferencePwr_12']) * 63.09573445 + 
                float(row['pmRadioRecInterferencePwr_13']) * 158.4893192 + 
                float(row['pmRadioRecInterferencePwr_14']) * 398.1071706 + 
                float(row['pmRadioRecInterferencePwr_15']) * 630.9573445 
        )
            
        if denominator == 0.0:
            value = 0.0
        else:
            value = 10*np.log10(( ( 
                numerator                
            ) / ( 
                denominator
            )  ) / 1000000000000)  
            
        return (value)      
        
    
    def weighted_avg_rssi_pucch_formula_new (self,row):
            
        denominator = (
                float(row['pmRadioRecInterferencePwrPucch_0']) + 
                float(row['pmRadioRecInterferencePwrPucch_1']) + 
                float(row['pmRadioRecInterferencePwrPucch_2']) + 
                float(row['pmRadioRecInterferencePwrPucch_3']) + 
                float(row['pmRadioRecInterferencePwrPucch_4']) + 
                float(row['pmRadioRecInterferencePwrPucch_5']) + 
                float(row['pmRadioRecInterferencePwrPucch_6']) + 
                float(row['pmRadioRecInterferencePwrPucch_7']) + 
                float(row['pmRadioRecInterferencePwrPucch_8']) + 
                float(row['pmRadioRecInterferencePwrPucch_9']) + 
                float(row['pmRadioRecInterferencePwrPucch_10']) + 
                float(row['pmRadioRecInterferencePwrPucch_11']) + 
                float(row['pmRadioRecInterferencePwrPucch_12']) + 
                float(row['pmRadioRecInterferencePwrPucch_13']) + 
                float(row['pmRadioRecInterferencePwrPucch_14']) + 
                float(row['pmRadioRecInterferencePwrPucch_15'])
        )
            
        numerator = (
                float(row['pmRadioRecInterferencePwrPucch_0']) * 0.794328235 + 
                float(row['pmRadioRecInterferencePwrPucch_1']) * 1 + 
                float(row['pmRadioRecInterferencePwrPucch_2']) * 1.258925412 + 
                float(row['pmRadioRecInterferencePwrPucch_3']) * 1.584893192 + 
                float(row['pmRadioRecInterferencePwrPucch_4']) * 1.995262315 + 
                float(row['pmRadioRecInterferencePwrPucch_5']) * 2.511886432 + 
                float(row['pmRadioRecInterferencePwrPucch_6']) * 3.16227766 + 
                float(row['pmRadioRecInterferencePwrPucch_7']) * 3.981071706 + 
                float(row['pmRadioRecInterferencePwrPucch_8']) * 5.011872336 + 
                float(row['pmRadioRecInterferencePwrPucch_9']) * 6.309573445 + 
                float(row['pmRadioRecInterferencePwrPucch_10']) * 10 + 
                float(row['pmRadioRecInterferencePwrPucch_11']) * 25.11886432 + 
                float(row['pmRadioRecInterferencePwrPucch_12']) * 63.09573445 + 
                float(row['pmRadioRecInterferencePwrPucch_13']) * 158.4893192 + 
                float(row['pmRadioRecInterferencePwrPucch_14']) * 398.1071706 + 
                float(row['pmRadioRecInterferencePwrPucch_15']) * 630.9573445
        )
            
        if denominator == 0.0:
            value = 0.0
        else:

            value = 10*np.log10(( ( 
                numerator                
            ) / ( 
                denominator
            )  ) / 1000000000000)            
            
        return (value)     
    
    def weighted_avg_rssi_nr (self,row):
        
        value = 999
        
        return (value)
       
    
    
    def dl_prb_util_pct_calc(self,row):
        
        value = 777
                        
        denominator = (
                float(row['pmPrbUtilDl_0']) + 
                float(row['pmPrbUtilDl_1']) + 
                float(row['pmPrbUtilDl_2']) + 
                float(row['pmPrbUtilDl_3']) + 
                float(row['pmPrbUtilDl_4']) + 
                float(row['pmPrbUtilDl_5']) + 
                float(row['pmPrbUtilDl_6']) + 
                float(row['pmPrbUtilDl_7']) + 
                float(row['pmPrbUtilDl_8']) + 
                float(row['pmPrbUtilDl_9'])
        )
        
        numerator = (
                       5* float(row['pmPrbUtilDl_0']) + 
                15*float(row['pmPrbUtilDl_1']) + 
                25*float(row['pmPrbUtilDl_2']) + 
                35*float(row['pmPrbUtilDl_3']) + 
                45*float(row['pmPrbUtilDl_4']) + 
                55*float(row['pmPrbUtilDl_5']) + 
                65*float(row['pmPrbUtilDl_6']) + 
                75*float(row['pmPrbUtilDl_7']) + 
                85*float(row['pmPrbUtilDl_8']) + 
                95*float(row['pmPrbUtilDl_9'])
        )
        
        if denominator == 0.0:
            value = 0.0
        else:
            value = ( numerator / denominator )
        
        return (value)
    
    

    def test_apply_function(self,row):    
        
        value = 999
        return (value)

        
    def avg_prb_rssi_calc(self,row):
        
        value = 0
        if row['PRBs'] == row['populated_PRBs']:
            value = (row['sum_prb_rssi'] / row['PRBs']) 
            #value = 888
        else:
            value = np.nan
        return (value)
    
    

# %%

import csv
import pandas as pd
import numpy as np
import math

import requests
from requests import Request, Session
from datetime import datetime, timedelta

import codecs
#from pandas.io.json import json_normalize
from pandas import json_normalize


# To make the methods in this class work in individual Jupyter Notebook cells,
# I'm using solution #2 from:
# https://stackoverflow.com/questions/45161393/jupyter-split-classes-in-multiple-cells


# %%


# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    #def pm_query_cell(self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    def pm_query_market(self,market_list):
        
        #self.start_date = start_date
        #self.end_date = end_date
        self.date_range = self.start_date+'-'+self.end_date
        #self.time_resolution = time_resolution
        self.market_list = market_list
        self.market_str = ','.join(market_list)
        #self.eNodeB_list = eNodeB_list
        #self.eNodeB_str = ','.join(eNodeB_list)
        #self.Cell_list = Cell_list
        #self.Cell_str = ','.join(Cell_list)
        #self.key = self.dict_keys[self.customer]
        self.rops = self.time_resolution_to_rops[self.time_resolution]
        
        #print(f'PM CELL for cell:{self.Cell_str}, temp res:{self.time_resolution}, date range:{self.date_range} ')

        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMPMFM/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "AppId": "EJOHBEA Testing", 
            "TemporalResolution": self.time_resolution, 
            #"SpatialResolution": "No Aggregation",
            "SpatialResolution":"cell",
            "ObjectidType": "SubNetwork1",
            #"Objectids": npm_cell,
            "Objectids": self.market_str,
            "Kpis": [
                {"name": "freqBand","formula": "freqBand"},
                {"name": "earfcndl","formula": "earfcndl"},
                {"name": "earfcnul","formula": "earfcnul"},
                {"name": "dlChannelBandwidth","formula": "dlChannelBandwidth"},
                {"name": "ulChannelBandwidth","formula": "ulChannelBandwidth"},
                {"name": "max_rrc_conn_users","formula": "pmRrcConnMax"},
                {"name": "prb_1","formula": "pmRadioRecInterferencePwrPrb1"},    
                {"name": "prb_1","formula": "pmRadioRecInterferencePwrPrb1"},
                {"name": "prb_2","formula": "pmRadioRecInterferencePwrPrb2"},
                {"name": "prb_3","formula": "pmRadioRecInterferencePwrPrb3"},
                {"name": "prb_4","formula": "pmRadioRecInterferencePwrPrb4"},
                {"name": "prb_5","formula": "pmRadioRecInterferencePwrPrb5"},
                {"name": "prb_6","formula": "pmRadioRecInterferencePwrPrb6"},
                {"name": "prb_7","formula": "pmRadioRecInterferencePwrPrb7"},
                {"name": "prb_8","formula": "pmRadioRecInterferencePwrPrb8"},
                {"name": "prb_9","formula": "pmRadioRecInterferencePwrPrb9"},
                {"name": "prb_10","formula": "pmRadioRecInterferencePwrPrb10"},
                {"name": "prb_11","formula": "pmRadioRecInterferencePwrPrb11"},
                {"name": "prb_12","formula": "pmRadioRecInterferencePwrPrb12"},
                {"name": "prb_13","formula": "pmRadioRecInterferencePwrPrb13"},
                {"name": "prb_14","formula": "pmRadioRecInterferencePwrPrb14"},
                {"name": "prb_15","formula": "pmRadioRecInterferencePwrPrb15"},
                {"name": "prb_16","formula": "pmRadioRecInterferencePwrPrb16"},
                {"name": "prb_17","formula": "pmRadioRecInterferencePwrPrb17"},
                {"name": "prb_18","formula": "pmRadioRecInterferencePwrPrb18"},
                {"name": "prb_19","formula": "pmRadioRecInterferencePwrPrb19"},
                {"name": "prb_20","formula": "pmRadioRecInterferencePwrPrb20"},
                {"name": "prb_21","formula": "pmRadioRecInterferencePwrPrb21"},
                {"name": "prb_22","formula": "pmRadioRecInterferencePwrPrb22"},
                {"name": "prb_23","formula": "pmRadioRecInterferencePwrPrb23"},
                {"name": "prb_24","formula": "pmRadioRecInterferencePwrPrb24"},
                {"name": "prb_25","formula": "pmRadioRecInterferencePwrPrb25"},
                {"name": "prb_26","formula": "pmRadioRecInterferencePwrPrb26"},
                {"name": "prb_27","formula": "pmRadioRecInterferencePwrPrb27"},
                {"name": "prb_28","formula": "pmRadioRecInterferencePwrPrb28"},
                {"name": "prb_29","formula": "pmRadioRecInterferencePwrPrb29"},
                {"name": "prb_30","formula": "pmRadioRecInterferencePwrPrb30"},
                {"name": "prb_31","formula": "pmRadioRecInterferencePwrPrb31"},
                {"name": "prb_32","formula": "pmRadioRecInterferencePwrPrb32"},
                {"name": "prb_33","formula": "pmRadioRecInterferencePwrPrb33"},
                {"name": "prb_34","formula": "pmRadioRecInterferencePwrPrb34"},
                {"name": "prb_35","formula": "pmRadioRecInterferencePwrPrb35"},
                {"name": "prb_36","formula": "pmRadioRecInterferencePwrPrb36"},
                {"name": "prb_37","formula": "pmRadioRecInterferencePwrPrb37"},
                {"name": "prb_38","formula": "pmRadioRecInterferencePwrPrb38"},
                {"name": "prb_39","formula": "pmRadioRecInterferencePwrPrb39"},                
                {"name": "prb_40","formula": "pmRadioRecInterferencePwrPrb40"},
                {"name": "prb_41","formula": "pmRadioRecInterferencePwrPrb41"},
                {"name": "prb_42","formula": "pmRadioRecInterferencePwrPrb42"},
                {"name": "prb_43","formula": "pmRadioRecInterferencePwrPrb43"},
                {"name": "prb_44","formula": "pmRadioRecInterferencePwrPrb44"},
                {"name": "prb_45","formula": "pmRadioRecInterferencePwrPrb45"},
                {"name": "prb_46","formula": "pmRadioRecInterferencePwrPrb46"},
                {"name": "prb_47","formula": "pmRadioRecInterferencePwrPrb47"},
                {"name": "prb_48","formula": "pmRadioRecInterferencePwrPrb48"},
                {"name": "prb_49","formula": "pmRadioRecInterferencePwrPrb49"},
                {"name": "prb_50","formula": "pmRadioRecInterferencePwrPrb50"},	          
                {"name": "prb_51","formula": "pmRadioRecInterferencePwrPrb51"},
                {"name": "prb_52","formula": "pmRadioRecInterferencePwrPrb52"},
                {"name": "prb_53","formula": "pmRadioRecInterferencePwrPrb53"},
                {"name": "prb_54","formula": "pmRadioRecInterferencePwrPrb54"},
                {"name": "prb_55","formula": "pmRadioRecInterferencePwrPrb55"},
                {"name": "prb_56","formula": "pmRadioRecInterferencePwrPrb56"},
                {"name": "prb_57","formula": "pmRadioRecInterferencePwrPrb57"},
                {"name": "prb_58","formula": "pmRadioRecInterferencePwrPrb58"},
                {"name": "prb_59","formula": "pmRadioRecInterferencePwrPrb59"},
                {"name": "prb_60","formula": "pmRadioRecInterferencePwrPrb60"},
                {"name": "prb_61","formula": "pmRadioRecInterferencePwrPrb61"},
                {"name": "prb_62","formula": "pmRadioRecInterferencePwrPrb62"},
                {"name": "prb_63","formula": "pmRadioRecInterferencePwrPrb63"},
                {"name": "prb_64","formula": "pmRadioRecInterferencePwrPrb64"},
                {"name": "prb_65","formula": "pmRadioRecInterferencePwrPrb65"},
                {"name": "prb_66","formula": "pmRadioRecInterferencePwrPrb66"},
                {"name": "prb_67","formula": "pmRadioRecInterferencePwrPrb67"},
                {"name": "prb_68","formula": "pmRadioRecInterferencePwrPrb68"},
                {"name": "prb_69","formula": "pmRadioRecInterferencePwrPrb69"},
                {"name": "prb_70","formula": "pmRadioRecInterferencePwrPrb70"},	
                {"name": "prb_71","formula": "pmRadioRecInterferencePwrPrb71"},
                {"name": "prb_72","formula": "pmRadioRecInterferencePwrPrb72"},
                {"name": "prb_73","formula": "pmRadioRecInterferencePwrPrb73"},
                {"name": "prb_74","formula": "pmRadioRecInterferencePwrPrb74"},
                {"name": "prb_75","formula": "pmRadioRecInterferencePwrPrb75"},
                {"name": "prb_76","formula": "pmRadioRecInterferencePwrPrb76"},
                {"name": "prb_77","formula": "pmRadioRecInterferencePwrPrb77"},
                {"name": "prb_78","formula": "pmRadioRecInterferencePwrPrb78"},
                {"name": "prb_79","formula": "pmRadioRecInterferencePwrPrb79"},
                {"name": "prb_80","formula": "pmRadioRecInterferencePwrPrb80"},	
                {"name": "prb_81","formula": "pmRadioRecInterferencePwrPrb81"},
                {"name": "prb_82","formula": "pmRadioRecInterferencePwrPrb82"},
                {"name": "prb_83","formula": "pmRadioRecInterferencePwrPrb83"},
                {"name": "prb_84","formula": "pmRadioRecInterferencePwrPrb84"},
                {"name": "prb_85","formula": "pmRadioRecInterferencePwrPrb85"},
                {"name": "prb_86","formula": "pmRadioRecInterferencePwrPrb86"},
                {"name": "prb_87","formula": "pmRadioRecInterferencePwrPrb87"},
                {"name": "prb_88","formula": "pmRadioRecInterferencePwrPrb88"},
                {"name": "prb_89","formula": "pmRadioRecInterferencePwrPrb89"},
                {"name": "prb_90","formula": "pmRadioRecInterferencePwrPrb90"},	
                {"name": "prb_91","formula": "pmRadioRecInterferencePwrPrb91"},
                {"name": "prb_92","formula": "pmRadioRecInterferencePwrPrb92"},
                {"name": "prb_93","formula": "pmRadioRecInterferencePwrPrb93"},
                {"name": "prb_94","formula": "pmRadioRecInterferencePwrPrb94"},
                {"name": "prb_95","formula": "pmRadioRecInterferencePwrPrb95"},
                {"name": "prb_96","formula": "pmRadioRecInterferencePwrPrb96"},
                {"name": "prb_97","formula": "pmRadioRecInterferencePwrPrb97"},
                {"name": "prb_98","formula": "pmRadioRecInterferencePwrPrb98"},
                {"name": "prb_99","formula": "pmRadioRecInterferencePwrPrb99"},
                {"name": "prb_100","formula": "pmRadioRecInterferencePwrPrb100"},           
                {"name": "pmRadioRecInterferencePwr_0","formula": "pmRadioRecInterferencePwr_0"},   
                {"name": "pmRadioRecInterferencePwr_1","formula": "pmRadioRecInterferencePwr_1"},   
                {"name": "pmRadioRecInterferencePwr_2","formula": "pmRadioRecInterferencePwr_2"},   
                {"name": "pmRadioRecInterferencePwr_3","formula": "pmRadioRecInterferencePwr_3"},   
                {"name": "pmRadioRecInterferencePwr_4","formula": "pmRadioRecInterferencePwr_4"},   
                {"name": "pmRadioRecInterferencePwr_5","formula": "pmRadioRecInterferencePwr_5"},   
                {"name": "pmRadioRecInterferencePwr_6","formula": "pmRadioRecInterferencePwr_6"},   
                {"name": "pmRadioRecInterferencePwr_7","formula": "pmRadioRecInterferencePwr_7"},   
                {"name": "pmRadioRecInterferencePwr_8","formula": "pmRadioRecInterferencePwr_8"},   
                {"name": "pmRadioRecInterferencePwr_9","formula": "pmRadioRecInterferencePwr_9"},   
                {"name": "pmRadioRecInterferencePwr_10","formula": "pmRadioRecInterferencePwr_10"},   
                {"name": "pmRadioRecInterferencePwr_11","formula": "pmRadioRecInterferencePwr_11"},   
                {"name": "pmRadioRecInterferencePwr_12","formula": "pmRadioRecInterferencePwr_12"},   
                {"name": "pmRadioRecInterferencePwr_13","formula": "pmRadioRecInterferencePwr_13"},   
                {"name": "pmRadioRecInterferencePwr_14","formula": "pmRadioRecInterferencePwr_14"},   
                {"name": "pmRadioRecInterferencePwr_15","formula": "pmRadioRecInterferencePwr_15"},   
                {"name": "pmRadioRecInterferencePwr_16","formula": "pmRadioRecInterferencePwr_16"},   
                {"name": "pmRadioRecInterferencePwrPucch_0","formula": "pmRadioRecInterferencePwrPucch_0"}, 
                {"name": "pmRadioRecInterferencePwrPucch_1","formula": "pmRadioRecInterferencePwrPucch_1"},   
                {"name": "pmRadioRecInterferencePwrPucch_2","formula": "pmRadioRecInterferencePwrPucch_2"},   
                {"name": "pmRadioRecInterferencePwrPucch_3","formula": "pmRadioRecInterferencePwrPucch_3"},   
                {"name": "pmRadioRecInterferencePwrPucch_4","formula": "pmRadioRecInterferencePwrPucch_4"},   
                {"name": "pmRadioRecInterferencePwrPucch_5","formula": "pmRadioRecInterferencePwrPucch_5"},   
                {"name": "pmRadioRecInterferencePwrPucch_6","formula": "pmRadioRecInterferencePwrPucch_6"},   
                {"name": "pmRadioRecInterferencePwrPucch_7","formula": "pmRadioRecInterferencePwrPucch_7"},   
                {"name": "pmRadioRecInterferencePwrPucch_8","formula": "pmRadioRecInterferencePwrPucch_8"},   
                {"name": "pmRadioRecInterferencePwrPucch_9","formula": "pmRadioRecInterferencePwrPucch_9"},   
                {"name": "pmRadioRecInterferencePwrPucch_10","formula": "pmRadioRecInterferencePwrPucch_10"},   
                {"name": "pmRadioRecInterferencePwrPucch_11","formula": "pmRadioRecInterferencePwrPucch_11"},   
                {"name": "pmRadioRecInterferencePwrPucch_12","formula": "pmRadioRecInterferencePwrPucch_12"},   
                {"name": "pmRadioRecInterferencePwrPucch_13","formula": "pmRadioRecInterferencePwrPucch_13"},   
                {"name": "pmRadioRecInterferencePwrPucch_14","formula": "pmRadioRecInterferencePwrPucch_14"},   
                {"name": "pmRadioRecInterferencePwrPucch_15","formula": "pmRadioRecInterferencePwrPucch_15"},   
                {"name": "pmRadioRecInterferencePwrPucch_16","formula": "pmRadioRecInterferencePwrPucch_16"}, 
                {"name": "pmRadioRecInterferencePwrPucch_16","formula": "pmRadioRecInterferencePwrPucch_16"},    
                {"name": "pmPrbUtilDl_0","formula": "pmPrbUtilDl_0"},         
                {"name": "pmPrbUtilDl_1","formula": "pmPrbUtilDl_1"},
                {"name": "pmPrbUtilDl_2","formula": "pmPrbUtilDl_2"},
                {"name": "pmPrbUtilDl_3","formula": "pmPrbUtilDl_3"},         
                {"name": "pmPrbUtilDl_4","formula": "pmPrbUtilDl_4"},
                {"name": "pmPrbUtilDl_5","formula": "pmPrbUtilDl_5"},
                {"name": "pmPrbUtilDl_6","formula": "pmPrbUtilDl_6"},         
                {"name": "pmPrbUtilDl_7","formula": "pmPrbUtilDl_7"},
                {"name": "pmPrbUtilDl_8","formula": "pmPrbUtilDl_8"},
                {"name": "pmPrbUtilDl_9","formula": "pmPrbUtilDl_9"}
          ],
          "Daterange": self.date_range,
          #"Daterange": "2021/06/30-2021/07/06",
          "TimeZoneFilter": "local",
          "TimeZoneOutput": "local",
          "MaxNumberOfRows": "1000000",
          "IgnoreMissingCounters": "True",
          "ConvertDistFrom1to0Based": "False"
        }        
                
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)     
        self.json_copy = self.json.copy()
        print(type(self.json_copy))
        
        # Normally, the return is a list.  A dict is returned if failed.  
        # Have to make sure a df is created either way
        if isinstance(self.json_copy, dict):   
            self.cols = []
            self.df_pm_cell = pd.DataFrame.from_dict(self.json_copy, orient="index")
            self.result_status = "Failed"
        else:  
            self.cols  = self.json_copy.pop(0)
            self.df_pm_cell = pd.DataFrame(self.json_copy, columns=self.cols)    
            self.result_status = "Ok"

        self.rows = len(self.df_pm_cell)         
                        
        
        # Miscellaneous other conversions
        self.df_pm_cell['datetime']        = pd.to_datetime(self.df_pm_cell['RecordDate'])
        self.df_pm_cell[["eNodeB","Cell"]] = self.df_pm_cell['ObjectId'].str.split(".",expand=True)
        self.df_pm_cell["Node"]            = self.df_pm_cell['ObjectId']
        self.df_pm_cell['PRBs']         = self.df_pm_cell['dlChannelBandwidth'].map(self.lte_bw_to_prbs)            
                        
        # Convert PRB values to RSSI
        self.df_pm_cell[self.prb_list] = self.df_pm_cell[self.prb_list].applymap(lambda x: (10* np.log10(int(x)* 0.00000000000005684341886080800 /(900 * self.rops * 1000/40))) if (int(x)>0) else np.NaN)
        self.df_pm_cell['pucch_rssi']  = self.df_pm_cell.apply(self.weighted_avg_rssi_pucch, axis=1)
        self.df_pm_cell['pusch_rssi']  = self.df_pm_cell.apply(self.weighted_avg_rssi_pusch, axis=1)
        
        # Convert arfcns to frequencies
        self.df_pm_cell['freq_dl_ctr']     = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','FREQ'))
        self.df_pm_cell['freq_ul_ctr']     = self.df_pm_cell['earfcnul'].apply(self.arfcn_to_freq, args=('UL','FREQ'))
        self.df_pm_cell['freq_band_id']    = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_ID'))
        self.df_pm_cell['freq_name']       = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_NAME'))
        self.df_pm_cell['freq_band_group'] = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_GROUP'))
        
        # Calculate average, 
        self.df_pm_cell['avg_prb_rssi']    = self.df_pm_cell[self.prb_list].mean(axis=1, skipna=True)
        self.df_pm_cell['used_PRBs']       = self.df_pm_cell[self.prb_list].count(axis=1)        
        
        # Calculate avg DL prb utilization
        self.df_pm_cell['dl_prb_util_pct'] = self.df_pm_cell[self.dl_prb_util_list].apply(self.dl_prb_util_pct, axis=1)
           
        #print("A")
        # Merge with date template and fill blanks with zeros
        self.df_pm_cell = self.time_template.merge(self.df_pm_cell,how='left', on='datetime')
        
        #print("B")
        # Convert NaNs to zeros
        self.df_pm_cell[self.prb_list] = self.df_pm_cell[self.prb_list].replace(np.nan, 0.0)            
        #print("C")            
            
        # Drop unnecessary columns
        self.df_pm_cell = self.df_pm_cell.drop(['DateInTicks','DateInHours','DateInDays'],axis =1)              
            
        # Sort and reindex the dataframe
        self.df_pm_cell.sort_values(['ObjectId','datetime'], inplace=True)  
        self.df_pm_cell.reset_index(drop=False, inplace=True)
        
        # Output final result
        #print(f"Hit, {self.rows} rows, {len(self.cols)}")            
        
        self.result_status = ""
        
        result = {}                 
        result['JSON'] = self.json        
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['result'] = self.df_pm_cell
        result['result_status'] = self.result_status
        result['columns'] = self.cols
        result['rows'] = self.rows
        result['customer'] = self.customer
        result['key'] = self.key
        result['date_range'] = self.date_range
        result['technology'] = self.tech
        result['time_resolution'] = self.time_resolution
        result['eNodeB'] = self.eNodeB
        result['Cell'] = self.Cell        
        result['body'] = self.body        
        
        return result
    
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    #def pm_query_branch(self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    def pm_query_branch(self,eNodeB_list,Cell_list):
               
        #self.start_date = start_date
        #self.end_date = end_date
        self.date_range = self.start_date+'-'+self.end_date
        #self.time_resolution = time_resolution
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(self.eNodeB_list)
        self.Cell_list = Cell_list
        self.Cell_str = ','.join(self.Cell_list)
        #self.key = self.dict_keys[self.customer]
        self.rops = self.time_resolution_to_rops[self.time_resolution]
        
        #print(f'PM BRANCH for eNodeBs:{self.eNodeB_str}, temp res:{self.time_resolution}, date range:{self.date_range} ')

        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMPMFM/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "AppId": "EJOHBEA Testing", 
            "TemporalResolution": self.time_resolution, 
            "SpatialResolution": "No Aggregation",
            #"SpatialResolution":"cell",
            "ObjectidType": "cell",
            "Objectids": self.eNodeB_str,
            "Kpis": [
                {"name": "prb_1","formula": "pmRadioRecInterferencePwrBrPrb1"},    
                {"name": "prb_1","formula": "pmRadioRecInterferencePwrBrPrb1"},
                {"name": "prb_2","formula": "pmRadioRecInterferencePwrBrPrb2"},
                {"name": "prb_3","formula": "pmRadioRecInterferencePwrBrPrb3"},
                {"name": "prb_4","formula": "pmRadioRecInterferencePwrBrPrb4"},
                {"name": "prb_5","formula": "pmRadioRecInterferencePwrBrPrb5"},
                {"name": "prb_6","formula": "pmRadioRecInterferencePwrBrPrb6"},
                {"name": "prb_7","formula": "pmRadioRecInterferencePwrBrPrb7"},
                {"name": "prb_8","formula": "pmRadioRecInterferencePwrBrPrb8"},
                {"name": "prb_9","formula": "pmRadioRecInterferencePwrBrPrb9"},
                {"name": "prb_10","formula": "pmRadioRecInterferencePwrBrPrb10"},
                {"name": "prb_11","formula": "pmRadioRecInterferencePwrBrPrb11"},
                {"name": "prb_12","formula": "pmRadioRecInterferencePwrBrPrb12"},
                {"name": "prb_13","formula": "pmRadioRecInterferencePwrBrPrb13"},
                {"name": "prb_14","formula": "pmRadioRecInterferencePwrBrPrb14"},
                {"name": "prb_15","formula": "pmRadioRecInterferencePwrBrPrb15"},
                {"name": "prb_16","formula": "pmRadioRecInterferencePwrBrPrb16"},
                {"name": "prb_17","formula": "pmRadioRecInterferencePwrBrPrb17"},
                {"name": "prb_18","formula": "pmRadioRecInterferencePwrBrPrb18"},
                {"name": "prb_19","formula": "pmRadioRecInterferencePwrBrPrb19"},
                {"name": "prb_20","formula": "pmRadioRecInterferencePwrBrPrb20"},
                {"name": "prb_21","formula": "pmRadioRecInterferencePwrBrPrb21"},
                {"name": "prb_22","formula": "pmRadioRecInterferencePwrBrPrb22"},
                {"name": "prb_23","formula": "pmRadioRecInterferencePwrBrPrb23"},
                {"name": "prb_24","formula": "pmRadioRecInterferencePwrBrPrb24"},
                {"name": "prb_25","formula": "pmRadioRecInterferencePwrBrPrb25"},
                {"name": "prb_26","formula": "pmRadioRecInterferencePwrBrPrb26"},
                {"name": "prb_27","formula": "pmRadioRecInterferencePwrBrPrb27"},
                {"name": "prb_28","formula": "pmRadioRecInterferencePwrBrPrb28"},
                {"name": "prb_29","formula": "pmRadioRecInterferencePwrBrPrb29"},
                {"name": "prb_30","formula": "pmRadioRecInterferencePwrBrPrb30"},
                {"name": "prb_31","formula": "pmRadioRecInterferencePwrBrPrb31"},
                {"name": "prb_32","formula": "pmRadioRecInterferencePwrBrPrb32"},
                {"name": "prb_33","formula": "pmRadioRecInterferencePwrBrPrb33"},
                {"name": "prb_34","formula": "pmRadioRecInterferencePwrBrPrb34"},
                {"name": "prb_35","formula": "pmRadioRecInterferencePwrBrPrb35"},
                {"name": "prb_36","formula": "pmRadioRecInterferencePwrBrPrb36"},
                {"name": "prb_37","formula": "pmRadioRecInterferencePwrBrPrb37"},
                {"name": "prb_38","formula": "pmRadioRecInterferencePwrBrPrb38"},
                {"name": "prb_39","formula": "pmRadioRecInterferencePwrBrPrb39"},                
                {"name": "prb_40","formula": "pmRadioRecInterferencePwrBrPrb40"},
                {"name": "prb_41","formula": "pmRadioRecInterferencePwrBrPrb41"},
                {"name": "prb_42","formula": "pmRadioRecInterferencePwrBrPrb42"},
                {"name": "prb_43","formula": "pmRadioRecInterferencePwrBrPrb43"},
                {"name": "prb_44","formula": "pmRadioRecInterferencePwrBrPrb44"},
                {"name": "prb_45","formula": "pmRadioRecInterferencePwrBrPrb45"},
                {"name": "prb_46","formula": "pmRadioRecInterferencePwrBrPrb46"},
                {"name": "prb_47","formula": "pmRadioRecInterferencePwrBrPrb47"},
                {"name": "prb_48","formula": "pmRadioRecInterferencePwrBrPrb48"},
                {"name": "prb_49","formula": "pmRadioRecInterferencePwrBrPrb49"},
                {"name": "prb_50","formula": "pmRadioRecInterferencePwrBrPrb50"},
                {"name": "prb_51","formula": "pmRadioRecInterferencePwrBrPrb51"},
                {"name": "prb_52","formula": "pmRadioRecInterferencePwrBrPrb52"},
                {"name": "prb_53","formula": "pmRadioRecInterferencePwrBrPrb53"},
                {"name": "prb_54","formula": "pmRadioRecInterferencePwrBrPrb54"},
                {"name": "prb_55","formula": "pmRadioRecInterferencePwrBrPrb55"},
                {"name": "prb_56","formula": "pmRadioRecInterferencePwrBrPrb56"},
                {"name": "prb_57","formula": "pmRadioRecInterferencePwrBrPrb57"},
                {"name": "prb_58","formula": "pmRadioRecInterferencePwrBrPrb58"},
                {"name": "prb_59","formula": "pmRadioRecInterferencePwrBrPrb59"},
                {"name": "prb_60","formula": "pmRadioRecInterferencePwrBrPrb60"},
                {"name": "prb_61","formula": "pmRadioRecInterferencePwrBrPrb61"},
                {"name": "prb_62","formula": "pmRadioRecInterferencePwrBrPrb62"},
                {"name": "prb_63","formula": "pmRadioRecInterferencePwrBrPrb63"},
                {"name": "prb_64","formula": "pmRadioRecInterferencePwrBrPrb64"},
                {"name": "prb_65","formula": "pmRadioRecInterferencePwrBrPrb65"},
                {"name": "prb_66","formula": "pmRadioRecInterferencePwrBrPrb66"},
                {"name": "prb_67","formula": "pmRadioRecInterferencePwrBrPrb67"},
                {"name": "prb_68","formula": "pmRadioRecInterferencePwrBrPrb68"},
                {"name": "prb_69","formula": "pmRadioRecInterferencePwrBrPrb69"},
                {"name": "prb_70","formula": "pmRadioRecInterferencePwrBrPrb70"},
                {"name": "prb_71","formula": "pmRadioRecInterferencePwrBrPrb71"},
                {"name": "prb_72","formula": "pmRadioRecInterferencePwrBrPrb72"},
                {"name": "prb_73","formula": "pmRadioRecInterferencePwrBrPrb73"},
                {"name": "prb_74","formula": "pmRadioRecInterferencePwrBrPrb74"},
                {"name": "prb_75","formula": "pmRadioRecInterferencePwrBrPrb75"},
                {"name": "prb_76","formula": "pmRadioRecInterferencePwrBrPrb76"},
                {"name": "prb_77","formula": "pmRadioRecInterferencePwrBrPrb77"},
                {"name": "prb_78","formula": "pmRadioRecInterferencePwrBrPrb78"},
                {"name": "prb_79","formula": "pmRadioRecInterferencePwrBrPrb79"},
                {"name": "prb_80","formula": "pmRadioRecInterferencePwrBrPrb70"},
                {"name": "prb_81","formula": "pmRadioRecInterferencePwrBrPrb81"},
                {"name": "prb_82","formula": "pmRadioRecInterferencePwrBrPrb82"},
                {"name": "prb_83","formula": "pmRadioRecInterferencePwrBrPrb83"},
                {"name": "prb_84","formula": "pmRadioRecInterferencePwrBrPrb84"},
                {"name": "prb_85","formula": "pmRadioRecInterferencePwrBrPrb85"},
                {"name": "prb_86","formula": "pmRadioRecInterferencePwrBrPrb86"},
                {"name": "prb_87","formula": "pmRadioRecInterferencePwrBrPrb87"},
                {"name": "prb_88","formula": "pmRadioRecInterferencePwrBrPrb88"},
                {"name": "prb_89","formula": "pmRadioRecInterferencePwrBrPrb89"},
                {"name": "prb_90","formula": "pmRadioRecInterferencePwrBrPrb90"},
                {"name": "prb_91","formula": "pmRadioRecInterferencePwrBrPrb91"},
                {"name": "prb_92","formula": "pmRadioRecInterferencePwrBrPrb92"},
                {"name": "prb_93","formula": "pmRadioRecInterferencePwrBrPrb93"},
                {"name": "prb_94","formula": "pmRadioRecInterferencePwrBrPrb94"},
                {"name": "prb_95","formula": "pmRadioRecInterferencePwrBrPrb95"},
                {"name": "prb_96","formula": "pmRadioRecInterferencePwrBrPrb96"},
                {"name": "prb_97","formula": "pmRadioRecInterferencePwrBrPrb97"},
                {"name": "prb_98","formula": "pmRadioRecInterferencePwrBrPrb98"},
                {"name": "prb_99","formula": "pmRadioRecInterferencePwrBrPrb99"},
                {"name": "prb_100","formula": "pmRadioRecInterferencePwrBrPrb100"},    
          ],
          "Daterange": self.date_range,
          "TimeZoneFilter": "local",
          "TimeZoneOutput": "local",
          "MaxNumberOfRows": "1000000",
          "IgnoreMissingCounters": "True",
          "ConvertDistFrom1to0Based": "false"
        }        
         
        # Process the JSON request
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        self.json_copy = self.json.copy()
        self.cols  = self.json_copy.pop(0)
        self.df_pm_branch = pd.DataFrame(self.json_copy, columns=self.cols)           
        self.rows = len(self.df_pm_branch)        
                       
        # Convert PRB RSSI counters to RSSI
        #self.df_pm_branch[self.prb_list] = self.df_pm_branch[self.prb_list].applymap(lambda x: (10* np.log10(int(x)* 0.00000000000005684341886080800 /(900 * self.rops * 1000/40))) if (int(x)>0) else np.NaN)
        #    self.df_pm_branch[self.prb_list] = self.df_pm_branch[self.prb_list].applymap(lambda x: (10* np.log10(int(x)* (0.00000000000005684341886080800) /90000)) if (int(x)>0) else np.NaN)           
            
        # Miscellaneous other conversions
        self.df_pm_branch['datetime']        = pd.to_datetime(self.df_pm_branch['RecordDate'])

        #Split out the objectId
        self.df_pm_branch[["ne","ManagedElement","ENodeBFunction","SectorCarrier","PmUlInterferenceReport"]] = self.df_pm_branch['ObjectId'].str.split(",",expand=True)

        # Then parse the individual strings back into their own columns
        self.df_pm_branch["eNodeB"]         = self.df_pm_branch["ManagedElement"].str.split("=",expand=True)[1]
        self.df_pm_branch["ENodeBFunction"] = self.df_pm_branch["ENodeBFunction"].str.split("=",expand=True)[1]
        self.df_pm_branch["SectorCarrier"]  = self.df_pm_branch["SectorCarrier"].str.split("=",expand=True)[1]
        self.df_pm_branch["Branch"]         = self.df_pm_branch["PmUlInterferenceReport"].str.split("=",expand=True)[1]        
               
        # Calculate average, calculate used
        #self.df_pm_branch['avg_prb_rssi']   = self.df_pm_branch[self.prb_list].mean(axis=1, skipna=True)
        #self.df_pm_branch['used_PRBs']      = self.df_pm_branch[self.prb_list].count(axis=1)  
            
        #print("A")
        # Merge with date template and fill blanks with zeros
        self.df_pm_branch = self.time_template.merge(self.df_pm_branch,how='left', on='datetime')
        
        #print("B")
        # Convert NaNs to zeros
        self.df_pm_branch[self.prb_list] = self.df_pm_branch[self.prb_list].replace(np.nan, 0.0)                           
            
        # Drop unnecessary columns
        self.df_pm_branch = self.df_pm_branch.drop(['DateInTicks','DateInHours','DateInDays'],axis =1)        
        
        # Sort the dataframe
        self.df_pm_branch.sort_values(['ObjectId','datetime'], inplace=True)  
        self.df_pm_branch.reset_index(drop=False, inplace=True)
        
        self.result_status = ""
        
        result = {}                 
        result['result'] = self.df_pm_branch
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['result_status'] = self.result_status
        result['JSON'] = self.json   
        result['columns'] = self.cols
        result['rows'] = self.rows
        result['customer'] = self.customer
        result['key'] = self.key
        result['date_range'] = self.date_range
        result['technology'] = self.tech
        result['time_resolution'] = self.time_resolution
        result['eNodeB'] = self.eNodeB
        result['Cell'] = self.Cell        
        result['body'] = self.body
        
        return result

# %%
class DopQueries(DopQueries):

    def arfcn_to_freq(self, arfcn, direction, output):
        
        self.arfcn = arfcn
        #self.ul_dl = ul_dl
        self.direction = direction
        self.output = output
        self.region= self.freq_region_lte
        self.result = {}
        
        self.valid = True
        self.freq = 0.0
        self.returned_band_count = 0
        self.band_id = ""
        self.band_name = ""
        self.band_group = "" 
        self.F_low = 0
        self.N_offset = 0
        self.flag = "Not Found"
       
        #print(f"A ARFCN:{self.arfcn}, Dir:{self.direction}, {self.output}, {self.region}")
    
        if self.direction.upper() == "DL":
            
            self.dfConvert = self.lookupLTE[(self.lookupLTE['DL_EARFCN_LO']<=int(self.arfcn)) & (self.lookupLTE['DL_EARFCN_HI']>int(self.arfcn)) & (self.lookupLTE['GEO_AREA']==self.region)] 
            self.returned_band_count = self.dfConvert.shape[0]
            self.conversion = (self.dfConvert[:1].to_dict(orient='list'))
    
            if self.returned_band_count > 0:
    
                self.band_id = (self.conversion['BAND_ID'])[0]
                self.band_name = self.conversion['NAME'][0]
                self.band_group = self.conversion['BAND_GROUP'][0]
                self.region= self.conversion['GEO_AREA'][0]
                self.mode = self.conversion['MODE'][0]      
                self.F_low = int(self.conversion['DL_FREQ_LO'][0])
                self.N_offset = int(self.conversion['DL_EARFCN_LO'][0])
                self.freq = self.F_low + 0.1 * (int(self.arfcn) - self.N_offset)
                self.flag = f'DL CALC:{self.arfcn},{self.F_low},{self.N_offset},{self.freq}'
                #flag = f'DL CALC: arfcn:{arfcn}, Ful_low:{self.returned_band_count}, Nul_offset:{tech}' 
                
            else:
                # Need to check for "Global "
                self.dfConvert = self.lookupLTE[(self.lookupLTE['DL_EARFCN_LO']<=int(self.arfcn)) & (self.lookupLTE['DL_EARFCN_HI']>int(self.arfcn)) & (self.lookupLTE['GEO_AREA']=='Global')] 
                self.returned_band_count = self.dfConvert.shape[0]
                self.conversion = (self.dfConvert[:1].to_dict(orient='list'))
                
                if self.returned_band_count > 0:
                    self.band_id = (self.conversion['BAND_ID'])[0]
                    self.band_name = self.conversion['NAME'][0]
                    self.band_group = self.conversion['BAND_GROUP'][0]
                    self.region = self.conversion['GEO_AREA'][0]
                    self.mode = self.conversion['MODE'][0]      
                    self.F_low = int(self.conversion['DL_FREQ_LO'][0])
                    self.N_offset = int(self.conversion['DL_EARFCN_LO'][0])
                    self.freq = self.F_low + 0.1 * (int(self.arfcn) - self.N_offset)
                    self.flag = f'DL CALC:{self.arfcn},{self.F_low},{self.N_offset},{self.freq} from {self.region}'               
                
                else:
                    self.valid = False
                    self.freq = 0.0
                    self.flag = f'NOT FOUND:{self.arfcn},{self.tech},{self.direction},{self.output},{self.region}'
     
                
        elif self.direction.upper() == "UL":
            
            self.dfConvert = self.lookupLTE[(self.lookupLTE['UL_EARFCN_LO']<=int(self.arfcn)) & (self.lookupLTE['UL_EARFCN_HI']>int(self.arfcn)) & (self.lookupLTE['GEO_AREA']==self.region)] 
            self.returned_band_count = self.dfConvert.shape[0]
            self.conversion = (self.dfConvert[:1].to_dict(orient='list'))
    
            if self.returned_band_count > 0:
      
                self.band_id = (self.conversion['BAND_ID'])[0]
                self.band_name = self.conversion['NAME'][0]
                self.band_group = self.conversion['BAND_GROUP'][0]
                self.region= self.conversion['GEO_AREA'][0]
                self.mode = self.conversion['MODE'][0]              
                self.F_low = int(self.conversion['UL_FREQ_LO'][0])
                self.N_offset = int(self.conversion['UL_EARFCN_LO'][0])
                self.freq = self.F_low + 0.1 * (int(self.arfcn) - self.N_offset)
                self.flag = f'UL CALC:{self.arfcn},{self.F_low},{self.N_offset},{self.freq}'
                #flag = f'UL CALC: arfcn:{self.arfcn}, Ful_low:{self.returned_band_count}, Nul_offset:{self.N_offset}' 
            else:
                # if No conversions, need to check for global values
                
                # Need to check for "Global "
                self.dfConvert = self.lookupLTE[(self.lookupLTE['UL_EARFCN_LO']<=int(self.arfcn)) & (self.lookupLTE['UL_EARFCN_HI']>int(self.arfcn)) & (self.lookupLTE['GEO_AREA']=='Global')] 
                self.returned_band_count = self.dfConvert.shape[0]
                self.conversion = (self.dfConvert[:1].to_dict(orient='list'))                
                
                if self.returned_band_count > 0:
                    self.band_id = (self.conversion['BAND_ID'])[0]
                    self.band_name = self.conversion['NAME'][0]
                    self.band_group = self.conversion['BAND_GROUP'][0]
                    self.region = self.conversion['GEO_AREA'][0]
                    self.mode = self.conversion['MODE'][0]      
                    self.F_low = int(conversion['DL_FREQ_LO'][0])
                    #self.freq = self.F_low + 0.1 * (int(self.arfcn) - self.N_offset)
                    self.freq = self.F_low + 0.1 * (int(self.arfcn) - self.N_offset)
                    self.flag = f'UL CALC:{self.arfcn},{self.F_low},{self.N_offset},{self.freq} from {self.region}'                         
                
                else:
                    self.valid = False
                    self.freq = 0.0
                    self.flag = f'NOT FOUND:{self.arfcn},{self.tech},{self.direction},{self.output},{self.region}'         

        
        #print(f"I ARFCN:{self.arfcn}, Dir:{self.direction}, {self.output}, {self.region}")
        #print(f"I freq:{self.freq}, id:{self.band_id}, name:{self.band_name}, group:{self.band_group}")
        #def convert_freq(arfcn, tech, direction, out, region):
    
        # Select the output
        if self.output.upper() == "FREQ":
            self.result = f'{self.freq:8.3f}'
        elif self.output.upper() == "BAND_ID":
            self.result = self.band_id
        elif self.output.upper() == "BAND_NAME":
            self.result = self.band_name
        elif self.output.upper() == "BAND_GROUP":
            self.result = self.band_group
        elif self.output.upper() == "COUNT":
            self.result = self.returned_band_count
        elif self.output.upper() == "REGION":
            self.result = self.region
        else:
            self.result = "INVALID OUT"    
    
        return self.result


# %%
class DopQueries(DopQueries):

    def placeholder(self):
        
        # Merge with date template and fill blanks with zeros
        #pm_data_process = pm_data_template.merge(pm_data_process,how='left', on='datetime')
        #pm_data_process[prb_col_list] = pm_data_process[prb_col_list].replace(np.nan, 0.0)
        
        result = ""
        
        return result
        
        

# %%
class DopQueries(DopQueries):

    def freq_band_ul_lower(self, row):
           
        #print(row)
        
        self.center_freq = row['freq_ul_ctr']
        self.prbs = row['PRBs']
        
        # PRB bandwidth is 180 kHz, or 0.180 MHz
                
        if math.isnan(float(self.prbs)) or self.center_freq == 0.0:
            #print("Blank")
            self.result_lower = 0.0
            self.result_upper = 0.0
        else:
            #print("Calc")
            self.result_lower = float(self.center_freq) - (int(self.prbs) * 0.180 / 2)
            self.result_upper = float(self.center_freq) + (int(self.prbs) * 0.180 / 2)

        print("Lower:{self.result_lower}, Ctr:{self.center_freq} Uppwer:{self.result_upper}")
        
        return self.result_lower


# %%
class DopQueries(DopQueries):

    def freq_band_ul_upper(self, row):
           
        #print(row)
        
        self.center_freq = row['freq_ul_ctr']
        self.prbs = row['PRBs']
        
        # PRB bandwidth is 180 kHz, or 0.180 MHz
                
        if math.isnan(float(self.prbs)) or self.center_freq == 0.0:
            #print("Blank")
            self.result_lower = 0.0
            self.result_upper = 0.0
        else:
            #print("Calc")
            self.result_lower = float(self.center_freq) - (int(self.prbs) * 0.180 / 2)
            self.result_upper = float(self.center_freq) + (int(self.prbs) * 0.180 / 2)

        print("Lower:{self.result_lower}, Ctr:{self.center_freq} Uppwer:{self.result_upper}")
        
        return self.result_upper


# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM Master Query for a whole eNodeB
    def cm_masterquery_gnodeb_full(self,eNodeB_list,Cell):
        
        self.eNodeB_list = eNodeB_list
        #self.Cell = Cell
        
        #print(f'CM Master Qry for cust: {self.customer} eNodeB:{self.eNodeB_list}, Cell:{self.Cell} - SINGLE CELL')
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMMasterQuery/{self.customer}/{self.vendor}/{"eNR"}?apikey=' + self.key
        self.body = {
            "Customer": self.customer,
            "Attributes": [
            "gNodeB",
            "Cell",
            #"vsDataSectorCarrier",
            #"Duplex Mode",
            "txDirection",
            "freqBand",
            "earfcndl",
            "earfcnul",
            "dlChannelBandwidth",
            "ulChannelBandwidth",
            "#administrativeState",
            "operationalState",
            #"cellBarred",    
            #"availabilityStatus",            
            #"IndoorOutdoor",
            "cellRange",
            "Azimuth",
            "latitude",
            #"longitude",
            "Radio ProductName",
            "Radio productNumber",
            #"swVersion",
            #"swRelease",
            #"DU ProductName",
            #"DU ProductNumber",
            "noOfTxAntennas",
            #"cellRange",
            #"alpha",
            #"transmissionMode",
            #"additionalFreqBandList",  Not supported
            #"ailgActive",
            #"pucchOverdimensioning",
            #"ulConfigurableFrequencyStart",
            #"ulFrequencyAllocationProportion",
            #"ulInterferenceManagementActive",
            #"ulInterferenceManagementDuration", Not supported
            #"ulInternalChannelBandwidth", Not supported
            #"ulImprovedUeSchedLastEnabled", Not supported
            #"ulSchedCtrlForOocUesEnabled", Not supported
            #"puschMaxNrOfPrbsPerUe",
            #"puschFrequencyAllocationBr", Not supported
            #"auPortRef",
            #"pZeroNominalPucch",
            #"pZeroNominalPusch",
            #"NeighborListWithDistance",
            #"dlAttenuation", use RfBranch instead
            #"eNBId",
            #"cellId",
            #"RelationId", "globalCellId",
            #"pci",                
        ],
        "Filter_SubNetworkLevel": 1,
        #"Filter_SubNetwork",
        #"Filter_Carriers:[]",
        #"Filter_Spatial:"(,)(,)",
        #"Filter_glbalCellId":[],
        #"Filter_cell": [ncell_mq],
        #"Filter_Cell_Operator":customer
        #"Filter_Rnc":rnc #WCDMA
        "Filter_eNodeB": eNodeB_list,
        #"Filter_cell": [Cell]
        } 
        
        self.result_status = "JSON start"     
        self.json = ""
        self.resp_status_code = ""
        self.resp_headers = ""
        self.cols = []
        self.rows = 0        
        
        try:
        
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
            #self.json_copy = self.json.copy()
            #self.cols  = self.json_copy.pop(0)
            self.df_cm_enodeb = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')

            self.cols = self.df_cm_enodeb.columns.values.tolist()
            self.rows = len(self.df_cm_enodeb)
        
            if "eNodeB" in self.cols:
                self.result_status = "Ok"
            else:
                self.result_status = "JSON Error"            
        
        except:
        
            self.result_status = "Code Error"

        
        # Process the dataframe
        if self.result_status == "Ok":
        
            self.df_cm_enodeb['Tech']     = "eNR"
            # New cells created from processed data cells        
            self.df_cm_enodeb['ObjectId'] = self.df_cm_enodeb['eNodeB']+'.'+self.df_cm_enodeb['Cell']
            #self.df_cm_enodeb['PRBs']     = self.df_cm_enodeb['dlChannelBandwidth'].map(self.lte_bw_to_prbs)
            self.df_cm_enodeb['PRBs']     = 0
            
            # Split out the refSectorCarrier number if present
            #self.df_cm_enodeb["refSectorCarrier"] = self.df_cm_enodeb["vsDataSectorCarrier"].apply(lambda x: (x.split('.',1))[1] if x is not None else x)
        
            # We also have to convert the earfcndls to frequencies, then convert the band edges
            #self.df_cm_enodeb['freq_dl_ctr'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','FREQ'))
            #self.df_cm_enodeb['freq_ul_ctr'] = self.df_cm_enodeb['earfcnul'].apply(self.arfcn_to_freq, args=('UL','FREQ'))
            #self.df_cm_enodeb['freq_band_id']    = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_ID'))
            #self.df_cm_enodeb['freq_name']      = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_NAME'))
            #self.df_cm_enodeb['freq_band_group'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_GROUP'))
            #self.df_cm_enodeb['freq_region'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','REGION'))

            self.test_list = ['freq_ul_ctr','PRBs']
            self.df_cm_enodeb['freq_ul_lower'] = 0.0
            self.df_cm_enodeb['freq_ul_upper'] = 0.0
            self.df_cm_enodeb['freq_ul_lower'] = self.df_cm_enodeb[self.test_list].apply(self.freq_band_ul_lower, axis=1)
            self.df_cm_enodeb['freq_ul_upper'] = self.df_cm_enodeb[self.test_list].apply(self.freq_band_ul_upper, axis=1)
            
        #self.df_pm_cell['pusch_rssi']  = self.df_pm_cell.apply(self.weighted_avg_rssi_pusch, axis=1)            
            
        # Output final result
        result = {}      
        result['result'] = self.df_cm_enodeb
        result['result_status'] = self.result_status
        #result['type_of_query'] = self.type_of_query
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB_list'] = self.eNodeB_list
        #result['Cell'] = self.Cell        
        result['body'] = self.body
        result['url'] = self.url
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM Master Query for a whole eNodeB
    def cm_masterquery_subnetwork(self,market_list):
        
        #self.eNodeB_list = eNodeB_list
        #self.Cell = Cell
        self.market_list = market_list
        
        #print(f'CM Master Qry for cust: {self.customer} eNodeB:{self.eNodeB_list}, Cell:{self.Cell} - SINGLE CELL')
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMMasterQuery/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "Customer": self.customer,
            "Attributes": [
            "eNodeB",
            "Cell",
            "vsDataSectorCarrier",
            "Duplex Mode",
            "freqBand",
            "earfcndl",
            "earfcnul",
            "dlChannelBandwidth",
            "ulChannelBandwidth",
            "administrativeState",
            "operationalState",
            "cellBarred",    
            "availabilityStatus",            
            "IndoorOutdoor",
            "cellRange",
            "Azimuth",
            "latitude",
            "longitude",
            "Radio productName",
            "Radio productNumber",
            "swVersion",
            "swRelease",
            "DU ProductName",
            "DU ProductNumber",
            "cellRange",
            "alpha",
            "transmissionMode",
            #"additionalFreqBandList",  Not supported
            "ailgActive",
            "pucchOverdimensioning",
            "ulConfigurableFrequencyStart",
            "ulFrequencyAllocationProportion",
            "ulInterferenceManagementActive",
            #"ulInterferenceManagementDuration", Not supported
            #"ulInternalChannelBandwidth", Not supported
            #"ulImprovedUeSchedLastEnabled", Not supported
            #"ulSchedCtrlForOocUesEnabled", Not supported
            "puschMaxNrOfPrbsPerUe",
            #"puschFrequencyAllocationBr", Not supported
            "auPortRef",
            "pZeroNominalPucch",
            "pZeroNominalPusch",
            "NeighborListWithDistance",
            #"dlAttenuation", use RfBranch instead
            "eNBId",
            "cellId",
            "RelationId", "globalCellId",
            "pci",                
        ],
        "Filter_SubNetworkLevel": 1,
        "Filter_SubNetwork": market_list,
        #"Filter_Carriers:[]",
        #"Filter_Spatial:"(,)(,)",
        #"Filter_glbalCellId":[],
        #"Filter_cell": [ncell_mq],
        #"Filter_Cell_Operator":customer
        #"Filter_Rnc":rnc #WCDMA
        #"Filter_eNodeB": eNodeB_list,
        #"Filter_cell": [Cell]
        } 
        
        self.result_status = "JSON start"        
        self.json = ""
        self.resp_status_code = ""
        self.resp_headers = ""
        self.cols = []
        self.rows = 0           
        
        try:
        
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
            #self.json_copy = self.json.copy()
            #self.cols  = self.json_copy.pop(0)
            self.df_cm_enodeb = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')

            self.cols = self.df_cm_enodeb.columns.values.tolist()
            self.rows = len(self.df_cm_enodeb)
        
            if "eNodeB" in self.cols:
                self.result_status = "Ok"
            else:
                self.result_status = "Error"            
                
        except:
        
            self.result_status = "Code Error"
             
                
        
        # Process the dataframe
        if self.result_status == "Ok":   
            
            self.df_cm_enodeb['Tech']     = "e"  
            
            # New cells created from processed data cells        
            self.df_cm_enodeb['ObjectId'] = self.df_cm_enodeb['eNodeB']+'.'+self.df_cm_enodeb['Cell']
            self.df_cm_enodeb['PRBs']     = self.df_cm_enodeb['dlChannelBandwidth'].map(self.lte_bw_to_prbs)
            
            # Split out the refSectorCarrier number if present
            self.df_cm_enodeb["refSectorCarrier"] = self.df_cm_enodeb["vsDataSectorCarrier"].apply(lambda x: (x.split('.',1))[1] if x is not None else x)
        
            # We also have to convert the earfcndls to frequencies, then convert the band edges
            self.df_cm_enodeb['freq_dl_ctr'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','FREQ'))
            self.df_cm_enodeb['freq_ul_ctr'] = self.df_cm_enodeb['earfcnul'].apply(self.arfcn_to_freq, args=('UL','FREQ'))
            self.df_cm_enodeb['freq_band_id']    = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_ID'))
            self.df_cm_enodeb['freq_name']      = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_NAME'))
            self.df_cm_enodeb['freq_band_group'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_GROUP'))
            self.df_cm_enodeb['freq_region'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','REGION'))

            self.test_list = ['freq_ul_ctr','PRBs']
            self.df_cm_enodeb['freq_ul_lower'] = 0.0
            self.df_cm_enodeb['freq_ul_upper'] = 0.0
            self.df_cm_enodeb['freq_ul_lower'] = self.df_cm_enodeb[self.test_list].apply(self.freq_band_ul_lower, axis=1)
            self.df_cm_enodeb['freq_ul_upper'] = self.df_cm_enodeb[self.test_list].apply(self.freq_band_ul_upper, axis=1)
            
        #self.df_pm_cell['pusch_rssi']  = self.df_pm_cell.apply(self.weighted_avg_rssi_pusch, axis=1)            
            
        # Output final result
        result = {}      
        result['result'] = self.df_cm_enodeb
        result['result_status'] = self.result_status
        #result['type_of_query'] = self.type_of_query
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB_list'] = self.eNodeB_list
        #result['Cell'] = self.Cell        
        result['body'] = self.body
        result['url'] = self.url
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM Master Query for a whole eNodeB
    def cm_masterquery_enodeb(self,eNodeB_list,Cell):
        
        self.eNodeB_list = eNodeB_list
        self.Cell = Cell
        
        #print(f'CM Master Qry for cust: {self.customer} eNodeB:{self.eNodeB_list}, Cell:{self.Cell} - SINGLE CELL')
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMMasterQuery/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "Customer": self.customer,
            "Attributes": [
            "eNodeB",
            "Cell",
            "vsDataSectorCarrier",
            "Duplex Mode",
            "freqBand",
            "earfcndl",
            "earfcnul",
            "dlChannelBandwidth",
            "ulChannelBandwidth",
            "administrativeState",
            "operationalState",
            "cellBarred",    
            "availabilityStatus",            
            "IndoorOutdoor",
            "cellRange",
            "Azimuth",
            "latitude",
            "longitude",
            "Radio productName",
            "Radio productNumber",
            "swVersion",
            "swRelease",
            "DU ProductName",
            "DU ProductNumber",
            "cellRange",
            "alpha",
            "transmissionMode",
            #"additionalFreqBandList",  Not supported
            "ailgActive",
            "pucchOverdimensioning",
            "ulConfigurableFrequencyStart",
            "ulFrequencyAllocationProportion",
            "ulInterferenceManagementActive",
            #"ulInterferenceManagementDuration", Not supported
            #"ulInternalChannelBandwidth", Not supported
            #"ulImprovedUeSchedLastEnabled", Not supported
            #"ulSchedCtrlForOocUesEnabled", Not supported
            "puschMaxNrOfPrbsPerUe",
            #"puschFrequencyAllocationBr", Not supported
            "auPortRef",
            "pZeroNominalPucch",
            "pZeroNominalPusch",
            "NeighborListWithDistance",
            #"dlAttenuation", use RfBranch instead
            "eNBId",
            "cellId",
            "RelationId", "globalCellId",
            "pci",                
        ],
        "Filter_SubNetworkLevel": 1,
        #"Filter_SubNetwork",
        #"Filter_Carriers:[]",
        #"Filter_Spatial:"(,)(,)",
        #"Filter_glbalCellId":[],
        #"Filter_cell": [ncell_mq],
        #"Filter_Cell_Operator":customer
        #"Filter_Rnc":rnc #WCDMA
        "Filter_eNodeB": eNodeB_list,
        #"Filter_cell": [Cell]
        } 
        
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        #self.json_copy = self.json.copy()
        #self.cols  = self.json_copy.pop(0)
        self.df_cm_enodeb = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')

        self.cols = self.df_cm_enodeb.columns.values.tolist()
        self.rows = len(self.df_cm_enodeb)
        
        if "eNodeB" in self.cols:
            self.result_status = "Ok"
        else:
            self.result_status = "Error"            
        
        # Process the dataframe
        if self.result_status == "Ok":
        
            self.df_cm_enodeb['Tech']     = "eLTE"             
        
            # New cells created from processed data cells        
            self.df_cm_enodeb['ObjectId'] = self.df_cm_enodeb['eNodeB']+'.'+self.df_cm_enodeb['Cell']
            self.df_cm_enodeb['PRBs']     = self.df_cm_enodeb['dlChannelBandwidth'].map(self.lte_bw_to_prbs)
            
            # Split out the refSectorCarrier number if present
            self.df_cm_enodeb["refSectorCarrier"] = self.df_cm_enodeb["vsDataSectorCarrier"].apply(lambda x: (x.split('.',1))[1] if x is not None else x)
        
            # We also have to convert the earfcndls to frequencies, then convert the band edges
            self.df_cm_enodeb['freq_dl_ctr'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','FREQ'))
            self.df_cm_enodeb['freq_ul_ctr'] = self.df_cm_enodeb['earfcnul'].apply(self.arfcn_to_freq, args=('UL','FREQ'))
            self.df_cm_enodeb['freq_band_id']    = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_ID'))
            self.df_cm_enodeb['freq_name']      = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_NAME'))
            self.df_cm_enodeb['freq_band_group'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_GROUP'))
            self.df_cm_enodeb['freq_region'] = self.df_cm_enodeb['earfcndl'].apply(self.arfcn_to_freq, args=('DL','REGION'))

            self.test_list = ['freq_ul_ctr','PRBs']
            self.df_cm_enodeb['freq_ul_lower'] = 0.0
            self.df_cm_enodeb['freq_ul_upper'] = 0.0
            self.df_cm_enodeb['freq_ul_lower'] = self.df_cm_enodeb[self.test_list].apply(self.freq_band_ul_lower, axis=1)
            self.df_cm_enodeb['freq_ul_upper'] = self.df_cm_enodeb[self.test_list].apply(self.freq_band_ul_upper, axis=1)
            
        #self.df_pm_cell['pusch_rssi']  = self.df_pm_cell.apply(self.weighted_avg_rssi_pusch, axis=1)            
            
        # Output final result
        result = {}      
        result['result'] = self.df_cm_enodeb
        result['result_status'] = self.result_status
        #result['type_of_query'] = self.type_of_query
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB_list'] = self.eNodeB_list
        #result['Cell'] = self.Cell        
        result['body'] = self.body
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):

    def api_data_retrieval(self,url,body):
    
        #import requests
        #from requests import Request, Session
        self.s = Session()
        self.time = 900
        self.req = Request('POST', url, json= body)
        self.prepped = self.req.prepare()
        self.resp = self.s.send(self.prepped,timeout=self.time)
        #print("Rest API Query  Status :",self.resp.status_code)
        #print("---------------------------------------------------------------------------------------------------------------------------")
        #print("Rest APU Query Header :",self.resp.headers)
    
        self.data_json = self.resp.json()
    
        return self.resp.status_code, self.resp.headers, self.data_json

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    #def create_hourly_template(self,start_date,end_date):
    # This function creates a dataframe with one row per day over the user defined time period
    # The dataframe provides columns used to label dates on the output reports
    # Requires global vars 
    #     self.start_date - Starts at midnight
    #     self.end_date- Ends one time increment before midnight
    #     self.time_resolution - The time increment of the report
    
    #def create_time_template(self,time_resolution,start_date,end_date):
    def create_time_template(self):
    
        #self.start_date = start_date
        #self.end_date = end_date
        self.date_range = self.start_date+'-'+self.end_date
        #self.time_resolution = time_resolution    
        
        #print(f"TIME: {self.start_date}, {self.end_date}")        
    
        # Mod the start and end date with included hours
        self.date_test_start = datetime.strptime(self.start_date+' 00:00:00', '%Y/%m/%d %H:%M:%S')
        if self.time_resolution == 'ROP':        
            self.date_test_end = datetime.strptime(self.end_date+' 23:45:00', '%Y/%m/%d %H:%M:%S')
        elif self.time_resolution == 'Hourly':  
            self.date_test_end = datetime.strptime(self.end_date+' 23:00:00', '%Y/%m/%d %H:%M:%S')
        elif self.time_resolution == 'Daily':  
            self.date_test_end = datetime.strptime(self.end_date+' 00:00:00', '%Y/%m/%d %H:%M:%S')    
        else:
            self.date_test_end = datetime.strptime(self.end_date+' 23:00:00', '%Y/%m/%d %H:%M:%S')
            
        #print(f"Dates: {self.date_test_start} to {self.date_test_end}")
        self.date_delta = self.date_test_end - self.date_test_start

        # Make the template dataframe on an hourly basis      
        if self.time_resolution == 'ROP':           
            self.self.date_range = pd.date_range(start=self.date_test_start, end=self.date_test_end, freq='15T')
        elif self.time_resolution == 'Hourly':  
            self.date_range = pd.date_range(start=self.date_test_start, end=self.date_test_end, freq='1H')            
        elif self.time_resolution == 'Daily':    
            self.date_range = pd.date_range(start=self.date_test_start, end=self.date_test_end, freq='1D')
        else:        
            self.date_range = pd.date_range(start=self.date_test_start, end=self.date_test_end, freq='1H')          
        
        self.time_template = pd.DataFrame({'datetime':self.date_range})
        self.time_template['Date']           = self.time_template['datetime'].apply(lambda x: x.strftime('%m/%d'))        
        self.time_template['Hour']           = self.time_template['datetime'].apply(lambda x: x.strftime('%H'))
        self.time_template['Weekday']        = self.time_template['datetime'].apply(lambda x: x.strftime('%A'))      
        self.time_template['Weekday_abbrev'] = self.time_template['datetime'].apply(lambda x: self.day_of_week_abbreviations[x.strftime('%A')])  
        self.time_template['day_night']      = ""
        self.time_template['datetime_t'] = self.time_template['datetime']    
        
        return self.time_template
        

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    #def create_daily_template(self,start_date,end_date):
    # This function creates a dataframe with one row per day over the user defined time period
    # The dataframe provides columns used to label dates on the output reports
    # Requires global vars 
    #     self.start_date - The first day of the report
    #     self.end_date - The last day of the report, inclusive

    def create_daily_template(self):

        self.day_template_start = datetime.strptime(self.start_date+' 00:00:00', '%Y/%m/%d %H:%M:%S')  
        self.day_template_end   = datetime.strptime(self.end_date+' 00:00:00', '%Y/%m/%d %H:%M:%S')  
        
        #print(f"DAY: {self.start_date}, {self.end_date}")
        
        #print(f"Dates: {self.date_test_start} to {self.date_test_end}")
        self.date_delta = self.day_template_end - self.day_template_start
        
        # Daily date dataframe, used to create the master image date range
        self.day_range = pd.date_range(start=self.day_template_start, end=self.day_template_end, freq='1D')       
        self.day_template = pd.DataFrame({'datetime':self.day_range})          
        self.day_template['mon-day']        = self.day_template['datetime'].apply(lambda x: x.strftime('%m/%d'))   
        self.day_template['Weekday']        = self.day_template['datetime'].apply(lambda x: x.strftime('%A'))      
        self.day_template['Weekday_abbrev'] = self.day_template['datetime'].apply(lambda x: self.day_of_week_abbreviations[x.strftime('%A')])          
        
        return self.day_template        
        

# %%


# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM eUtranCellFDD Query for single cells
    def cm_query_eUtranCellFDD(self,eNodeB_list,record_date):
             
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(eNodeB_list)
        #self.key = self.dict_keys[self.customer]
        self.record_date = record_date
        self.table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataEUtranCellFDD"
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMData/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        # ColumnNames parameter is commented out so that all columns are returned
        self.body = {
            "TableName":self.table_name,
            #"ColumnNames":"auPortRef,ulAttenuation,ulAttenuationPerFqRange,ulTrafficDelay,ulTrafficDelayPerFqRange",
            "Nodes":self.eNodeB_str,
            "RecordDate":self.record_date,
            #"ENM":""
        } 
        
        # Run the query and create the output dataframe
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        self.df_eUtranCellFDD = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')   
            
        # Split column sectorCarrierRef into parts        
        self.df_eUtranCellFDD[[
            "y_SubNetworkMo",            
            "y_SubNetwork",
            "y_MeContext",
            "y_ManagedElement",
            "y_vsDataENodeBFunction",
            "vsDataSectorCarrier",
            ]] = self.df_eUtranCellFDD['sectorCarrierRef'].str.split(",",expand=True)               
                      
        # Split individual fields and save the value to same column
        self.df_eUtranCellFDD["vsDataSectorCarrier"]  = self.df_eUtranCellFDD["vsDataSectorCarrier"].str.split("=",expand=True)[1]
        
        # Drop unneded column names
        self.df_eUtranCellFDD = self.df_eUtranCellFDD.drop([
            "y_SubNetworkMo",
            "y_SubNetwork",
            "y_MeContext",
            "y_ManagedElement", 
            "y_vsDataENodeBFunction",
            ],axis=1)
        
        # Sort the dataframe
        self.df_eUtranCellFDD.sort_values(['ManagedElement','vsDataEUtranCellFDD'], inplace=True)                    
            
        # Output final result
        result = {}      
        result['result'] = self.df_eUtranCellFDD
        result['result_status'] = ""
        #result['type_of_query'] = self.type_of_query
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB_list'] = self.eNodeB_list
        result['eNodeB_str'] = self.eNodeB_list
        result['Cell'] = self.Cell
        result['record_date'] = self.record_date
        result['body'] = self.body
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.df_eUtranCellFDD.columns.values.tolist()
        result['rows'] = self.rows = len(self.df_eUtranCellFDD) 
        
        return result

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM PmUlInterferenceReport Query for enodeb list
    def cm_query_DataSectorCarrier(self,eNodeB_list,record_date):
             
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(eNodeB_list)
        #self.key = self.dict_keys[self.customer]
        self.record_date = record_date 
        self.table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataSectorCarrier"        
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMData/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        # ColumnNames parameter is commented out so that all columns are returned
        self.body = {
            "TableName":self.table_name,
            #"ColumnNames":"ManagedElement,eNodeB","vsDataSectorCarrier","vsDataPmUlInterferenceReport"
            "Nodes":self.eNodeB_str,
            "RecordDate":self.record_date,
            #"ENM":""
        } 
        
        # Run the JSON query
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        self.df_DataSectorCarrier = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')
        self.cols = self.df_DataSectorCarrier.columns.values.tolist()
        self.rows = len(self.df_DataSectorCarrier)           
                 
        # Split column sectorFunctionRef into parts        
        self.df_DataSectorCarrier[[
            "y_SubNetworkMo",            
            "y_SubNetwork",
            "y_MeContext",
            "y_ManagedElement",
            "vsDataNodeSupport",
            "vsDataSectorEquipmentFunction"]
            ] = self.df_DataSectorCarrier['sectorFunctionRef'].str.split(",",expand=True)               
                      
        self.df_DataSectorCarrier["vsDataNodeSupport"]             = self.df_DataSectorCarrier["vsDataNodeSupport"].str.split("=",expand=True)[1]
        self.df_DataSectorCarrier["vsDataSectorEquipmentFunction"] = self.df_DataSectorCarrier["vsDataSectorEquipmentFunction"].str.split("=",expand=True)[1]
        
        self.df_DataSectorCarrier = self.df_DataSectorCarrier.drop([
            "y_SubNetworkMo",
            "y_SubNetwork",
            "y_MeContext",
            "y_ManagedElement",      
            ],axis=1)
        
        self.df_DataSectorCarrier.sort_values(['ManagedElement','vsDataSectorCarrier'], inplace=True)        
        
        # Output final result
        result = {}      
        result['result'] = self.df_DataSectorCarrier
        #result['result_status'] = self.result_status
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB_list'] = self.eNodeB_list
        result['eNodeB_str'] = self.eNodeB_list
        result['Cell'] = self.Cell
        result['record_date'] = self.record_date
        result['body'] = self.body
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows
        
        return result

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM PmUlInterferenceReport Query for enodeb list
    def cm_query_PmUlInterferenceReport(self,eNodeB_list,record_date):
             
        #self.type_of_query = type_of_query
        #self.customer = customer
        #self.tech = tech
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(eNodeB_list)
        #self.Cell = Cell
        #self.key = self.dict_keys[self.customer]
        self.record_date = record_date 
        self.table_name = "SubNetwork_MeContext_ManagedElement_vsDataENodeBFunction_vsDataSectorCarrier_vsDataPmUlInterferenceReport"
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMData/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        # Column names is commented out so that all columns are returned
        self.body = {
            "TableName":self.table_name,
            #"ColumnNames":"ManagedElement,eNodeB","vsDataSectorCarrier","vsDataPmUlInterferenceReport"
            "Nodes":self.eNodeB_str,
            "RecordDate":self.record_date,
            #"ENM":""
        } 
        
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        #self.json_copy = self.json.copy()
        #self.cols  = self.json_copy.pop(0)
        self.df_PmUlInterferenceReport = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')

        self.cols = self.df_PmUlInterferenceReport.columns.values.tolist()
        self.rows = len(self.df_PmUlInterferenceReport)           
                 
        # Split rfBranchRxRef into parts

        self.df_PmUlInterferenceReport[["x_SubNetworkMo",
                 "x_SubNetwork",
                 "x_MeContext",
                 "x_ManagedElement",
                 "vsDataEquipment",
                 "vsDataAntennaUnitGroup",
                 "vsDataRfBranch"]
               ] = self.df_PmUlInterferenceReport['rfBranchRxRef'].str.split(",",expand=True)
            
            
        #self.df_PmUlInterferenceReport[["y_SubNetworkMo",
        #         "y_SubNetwork",
        #         "y_MeContext",
        #         "x_ManagedElement",
        #         "y_vsDataENodeBFunction",
        #         "y_,vsDataSectorCarrier",
        #         "y_vsDataPmUlInterferenceReport"]
        #       ] = self.df_PmUlInterferenceReport['ComponentInstance'].str.split(",",expand=True)            
            
        self.df_PmUlInterferenceReport["vsDataEquipment"]        = self.df_PmUlInterferenceReport["vsDataEquipment"].str.split("=",expand=True)[1]
        self.df_PmUlInterferenceReport["vsDataAntennaUnitGroup"] = self.df_PmUlInterferenceReport["vsDataAntennaUnitGroup"].str.split("=",expand=True)[1]
        self.df_PmUlInterferenceReport["vsDataRfBranch"]         = self.df_PmUlInterferenceReport["vsDataRfBranch"].str.split("=",expand=True)[1]
            
        self.df_PmUlInterferenceReport = self.df_PmUlInterferenceReport.drop([
                 "x_SubNetworkMo",
                 "x_SubNetwork",
                 "x_MeContext",
                 "x_ManagedElement",                         
                ],axis=1)
            
        # Output final result
        result = {}      
        result['result'] = self.df_PmUlInterferenceReport
        #result['result_status'] = self.result_status
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB_list'] = self.eNodeB_list
        result['eNodeB_str'] = self.eNodeB_list
        result['Cell'] = self.Cell
        result['record_date'] = self.record_date
        result['body'] = self.body
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows
        
        return result

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM Master Query for single cells
    def cm_masterquery_cell(self,eNodeB,Cell):
        
        #self.customer = customer
        #self.tech = tech
        self.eNodeB = eNodeB
        self.Cell = Cell
        #self.key = self.dict_keys[self.customer]
        
        print(f'CM Master Qry for cust: {self.customer} eNodeB:{self.eNodeB}, Cell:{self.Cell} - SINGLE CELL')
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMMasterQuery/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "Customer": self.customer,
            "Attributes": [
            "eNodeB",
            "Cell",
            "administrativeState",
            "vsDataSectorCarrier",
            "freqBand",
            "earfcndl",
            "earfcnul",
            "dlChannelBandwidth",
            "ulChannelBandwidth",
            "IndoorOutdoor",
            "Duplex Mode",
            "Azimuth",
            "latitude",
            "longitude",
            "altitude",
            "Radio productName",
            "Radio productNumber",
            "swVersion",
            "swRelease",
            "DU ProductName",
            "DU ProductNumber",
            "cellRange",
            "alpha",
            "pucchOverdimensioning",
            "ulConfigurableFrequencyStart",
            "dlAttenuation",
            "puschMaxNrOfPrbsPerUe",
            "auPortRef",
            "pZeroNominalPucch",
            "pZeroNominalPusch",
            "NeighborListWithDistance",
            "dlAttenuation",
            "eNBId",
            "cellId",
            "RelationId", 
            "globalCellId",
            "pci",   
        ],
        "Filter_SubNetworkLevel": 1,
        #"Filter_SubNetwork",
        #"Filter_eNodeB": ncm
        #"Filter_eNodeB": [nenodeb_mq],
        #"Filter_Carriers:[]",
        #"Filter_Spatial:"(,)(,)",
        #"Filter_glbalCellId":[],
        #"Filter_cell": [ncell_mq],
        #"Filter_Cell_Operator":customer
        #"Filter_Rnc":rnc #WCDMA
        "Filter_eNodeB": [eNodeB],
        "Filter_cell": [Cell]
        } 
        
        #print("Ping A")
        
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        #self.json_copy = self.json.copy()
        #self.cols  = self.json_copy.pop(0)
        self.df_cm_cell = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')

        self.cols = self.df_cm_cell.columns.values.tolist()
        self.rows = len(self.df_cm_cell)           
        
        if "eNodeB" in self.cols:
            self.result_status = "JSON Ok"
        else:
            self.result_status = "JSON Error"
        
        # Process the dataframe
        if self.result_status == "JSON Ok":
        
            self.df_cm_enodeb['Tech']     = "eLTE"             
        
            # New cells created from processed data cells        
            self.df_cm_cell['ObjectId'] = self.df_cm_cell['eNodeB']+'.'+self.df_cm_cell['Cell']
            self.df_cm_cell['PRBs']     = self.df_cm_cell['dlChannelBandwidth'].map(self.lte_bw_to_prbs)
                        
            # Split out the refSectorCarrier number if present
            self.df_cm_cell["refSectorCarrier"] = self.df_cm_cell["vsDataSectorCarrier"].apply(lambda x: (x.split('.',1))[1] if x is not None else x)
        
            # We also have to convert the earfcndls to frequencies, then convert the band edges
            self.df_cm_cell['freq_dl_ctr']     = self.df_cm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','FREQ'))
            self.df_cm_cell['freq_ul_ctr']     = self.df_cm_cell['earfcnul'].apply(self.arfcn_to_freq, args=('UL','FREQ'))
            self.df_cm_cell['freq_band_id']    = self.df_cm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_ID'))
            self.df_cm_cell['freq_name']       = self.df_cm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_NAME'))
            self.df_cm_cell['freq_band_group'] = self.df_cm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_GROUP'))
            self.df_cm_cell['freq_region']     = self.df_cm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','REGION'))
                
        #print("Ping B")        
        
        # Output final result
        result = {}      
        result['result'] = self.df_cm_cell
        result['result_status'] = self.result_status
        result['type_of_query'] = self.type_of_query
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB'] = self.eNodeB
        #result['Cell'] = self.Cell        
        result['body'] = self.body
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM ulAttenuation Query for single cells
    def cm_query_data_rf_branch(self,eNodeB_list,record_date):
      
        
        #self.type_of_query = type_of_query
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(eNodeB_list)
        #self.Cell = Cell
        self.record_date = record_date
        self.table_name = "SubNetwork_MeContext_ManagedElement_vsDataEquipment_vsDataAntennaUnitGroup_vsDataRfBranch"
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMData/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        
        # Column names is commented out so that all columns are returned
        self.body = {
            "TableName":self.table_name,
            #"ColumnNames":"auPortRef,ulAttenuation,ulAttenuationPerFqRange,ulTrafficDelay,ulTrafficDelayPerFqRange",
            "Nodes":self.eNodeB_str,
            "RecordDate":self.record_date,
            #"ENM":""
        } 
        
        try:
        
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
            #self.json_copy = self.json.copy()
            #self.cols  = self.json_copy.pop(0)
            self.df_ulAtt = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')

            self.cols = self.df_ulAtt.columns.values.tolist()
            self.rows = len(self.df_ulAtt)           
        
            print(self.cols) 
            if 'ManagedElement' in self.cols:
                self.result_status = "JSON Ok"
            else:
                self.result_status = "JSON Error"                
        
        except:
        
            self.result_status = "Code Error"
            self.body = ""
            self.json = ""
            self.resp_status_code = ""
            self.resp_headers = ""
            self.cols = 0
            self.rows = 0
        
        # Output final result
        result = {}      
        result['result'] = self.df_ulAtt
        result['result_status'] = self.result_status
        #result['type_of_query'] = self.type_of_query
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB'] = self.eNodeB
        result['eNodeB_list'] = self.eNodeB_list
        result['eNodeB_str'] = self.eNodeB_str
        #result['Cell'] = self.Cell
        #result['Cell_list'] = self.Cell_list
        #result['Cell_str'] = self.Cell_str
        result['record_date'] = self.record_date
        result['body'] = self.body
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    def cm_query_history(self):

        result = {}
        result['bleah'] = 1
        return result

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    def time_template(self):
        result = {}
        result['bleah'] = 1
        return result

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    def schema_list(self,customer,vendor,technology,data_type,time_resolution):

        #self.type_of_query = type_of_query
        #self.customer = customer
        #self.tech = tech
        #self.eNodeB = eNodeB
        #self.Cell = Cell
        #self.key = self.dict_keys[self.customer]
        self.data_type = data_type
        self.time_resolution = time_resolution
        #self.vendor = "ericsson"
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/SchemaList/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "Datatype":self.data_type,
            "Resolution":self.time_resolution,
        }         
        
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        self.df = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')        
        
        # Output final result
        result = {}      
        result['result'] = self.df
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['url'] = self.url
        result['body'] = self.body
        result['JSON'] = self.json
        #result['result_status'] = self.result_status
        result['customer'] = self.customer
        result['vendor'] = self.vendor
        result['time_resolution'] = self.time_resolution
        result['data_type'] = self.data_type
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    def schema_attributes(self,time_resolution,data_type,table_name):

        #self.type_of_query = type_of_query
        #self.customer = customer
        #self.tech = tech
        #self.eNodeB = eNodeB
        #self.Cell = Cell
        #self.key = self.dict_keys[self.customer]
        self.data_type = data_type
        self.time_resolution = time_resolution
        #self.vendor = "ericsson"
        self.table_name = table_name
        self.data_type = data_type
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/SchemaAttributes/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "Datatype":self.data_type,
            "Resolution":self.time_resolution,
            "TableName":self.table_name
        }         
        
        self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
        self.df = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')        
        
        # Output final result
        result = {}      
        result['result'] = self.df
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        #result['url'] = self.url
        result['body'] = self.body
        result['JSON'] = self.json
        #result['result_status'] = self.result_status
        result['customer'] = self.customer
        result['vendor'] = self.vendor
        result['time_resolution'] = self.time_resolution
        result['data_type'] = self.data_type
        result['table_name'] = self.table_name
        
        return result
    

# %%

# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
  
    # Query from CM Master Query for a whole eNodeB
    def cm_masterquery_gnodeb(self,eNodeB_list,Cell_list):
        
        self.eNodeB_list = eNodeB_list
        self.Cell_list = Cell_list
        #self.tech
        
        #print(f'CM Master Qry for cust: {self.customer} eNodeB:{self.eNodeB_list}, Cell:{self.Cell} - SINGLE CELL')
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMMasterQuery/{self.customer}/{self.vendor}/NR?apikey=' + self.key
        self.body = {
            "Customer": self.customer,
            "Attributes": [
            "gNodeB",
            "Cell",
            "vsDataSectorCarrier",
            #"Duplex Mode",
            "txDirection",
            "freqBand",
            "earfcndl",
            "earfcnul",
            "dlChannelBandwidth",
            "ulChannelBandwidth",
            #"#administrativeState",
            "operationalState",
            #"cellBarred",    
            #"availabilityStatus",            
            #"IndoorOutdoor",
            "cellRange",
            "Azimuth",
            "latitude",
            "longitude",
            "Radio ProductName",
            "Radio productNumber",
            "Radio SerialNumber", 
            "swVersion",
            "bandList",
            #"swRelease",
            #"DU ProductName", not supported
            #"DU ProductNumber", not supported
            "noOfTxAntennas",
            #"cellRange",
            #"alpha",
            #"transmissionMode",
            #"additionalFreqBandList",  Not supported
            #"ailgActive",
            #"pucchOverdimensioning",
            #"ulConfigurableFrequencyStart",
            #"ulFrequencyAllocationProportion",
            #"ulInterferenceManagementActive",
            #"ulInterferenceManagementDuration", Not supported
            #"ulInternalChannelBandwidth", Not supported
            #"ulImprovedUeSchedLastEnabled", Not supported
            #"ulSchedCtrlForOocUesEnabled", Not supported
            #"puschMaxNrOfPrbsPerUe",
            #"puschFrequencyAllocationBr", Not supported
            #"auPortRef",
            #"pZeroNominalPucch",
            #"pZeroNominalPusch",
            #"NeighborListWithDistance",
            #"dlAttenuation", use RfBranch instead
            #"eNBId",
            #"cellId",
            #"RelationId", "globalCellId",
            #"pci",                
        ],
        "Filter_SubNetworkLevel": 1,
        #"Filter_SubNetwork",
        #"Filter_Carriers:[]",
        #"Filter_Spatial:"(,)(,)",
        #"Filter_glbalCellId":[],
        #"Filter_cell": [ncell_mq],
        #"Filter_Cell_Operator":customer
        #"Filter_Rnc":rnc #WCDMA
        #"Filter_eNodeB": eNodeB_list,
        "Filter_eNodeB": eNodeB_list,
        #"Filter_cell": [Cell_list]
        } 
        
        self.result_status = "JSON start"
        self.json = ""
        self.resp_status_code = ""
        self.resp_headers = ""
        self.cols = []
        self.rows = 0           
        
        try:
        
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
            #self.json_copy = self.json.copy()
            #self.cols  = self.json_copy.pop(0)
            self.df_cm_gnodeb = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')        
        

            self.cols = self.df_cm_gnodeb.columns.values.tolist()
            self.rows = len(self.df_cm_gnodeb)
        
            if "gNodeB" in self.cols:
                self.result_status = "Ok"
            else:
                self.result_status = "JSON Error"           
        
        
        except:
        
            self.result_status = "Code Error"    
        
        
        # Output final result
        result = {}            
        result['result'] = self.df_cm_gnodeb
        result['result_status'] = self.result_status
        #result['type_of_query'] = self.type_of_query
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['eNodeB_list'] = self.eNodeB_list
        result['Cell_list'] = self.Cell_list
        #result['Cell'] = self.Cell        
        result['body'] = self.body
        result['url'] = self.url
        result['JSON'] = self.json
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['columns'] = self.cols
        result['rows'] = self.rows        
        
        return result
        

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    #def pm_query_cell(self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    def pm_query_cell_NR_test(self,eNodeB_list,Cell_list):
        
        #self.start_date = start_date
        #self.end_date = end_date
        self.date_range = self.start_date+'-'+self.end_date
        #self.time_resolution = time_resolution
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(eNodeB_list)
        self.Cell_list = Cell_list
        self.Cell_str = ','.join(Cell_list)
        #self.key = self.dict_keys[self.customer]
        self.rops = self.time_resolution_to_rops[self.time_resolution]
        
        #print(f'PM CELL for cell:{self.Cell_str}, temp res:{self.time_resolution}, date range:{self.date_range} ')

        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMPMFM/{self.customer}/{self.vendor}/NR?apikey=' + self.key
        self.body = {
            "AppId": "EJOHBEA Testing", 
            "TemporalResolution": self.time_resolution, 
            #"SpatialResolution": "No Aggregation",
            "SpatialResolution":"cell",
            "ObjectidType": "cell",
            #"Objectids": npm_cell,
            "Objectids": self.Cell_str,
            "Kpis": [
                #{"name": "freqBand","formula": "freqBand"},
                #{"name": "earfcndl","formula": "earfcndl"},
                #{"name": "earfcnul","formula": "earfcnul"},
                #{"name": "dlChannelBandwidth","formula": "dlChannelBandwidth"},
                #{"name": "ulChannelBandwidth","formula": "ulChannelBandwidth"},
                #{"name": "max_rrc_conn_users","formula": "pmRrcConnMax"},
                {"name": "pmRadioRecInterferencePwrDistr_0","formula": "pmRadioRecInterferencePwrDistr_0"},   
                #{"name": "pmRadioRecInterferencePwr_1","formula": "pmRadioRecInterferencePwr_1"},   
                #{"name": "pmRadioRecInterferencePwr_2","formula": "pmRadioRecInterferencePwr_2"},   
                #{"name": "pmRadioRecInterferencePwr_3","formula": "pmRadioRecInterferencePwr_3"},   
                #{"name": "pmRadioRecInterferencePwr_4","formula": "pmRadioRecInterferencePwr_4"},   
                #{"name": "pmRadioRecInterferencePwr_5","formula": "pmRadioRecInterferencePwr_5"},   
                #{"name": "pmRadioRecInterferencePwr_6","formula": "pmRadioRecInterferencePwr_6"},   
                #{"name": "pmRadioRecInterferencePwr_7","formula": "pmRadioRecInterferencePwr_7"},   
                #{"name": "pmRadioRecInterferencePwr_8","formula": "pmRadioRecInterferencePwr_8"},   
                #{"name": "pmRadioRecInterferencePwr_9","formula": "pmRadioRecInterferencePwr_9"},   
                #{"name": "pmRadioRecInterferencePwr_10","formula": "pmRadioRecInterferencePwr_10"},   
                #{"name": "pmRadioRecInterferencePwr_11","formula": "pmRadioRecInterferencePwr_11"},   
                #{"name": "pmRadioRecInterferencePwr_12","formula": "pmRadioRecInterferencePwr_12"},   
                #{"name": "pmRadioRecInterferencePwr_13","formula": "pmRadioRecInterferencePwr_13"},   
                #{"name": "pmRadioRecInterferencePwr_14","formula": "pmRadioRecInterferencePwr_14"},   
                #{"name": "pmRadioRecInterferencePwr_15","formula": "pmRadioRecInterferencePwr_15"},   
                #{"name": "pmRadioRecInterferencePwr_16","formula": "pmRadioRecInterferencePwr_16"},   
                #{"name": "pmRadioRecInterferencePwrPucch_0","formula": "pmRadioRecInterferencePwrPucch_0"}, 
                #{"name": "pmRadioRecInterferencePwrPucch_1","formula": "pmRadioRecInterferencePwrPucch_1"},   
                #{"name": "pmRadioRecInterferencePwrPucch_2","formula": "pmRadioRecInterferencePwrPucch_2"},   
                #{"name": "pmRadioRecInterferencePwrPucch_3","formula": "pmRadioRecInterferencePwrPucch_3"},   
                #{"name": "pmRadioRecInterferencePwrPucch_4","formula": "pmRadioRecInterferencePwrPucch_4"},   
                #{"name": "pmRadioRecInterferencePwrPucch_5","formula": "pmRadioRecInterferencePwrPucch_5"},   
                #{"name": "pmRadioRecInterferencePwrPucch_6","formula": "pmRadioRecInterferencePwrPucch_6"},   
                #{"name": "pmRadioRecInterferencePwrPucch_7","formula": "pmRadioRecInterferencePwrPucch_7"},   
                #{"name": "pmRadioRecInterferencePwrPucch_8","formula": "pmRadioRecInterferencePwrPucch_8"},   
                #{"name": "pmRadioRecInterferencePwrPucch_9","formula": "pmRadioRecInterferencePwrPucch_9"},   
                #{"name": "pmRadioRecInterferencePwrPucch_10","formula": "pmRadioRecInterferencePwrPucch_10"},   
                #{"name": "pmRadioRecInterferencePwrPucch_11","formula": "pmRadioRecInterferencePwrPucch_11"},   
                #{"name": "pmRadioRecInterferencePwrPucch_12","formula": "pmRadioRecInterferencePwrPucch_12"},   
                #{"name": "pmRadioRecInterferencePwrPucch_13","formula": "pmRadioRecInterferencePwrPucch_13"},   
                #{"name": "pmRadioRecInterferencePwrPucch_14","formula": "pmRadioRecInterferencePwrPucch_14"},   
                #{"name": "pmRadioRecInterferencePwrPucch_15","formula": "pmRadioRecInterferencePwrPucch_15"},   
                #{"name": "pmRadioRecInterferencePwrPucch_16","formula": "pmRadioRecInterferencePwrPucch_16"}, 
                #{"name": "pmRadioRecInterferencePwrPucch_16","formula": "pmRadioRecInterferencePwrPucch_16"},    
          ],
          "Daterange": self.date_range,
          #"Daterange": "2021/06/30-2021/07/06",
          "TimeZoneFilter": "local",
          "TimeZoneOutput": "local",
          "MaxNumberOfRows": "1000000",
          "IgnoreMissingCounters": "True",
          "ConvertDistFrom1to0Based": "False"
        }        
                
            
        #self.result_status = "JSON start"
        #self.json = ""
        #self.resp_status_code = ""
        #self.resp_headers = ""
        #self.cols = []
        #self.rows = 0          
        #self.df_pm_cell_nr = pd.DataFrame()
            
        try:
            
            #self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)     
            #self.json_copy = self.json.copy()
            print(type(self.json_copy))
        
            # Normally, the return is a list.  A dict is returned if failed.  
            # Have to make sure a df is created either way
            if isinstance(self.json_copy, dict):   
                self.df_pm_cell_nr = pd.DataFrame.from_dict(self.json_copy, orient="index")
                self.result_status = "JSON Error"
            else:  
                self.cols  = self.json_copy.pop(0)
                self.df_pm_cell_nr = pd.DataFrame(self.json_copy, columns=self.cols)    
                self.result_status = "Ok"

            #self.rows = len(self.df_pm_cell_nr)         
                        
        
        except:
        
        
            self.result_status = "Code Error"    
        
        # Output final result
        #print(f"Hit, {self.rows} rows, {len(self.cols)}")            
        
        #self.result_status = ""
        
        result = {}                 
        result['JSON'] = self.json        
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        result['result'] = self.df_pm_cell_nr
        result['result_status'] = self.result_status
        result['columns'] = self.cols
        result['rows'] = self.rows
        result['customer'] = self.customer
        result['key'] = self.key
        result['date_range'] = self.date_range
        result['technology'] = self.tech
        result['time_resolution'] = self.time_resolution
        result['eNodeB'] = self.eNodeB
        result['Cell'] = self.Cell        
        result['body'] = self.body        
        
        return result
    
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    #def pm_query_cell(self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    def pm_query_cell_NR(self,eNodeB_list,Cell_list):
        
        #self.start_date = start_date
        #self.end_date = end_date
        self.date_range = self.start_date+'-'+self.end_date
        #self.time_resolution = time_resolution
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(eNodeB_list)
        self.Cell_list = Cell_list
        self.Cell_str = ','.join(Cell_list)
        #self.key = self.dict_keys[self.customer]
        self.rops = self.time_resolution_to_rops[self.time_resolution]        
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMPMFM/{self.customer}/{self.vendor}/NR?apikey=' + self.key
        self.body = {
            "AppId": "EJOHBEA Testing", 
            "TemporalResolution": self.time_resolution, 
            #"SpatialResolution": "No Aggregation",
            "SpatialResolution":"cell",
            "ObjectidType": "cell",
            #"Objectids": npm_cell,
            "Objectids": self.Cell_str,
            "Kpis": [
                #{"name": "freqBand","formula": "freqBand"},
                #{"name": "earfcndl","formula": "earfcndl"},
                #{"name": "earfcnul","formula": "earfcnul"},
                #{"name": "dlChannelBandwidth","formula": "dlChannelBandwidth"},
                #{"name": "ulChannelBandwidth","formula": "ulChannelBandwidth"},
                
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_0","formula": "pmRadioRecInterferencePwrSumPrbDistr_0"},   
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_1","formula": "pmRadioRecInterferencePwrSumPrbDistr_1"},               
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_2","formula": "pmRadioRecInterferencePwrSumPrbDistr_2"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_3","formula": "pmRadioRecInterferencePwrSumPrbDistr_3"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_4","formula": "pmRadioRecInterferencePwrSumPrbDistr_4"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_5","formula": "pmRadioRecInterferencePwrSumPrbDistr_5"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_6","formula": "pmRadioRecInterferencePwrSumPrbDistr_6"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_7","formula": "pmRadioRecInterferencePwrSumPrbDistr_7"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_8","formula": "pmRadioRecInterferencePwrSumPrbDistr_8"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_9","formula": "pmRadioRecInterferencePwrSumPrbDistr_9"},                  
                {"name": "pmRadioRecInterferencePwrSumPrbDistr_10","formula": "pmRadioRecInterferencePwrSumPrbDistr_10"},                  
    
                
                {"name": "pmRadioRecInterferencePwrDistr_0","formula": "pmRadioRecInterferencePwrDistr_0"},   
                {"name": "pmRadioRecInterferencePwrDistr_1","formula": "pmRadioRecInterferencePwrDistr_1"}, 
                {"name": "pmRadioRecInterferencePwrDistr_2","formula": "pmRadioRecInterferencePwrDistr_2"}, 
                {"name": "pmRadioRecInterferencePwrDistr_3","formula": "pmRadioRecInterferencePwrDistr_3"}, 
                {"name": "pmRadioRecInterferencePwrDistr_4","formula": "pmRadioRecInterferencePwrDistr_4"}, 
                {"name": "pmRadioRecInterferencePwrDistr_5","formula": "pmRadioRecInterferencePwrDistr_5"}, 
                {"name": "pmRadioRecInterferencePwrDistr_6","formula": "pmRadioRecInterferencePwrDistr_6"}, 
                {"name": "pmRadioRecInterferencePwrDistr_7","formula": "pmRadioRecInterferencePwrDistr_7"}, 
                {"name": "pmRadioRecInterferencePwrDistr_8","formula": "pmRadioRecInterferencePwrDistr_8"}, 
                {"name": "pmRadioRecInterferencePwrDistr_9","formula": "pmRadioRecInterferencePwrDistr_9"}, 
                {"name": "pmRadioRecInterferencePwrDistr_10","formula": "pmRadioRecInterferencePwrDistr_10"}, 
                {"name": "pmRadioRecInterferencePwrDistr_11","formula": "pmRadioRecInterferencePwrDistr_11"}, 
                {"name": "pmRadioRecInterferencePwrDistr_12","formula": "pmRadioRecInterferencePwrDistr_12"}, 
                {"name": "pmRadioRecInterferencePwrDistr_13","formula": "pmRadioRecInterferencePwrDistr_13"}, 
                {"name": "pmRadioRecInterferencePwrDistr_14","formula": "pmRadioRecInterferencePwrDistr_14"}, 
                {"name": "pmRadioRecInterferencePwrDistr_15","formula": "pmRadioRecInterferencePwrDistr_15"}, 
                {"name": "pmRadioRecInterferencePwrDistr_16","formula": "pmRadioRecInterferencePwrDistr_16"}, 
                {"name": "pmRadioRecInterferencePwrDistr_17","formula": "pmRadioRecInterferencePwrDistr_17"}, 
                {"name": "pmRadioRecInterferencePwrDistr_18","formula": "pmRadioRecInterferencePwrDistr_18"}, 
                {"name": "pmRadioRecInterferencePwrDistr_19","formula": "pmRadioRecInterferencePwrDistr_19"}, 
                {"name": "pmRadioRecInterferencePwrDistr_20","formula": "pmRadioRecInterferencePwrDistr_20"},
                {"name": "pmRadioRecInterferencePwrDistr_21","formula": "pmRadioRecInterferencePwrDistr_21"},
                
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_0","formula": "pmRadioRecInterferencePwrPucchF0Distr_0"},   
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_1","formula": "pmRadioRecInterferencePwrPucchF0Distr_1"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_2","formula": "pmRadioRecInterferencePwrPucchF0Distr_2"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_3","formula": "pmRadioRecInterferencePwrPucchF0Distr_3"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_4","formula": "pmRadioRecInterferencePwrPucchF0Distr_4"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_5","formula": "pmRadioRecInterferencePwrPucchF0Distr_5"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_6","formula": "pmRadioRecInterferencePwrPucchF0Distr_6"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_7","formula": "pmRadioRecInterferencePwrPucchF0Distr_7"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_8","formula": "pmRadioRecInterferencePwrPucchF0Distr_8"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_9","formula": "pmRadioRecInterferencePwrPucchF0Distr_9"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_10","formula": "pmRadioRecInterferencePwrPucchF0Distr_10"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_11","formula": "pmRadioRecInterferencePwrPucchF0Distr_11"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_12","formula": "pmRadioRecInterferencePwrPucchF0Distr_12"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_13","formula": "pmRadioRecInterferencePwrPucchF0Distr_13"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_14","formula": "pmRadioRecInterferencePwrPucchF0Distr_14"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_15","formula": "pmRadioRecInterferencePwrPucchF0Distr_15"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_16","formula": "pmRadioRecInterferencePwrPucchF0Distr_16"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_17","formula": "pmRadioRecInterferencePwrPucchF0Distr_17"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_18","formula": "pmRadioRecInterferencePwrPucchF0Distr_18"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_19","formula": "pmRadioRecInterferencePwrPucchF0Distr_19"}, 
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_20","formula": "pmRadioRecInterferencePwrPucchF0Distr_20"},
                {"name": "pmRadioRecInterferencePwrPucchF0Distr_21","formula": "pmRadioRecInterferencePwrPucchF0Distr_21"},
                
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_0","formula": "pmRadioRecInterferencePwrPucchF2Distr_0"},   
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_1","formula": "pmRadioRecInterferencePwrPucchF2Distr_1"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_2","formula": "pmRadioRecInterferencePwrPucchF2Distr_2"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_3","formula": "pmRadioRecInterferencePwrPucchF2Distr_3"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_4","formula": "pmRadioRecInterferencePwrPucchF2Distr_4"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_5","formula": "pmRadioRecInterferencePwrPucchF2Distr_5"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_6","formula": "pmRadioRecInterferencePwrPucchF2Distr_6"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_7","formula": "pmRadioRecInterferencePwrPucchF2Distr_7"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_8","formula": "pmRadioRecInterferencePwrPucchF2Distr_8"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_9","formula": "pmRadioRecInterferencePwrPucchF2Distr_9"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_10","formula": "pmRadioRecInterferencePwrPucchF2Distr_10"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_11","formula": "pmRadioRecInterferencePwrPucchF2Distr_11"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_12","formula": "pmRadioRecInterferencePwrPucchF2Distr_12"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_13","formula": "pmRadioRecInterferencePwrPucchF2Distr_13"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_14","formula": "pmRadioRecInterferencePwrPucchF2Distr_14"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_15","formula": "pmRadioRecInterferencePwrPucchF2Distr_15"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_16","formula": "pmRadioRecInterferencePwrPucchF2Distr_16"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_17","formula": "pmRadioRecInterferencePwrPucchF2Distr_17"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_18","formula": "pmRadioRecInterferencePwrPucchF2Distr_18"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_19","formula": "pmRadioRecInterferencePwrPucchF2Distr_19"}, 
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_20","formula": "pmRadioRecInterferencePwrPucchF2Distr_20"},
                {"name": "pmRadioRecInterferencePwrPucchF2Distr_21","formula": "pmRadioRecInterferencePwrPucchF2Distr_21"},                
                
          ],
          "Daterange": self.date_range,
          #"Daterange": "2021/06/30-2021/07/06",
          "TimeZoneFilter": "local",
          "TimeZoneOutput": "local",
          "MaxNumberOfRows": "1000000",
          "IgnoreMissingCounters": "True",
          "ConvertDistFrom1to0Based": "False"
        }        
               
        
        self.result_status = "JSON start"
        self.json = ""
        self.resp_status_code = ""
        self.resp_headers = ""
        self.cols = []
        self.rows = 0          
        self.df_pm_cell_nr = pd.DataFrame()        
        
        try:
            
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)     
            self.json_copy = self.json.copy()
            print(type(self.json_copy))
        
            # Normally, the return is a list.  A dict is returned if failed.  
            # Have to make sure a df is created either way
            if isinstance(self.json_copy, dict):   
                self.df_pm_cell_nr = pd.DataFrame.from_dict(self.json_copy, orient="index")
                self.result_status = "JSON Error"
            else:  
                self.cols  = self.json_copy.pop(0)
                self.df_pm_cell_nr = pd.DataFrame(self.json_copy, columns=self.cols)    
                self.result_status = "JSON Ok"

            self.rows = len(self.df_pm_cell_nr)         
                        
        
        except:
        
            self.result_status = "Code Error"         
        
        # Process the query results if query was ok
        if self.result_status == "JSON Ok":
        
            self.df_pm_cell_nr[["eNodeB","Cell"]] = self.df_pm_cell_nr['ObjectId'].str.split(".",expand=True)        
        
            # Sort the dataframe
            self.df_pm_cell_nr.sort_values(['ObjectId','RecordDate'], inplace=True)  
            self.df_pm_cell_nr.reset_index(drop=False, inplace=True)        
        
            # Drop unnecessary columns
            self.df_pm_cell_nr = self.df_pm_cell_nr.drop(['index','DateInTicks','DateInHours','DateInDays'],axis =1)              
        
        
        result = {}                  
        # Parameters
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['time_resolution'] = self.time_resolution        
        result['date_range'] = self.date_range
        result['eNodeB_list'] = self.eNodeB_list
        result['eNodeB_str'] = self.eNodeB_str
        result['Cell_list'] = self.Cell_list        
        result['Cell_str'] = self.Cell_str
        
        # URL & query body
        result['url'] = self.url
        result['body'] = self.body

        # JSON results
        result['JSON'] = self.json   
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        
        # Debugging values
        result['result_status'] = self.result_status
        
        # Query contents
        result['result'] = self.df_pm_cell_nr
        result['rows'] = self.rows
        result['columns'] = self.cols

        return result        
        
        

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    def precanned_report_list(self,customer,vendor,tech):

        #self.type_of_query = type_of_query
        self.customer = customer
        self.tech = tech
        #self.eNodeB = eNodeB
        #self.Cell = Cell
        #self.key = self.dict_keys[self.customer]
        #self.data_type = data_type
        #self.time_resolution = time_resolution
        #self.vendor = "ericsson"
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/PreCannedReportList/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
        }         
        
        self.result_status = "JSON start"
        self.json = ""
        self.resp_status_code = ""
        self.resp_headers = ""
        self.cols = []
        self.rows = 0          
        self.df_pm_cell_nr = pd.DataFrame()
        
        try:        
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
            self.df = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')        

            self.result_status = "JSON complete"        
            #self.cols  = self.json_copy.pop(0)
            #self.rows = len(self.df_pm_cell_nr)        
        
        except:
            self.result_status = "Code Error"    
        
        # Output final result
        result = {}                  
        # Parameters
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        
        # URL & query body
        result['url'] = self.url
        result['body'] = self.body

        # JSON results
        result['JSON'] = self.json   
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        
        # Debugging values
        result['result_status'] = self.result_status
        
        # Query contents
        result['result'] = self.df
        result['rows'] = self.rows
        result['columns'] = self.cols
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    def precanned_report_schema_attributes(self,tech,report_name):

        #self.type_of_query = type_of_query
        #self.customer = customer
        self.tech = tech
        #self.eNodeB = eNodeB
        #self.Cell = Cell
        #self.key = self.dict_keys[self.customer]
        #self.data_type = data_type
        #self.time_resolution = time_resolution
        #self.vendor = "ericsson"
        self.report_name = report_name
        
        self.url = f'http://172.18.29.65/EPPMiddleware/api/PreCannedReportSchema/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "AppId":"External User",
            "ReportName":self.report_name
        }         
        
        self.result_status = "JSON start"
        self.json = ""
        self.resp_status_code = ""
        self.resp_headers = ""
        self.cols = []
        self.rows = 0          
        self.df_pc_rpt_schema = pd.DataFrame()
        
        try:
        
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)
            self.df_pc_rpt_schema = pd.DataFrame.from_dict(json_normalize(self.json), orient='columns')        
        
            self.result_status = "JSON complete"        
            self.cols  = self.json_copy.pop(0)
            self.rows = len(self.df_pc_rpt_schema)        
        
        except:
            self.result_status = "Code Error"    
        
        # Output final result
        result = {}                  
        # Parameters
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        result['report_name'] = self.report_name
        
        # URL & query body
        result['url'] = self.url
        result['body'] = self.body

        # JSON results
        result['JSON'] = self.json   
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        
        # Debugging values
        result['result_status'] = self.result_status
        
        # Query contents
        result['result'] = self.df_pc_rpt_schema
        result['rows'] = self.rows
        result['columns'] = self.cols
        
        return result
    

# %%
# Below class definition required for Jupyter Notebook Cells
class DopQueries(DopQueries):
    
    #def pm_query_cell(self,time_resolution,eNodeB_list,Cell_list,start_date,end_date):
    def pm_query_cell(self,eNodeB_list,Cell_list,prb_to_dbm):
        
        #self.start_date = start_date
        #self.end_date = end_date
        self.date_range = self.start_date+'-'+self.end_date
        #self.time_resolution = time_resolution
        self.eNodeB_list = eNodeB_list
        self.eNodeB_str = ','.join(eNodeB_list)
        self.Cell_list = Cell_list
        self.Cell_str = ','.join(Cell_list)
        #self.key = self.dict_keys[self.customer]
        self.rops = self.time_resolution_to_rops[self.time_resolution]
        self.prb_to_dbm = prb_to_dbm
        
        #print(f'PM CELL for cell:{self.Cell_str}, temp res:{self.time_resolution}, date range:{self.date_range} ')

        self.url = f'http://172.18.29.65/EPPMiddleware/api/CMPMFM/{self.customer}/{self.vendor}/{self.tech}?apikey=' + self.key
        self.body = {
            "AppId": "EJOHBEA Testing", 
            "TemporalResolution": self.time_resolution, 
            #"SpatialResolution": "No Aggregation",
            "SpatialResolution":"cell",
            "ObjectidType": "cell",
            #"Objectids": npm_cell,
            "Objectids": self.Cell_str,
            "Kpis": [
                {"name": "freqBand","formula": "freqBand"},
                {"name": "earfcndl","formula": "earfcndl"},
                {"name": "earfcnul","formula": "earfcnul"},
                {"name": "dlChannelBandwidth","formula": "dlChannelBandwidth"},
                {"name": "ulChannelBandwidth","formula": "ulChannelBandwidth"},
                {"name": "max_rrc_conn_users","formula": "pmRrcConnMax"},
                {"name": "prb_1","formula": "pmRadioRecInterferencePwrPrb1"},    
                {"name": "prb_1","formula": "pmRadioRecInterferencePwrPrb1"},
                {"name": "prb_2","formula": "pmRadioRecInterferencePwrPrb2"},
                {"name": "prb_3","formula": "pmRadioRecInterferencePwrPrb3"},
                {"name": "prb_4","formula": "pmRadioRecInterferencePwrPrb4"},
                {"name": "prb_5","formula": "pmRadioRecInterferencePwrPrb5"},
                {"name": "prb_6","formula": "pmRadioRecInterferencePwrPrb6"},
                {"name": "prb_7","formula": "pmRadioRecInterferencePwrPrb7"},
                {"name": "prb_8","formula": "pmRadioRecInterferencePwrPrb8"},
                {"name": "prb_9","formula": "pmRadioRecInterferencePwrPrb9"},
                {"name": "prb_10","formula": "pmRadioRecInterferencePwrPrb10"},
                {"name": "prb_11","formula": "pmRadioRecInterferencePwrPrb11"},
                {"name": "prb_12","formula": "pmRadioRecInterferencePwrPrb12"},
                {"name": "prb_13","formula": "pmRadioRecInterferencePwrPrb13"},
                {"name": "prb_14","formula": "pmRadioRecInterferencePwrPrb14"},
                {"name": "prb_15","formula": "pmRadioRecInterferencePwrPrb15"},
                {"name": "prb_16","formula": "pmRadioRecInterferencePwrPrb16"},
                {"name": "prb_17","formula": "pmRadioRecInterferencePwrPrb17"},
                {"name": "prb_18","formula": "pmRadioRecInterferencePwrPrb18"},
                {"name": "prb_19","formula": "pmRadioRecInterferencePwrPrb19"},
                {"name": "prb_20","formula": "pmRadioRecInterferencePwrPrb20"},
                {"name": "prb_21","formula": "pmRadioRecInterferencePwrPrb21"},
                {"name": "prb_22","formula": "pmRadioRecInterferencePwrPrb22"},
                {"name": "prb_23","formula": "pmRadioRecInterferencePwrPrb23"},
                {"name": "prb_24","formula": "pmRadioRecInterferencePwrPrb24"},
                {"name": "prb_25","formula": "pmRadioRecInterferencePwrPrb25"},
                {"name": "prb_26","formula": "pmRadioRecInterferencePwrPrb26"},
                {"name": "prb_27","formula": "pmRadioRecInterferencePwrPrb27"},
                {"name": "prb_28","formula": "pmRadioRecInterferencePwrPrb28"},
                {"name": "prb_29","formula": "pmRadioRecInterferencePwrPrb29"},
                {"name": "prb_30","formula": "pmRadioRecInterferencePwrPrb30"},
                {"name": "prb_31","formula": "pmRadioRecInterferencePwrPrb31"},
                {"name": "prb_32","formula": "pmRadioRecInterferencePwrPrb32"},
                {"name": "prb_33","formula": "pmRadioRecInterferencePwrPrb33"},
                {"name": "prb_34","formula": "pmRadioRecInterferencePwrPrb34"},
                {"name": "prb_35","formula": "pmRadioRecInterferencePwrPrb35"},
                {"name": "prb_36","formula": "pmRadioRecInterferencePwrPrb36"},
                {"name": "prb_37","formula": "pmRadioRecInterferencePwrPrb37"},
                {"name": "prb_38","formula": "pmRadioRecInterferencePwrPrb38"},
                {"name": "prb_39","formula": "pmRadioRecInterferencePwrPrb39"},                
                {"name": "prb_40","formula": "pmRadioRecInterferencePwrPrb40"},
                {"name": "prb_41","formula": "pmRadioRecInterferencePwrPrb41"},
                {"name": "prb_42","formula": "pmRadioRecInterferencePwrPrb42"},
                {"name": "prb_43","formula": "pmRadioRecInterferencePwrPrb43"},
                {"name": "prb_44","formula": "pmRadioRecInterferencePwrPrb44"},
                {"name": "prb_45","formula": "pmRadioRecInterferencePwrPrb45"},
                {"name": "prb_46","formula": "pmRadioRecInterferencePwrPrb46"},
                {"name": "prb_47","formula": "pmRadioRecInterferencePwrPrb47"},
                {"name": "prb_48","formula": "pmRadioRecInterferencePwrPrb48"},
                {"name": "prb_49","formula": "pmRadioRecInterferencePwrPrb49"},
                {"name": "prb_50","formula": "pmRadioRecInterferencePwrPrb50"},	          
                {"name": "prb_51","formula": "pmRadioRecInterferencePwrPrb51"},
                {"name": "prb_52","formula": "pmRadioRecInterferencePwrPrb52"},
                {"name": "prb_53","formula": "pmRadioRecInterferencePwrPrb53"},
                {"name": "prb_54","formula": "pmRadioRecInterferencePwrPrb54"},
                {"name": "prb_55","formula": "pmRadioRecInterferencePwrPrb55"},
                {"name": "prb_56","formula": "pmRadioRecInterferencePwrPrb56"},
                {"name": "prb_57","formula": "pmRadioRecInterferencePwrPrb57"},
                {"name": "prb_58","formula": "pmRadioRecInterferencePwrPrb58"},
                {"name": "prb_59","formula": "pmRadioRecInterferencePwrPrb59"},
                {"name": "prb_60","formula": "pmRadioRecInterferencePwrPrb60"},
                {"name": "prb_61","formula": "pmRadioRecInterferencePwrPrb61"},
                {"name": "prb_62","formula": "pmRadioRecInterferencePwrPrb62"},
                {"name": "prb_63","formula": "pmRadioRecInterferencePwrPrb63"},
                {"name": "prb_64","formula": "pmRadioRecInterferencePwrPrb64"},
                {"name": "prb_65","formula": "pmRadioRecInterferencePwrPrb65"},
                {"name": "prb_66","formula": "pmRadioRecInterferencePwrPrb66"},
                {"name": "prb_67","formula": "pmRadioRecInterferencePwrPrb67"},
                {"name": "prb_68","formula": "pmRadioRecInterferencePwrPrb68"},
                {"name": "prb_69","formula": "pmRadioRecInterferencePwrPrb69"},
                {"name": "prb_70","formula": "pmRadioRecInterferencePwrPrb70"},	
                {"name": "prb_71","formula": "pmRadioRecInterferencePwrPrb71"},
                {"name": "prb_72","formula": "pmRadioRecInterferencePwrPrb72"},
                {"name": "prb_73","formula": "pmRadioRecInterferencePwrPrb73"},
                {"name": "prb_74","formula": "pmRadioRecInterferencePwrPrb74"},
                {"name": "prb_75","formula": "pmRadioRecInterferencePwrPrb75"},
                {"name": "prb_76","formula": "pmRadioRecInterferencePwrPrb76"},
                {"name": "prb_77","formula": "pmRadioRecInterferencePwrPrb77"},
                {"name": "prb_78","formula": "pmRadioRecInterferencePwrPrb78"},
                {"name": "prb_79","formula": "pmRadioRecInterferencePwrPrb79"},
                {"name": "prb_80","formula": "pmRadioRecInterferencePwrPrb80"},	
                {"name": "prb_81","formula": "pmRadioRecInterferencePwrPrb81"},
                {"name": "prb_82","formula": "pmRadioRecInterferencePwrPrb82"},
                {"name": "prb_83","formula": "pmRadioRecInterferencePwrPrb83"},
                {"name": "prb_84","formula": "pmRadioRecInterferencePwrPrb84"},
                {"name": "prb_85","formula": "pmRadioRecInterferencePwrPrb85"},
                {"name": "prb_86","formula": "pmRadioRecInterferencePwrPrb86"},
                {"name": "prb_87","formula": "pmRadioRecInterferencePwrPrb87"},
                {"name": "prb_88","formula": "pmRadioRecInterferencePwrPrb88"},
                {"name": "prb_89","formula": "pmRadioRecInterferencePwrPrb89"},
                {"name": "prb_90","formula": "pmRadioRecInterferencePwrPrb90"},	
                {"name": "prb_91","formula": "pmRadioRecInterferencePwrPrb91"},
                {"name": "prb_92","formula": "pmRadioRecInterferencePwrPrb92"},
                {"name": "prb_93","formula": "pmRadioRecInterferencePwrPrb93"},
                {"name": "prb_94","formula": "pmRadioRecInterferencePwrPrb94"},
                {"name": "prb_95","formula": "pmRadioRecInterferencePwrPrb95"},
                {"name": "prb_96","formula": "pmRadioRecInterferencePwrPrb96"},
                {"name": "prb_97","formula": "pmRadioRecInterferencePwrPrb97"},
                {"name": "prb_98","formula": "pmRadioRecInterferencePwrPrb98"},
                {"name": "prb_99","formula": "pmRadioRecInterferencePwrPrb99"},
                {"name": "prb_100","formula": "pmRadioRecInterferencePwrPrb100"},           
                {"name": "pmRadioRecInterferencePwr_0","formula": "pmRadioRecInterferencePwr_0"},   
                {"name": "pmRadioRecInterferencePwr_1","formula": "pmRadioRecInterferencePwr_1"},   
                {"name": "pmRadioRecInterferencePwr_2","formula": "pmRadioRecInterferencePwr_2"},   
                {"name": "pmRadioRecInterferencePwr_3","formula": "pmRadioRecInterferencePwr_3"},   
                {"name": "pmRadioRecInterferencePwr_4","formula": "pmRadioRecInterferencePwr_4"},   
                {"name": "pmRadioRecInterferencePwr_5","formula": "pmRadioRecInterferencePwr_5"},   
                {"name": "pmRadioRecInterferencePwr_6","formula": "pmRadioRecInterferencePwr_6"},   
                {"name": "pmRadioRecInterferencePwr_7","formula": "pmRadioRecInterferencePwr_7"},   
                {"name": "pmRadioRecInterferencePwr_8","formula": "pmRadioRecInterferencePwr_8"},   
                {"name": "pmRadioRecInterferencePwr_9","formula": "pmRadioRecInterferencePwr_9"},   
                {"name": "pmRadioRecInterferencePwr_10","formula": "pmRadioRecInterferencePwr_10"},   
                {"name": "pmRadioRecInterferencePwr_11","formula": "pmRadioRecInterferencePwr_11"},   
                {"name": "pmRadioRecInterferencePwr_12","formula": "pmRadioRecInterferencePwr_12"},   
                {"name": "pmRadioRecInterferencePwr_13","formula": "pmRadioRecInterferencePwr_13"},   
                {"name": "pmRadioRecInterferencePwr_14","formula": "pmRadioRecInterferencePwr_14"},   
                {"name": "pmRadioRecInterferencePwr_15","formula": "pmRadioRecInterferencePwr_15"},   
                {"name": "pmRadioRecInterferencePwr_16","formula": "pmRadioRecInterferencePwr_16"},   
                {"name": "pmRadioRecInterferencePwrPucch_0","formula": "pmRadioRecInterferencePwrPucch_0"}, 
                {"name": "pmRadioRecInterferencePwrPucch_1","formula": "pmRadioRecInterferencePwrPucch_1"},   
                {"name": "pmRadioRecInterferencePwrPucch_2","formula": "pmRadioRecInterferencePwrPucch_2"},   
                {"name": "pmRadioRecInterferencePwrPucch_3","formula": "pmRadioRecInterferencePwrPucch_3"},   
                {"name": "pmRadioRecInterferencePwrPucch_4","formula": "pmRadioRecInterferencePwrPucch_4"},   
                {"name": "pmRadioRecInterferencePwrPucch_5","formula": "pmRadioRecInterferencePwrPucch_5"},   
                {"name": "pmRadioRecInterferencePwrPucch_6","formula": "pmRadioRecInterferencePwrPucch_6"},   
                {"name": "pmRadioRecInterferencePwrPucch_7","formula": "pmRadioRecInterferencePwrPucch_7"},   
                {"name": "pmRadioRecInterferencePwrPucch_8","formula": "pmRadioRecInterferencePwrPucch_8"},   
                {"name": "pmRadioRecInterferencePwrPucch_9","formula": "pmRadioRecInterferencePwrPucch_9"},   
                {"name": "pmRadioRecInterferencePwrPucch_10","formula": "pmRadioRecInterferencePwrPucch_10"},   
                {"name": "pmRadioRecInterferencePwrPucch_11","formula": "pmRadioRecInterferencePwrPucch_11"},   
                {"name": "pmRadioRecInterferencePwrPucch_12","formula": "pmRadioRecInterferencePwrPucch_12"},   
                {"name": "pmRadioRecInterferencePwrPucch_13","formula": "pmRadioRecInterferencePwrPucch_13"},   
                {"name": "pmRadioRecInterferencePwrPucch_14","formula": "pmRadioRecInterferencePwrPucch_14"},   
                {"name": "pmRadioRecInterferencePwrPucch_15","formula": "pmRadioRecInterferencePwrPucch_15"},   
                {"name": "pmRadioRecInterferencePwrPucch_16","formula": "pmRadioRecInterferencePwrPucch_16"}, 
                {"name": "pmRadioRecInterferencePwrPucch_16","formula": "pmRadioRecInterferencePwrPucch_16"},    
                {"name": "pmPrbUtilDl_0","formula": "pmPrbUtilDl_0"},         
                {"name": "pmPrbUtilDl_1","formula": "pmPrbUtilDl_1"},
                {"name": "pmPrbUtilDl_2","formula": "pmPrbUtilDl_2"},
                {"name": "pmPrbUtilDl_3","formula": "pmPrbUtilDl_3"},         
                {"name": "pmPrbUtilDl_4","formula": "pmPrbUtilDl_4"},
                {"name": "pmPrbUtilDl_5","formula": "pmPrbUtilDl_5"},
                {"name": "pmPrbUtilDl_6","formula": "pmPrbUtilDl_6"},         
                {"name": "pmPrbUtilDl_7","formula": "pmPrbUtilDl_7"},
                {"name": "pmPrbUtilDl_8","formula": "pmPrbUtilDl_8"},
                {"name": "pmPrbUtilDl_9","formula": "pmPrbUtilDl_9"}
          ],
          "Daterange": self.date_range,
          #"Daterange": "2021/06/30-2021/07/06",
          "TimeZoneFilter": "local",
          "TimeZoneOutput": "local",
          "MaxNumberOfRows": "1000000",
          "IgnoreMissingCounters": "True",
          "ConvertDistFrom1to0Based": "False"
        }        
                
        self.result_status = "JSON start"
        self.json = ""
        self.resp_status_code = ""
        self.resp_headers = ""
        self.cols = []
        self.rows = 0          
        self.df_pm_cell     = pd.DataFrame()
        self.df_pm_cell_raw = pd.DataFrame()
        breakpoint = ""
        
        try:            
            
            self.resp_status_code, self.resp_headers,self.json = self.api_data_retrieval(self.url,self.body)     
            self.json_copy = self.json.copy()
            print(type(self.json_copy))
        
            # Normally, the return is a list.  A dict is returned if failed.  
            # Have to make sure a df is created either way
            if isinstance(self.json_copy, dict):   
                self.cols = []
                self.df_pm_cell_raw = pd.DataFrame.from_dict(self.json_copy, orient="index")
                self.result_status = "JSON Fail"
            else:  
                self.cols  = self.json_copy.pop(0)
                self.df_pm_cell_raw = pd.DataFrame(self.json_copy, columns=self.cols)    
                self.result_status = "JSON Ok"

            self.rows = len(self.df_pm_cell_raw)
                        
        except:
            self.result_status = "Code Error"                  
                
        
        if self.result_status == "JSON Ok":
        
            process_flag = "Start"        
            #try:
            if True:
            
                process_flag = "Sort and reindex"               
                # Create datetime and reindex the dataframe
                self.df_pm_cell_raw['datetime']   = pd.to_datetime(self.df_pm_cell_raw['RecordDate'])
                self.df_pm_cell_raw.sort_values(['ObjectId','datetime'], inplace=True)  
                #self.df_pm_cell_raw.reset_index(drop=False, inplace=True)               
            
                process_flag = "Create output df structure"            
                # Create the final output template with same structure as the query dataframe             
                self.df_pm_cell = pd.DataFrame(columns=self.df_pm_cell_raw.columns.values)
                self.df_pm_cell_empty = self.df_pm_cell.copy()
                
                process_flag = "Get list of unique cells"            
                # Get unique objects                   
                self.unique_object_ids = self.df_pm_cell_raw['ObjectId'].unique()
                
                process_flag = "Iterate through cells"
                # iterate thru unique object IDs, apply time template and save to final output template
                for object_id in self.unique_object_ids:
                
                    #print(object_id)
                    process_flag = f"{object_id}: Setup debugging values"
                    #pm_data_process = pm_data_template.merge(pm_data_process,how='left', on='datetime')
                    len_raw      = len(self.df_pm_cell_raw)                     
                    len_template = len(self.df_time_template)
                    len_clipped  = 0
                    len_merged   = 0
                    len_out      = len(self.df_pm_cell)

                    process_flag = f"{object_id}: Clip objid from main into temp     - raw:{len_raw}, clipped: {len_clipped}, merged: {len_merged}, combined:{len_out}, time temp: {len_template}"
                    #print(process_flag)
                    self.df_pm_cell_temp  = self.df_pm_cell_raw.loc[self.df_pm_cell_raw['ObjectId']==object_id]
                    len_raw      = len(self.df_pm_cell_raw)  
                    len_clipped   = len(self.df_pm_cell_temp)
                    len_raw       = len(self.df_pm_cell_raw)
                    len_out      = len(self.df_pm_cell)

                    process_flag = f"{object_id}: Merge cell raw, template into temp - raw:{len_raw}, clipped: {len_clipped}, merged: {len_merged}, combined:{len_out}, time temp: {len_template}"
                    #print(process_flag)    
                    self.df_pm_cell_temp = self.df_time_template.merge(self.df_pm_cell_temp, how='left', on='datetime')  
                    self.df_pm_cell_temp = self.df_pm_cell_temp.assign(ObjectId=object_id)
                    len_raw      = len(self.df_pm_cell_raw)
                    len_clipped   = len(self.df_pm_cell_temp)
                    len_merged = len(self.df_pm_cell_temp)
                    len_out      = len(self.df_pm_cell)

                    process_flag = f"{object_id}: Append temp df to final df         - raw:{len_raw}, clipped: {len_clipped}, merged: {len_merged}, combined:{len_out}, time temp: {len_template}"
                    #print(process_flag)        
                    self.df_pm_cell = self.df_pm_cell.append(self.df_pm_cell_temp)
                    process_flag = f"{object_id}: Loop complete"
                    
            
                process_flag = "Re-sort "
                # Miscellaneous conversions              
                self.df_pm_cell['datetime']   = pd.to_datetime(self.df_pm_cell['datetime_t'])
                self.df_pm_cell.sort_values(['ObjectId','datetime'], inplace=True)  
                #self.df_pm_cell_raw.reset_index(drop=False, inplace=True)               
            
                process_flag = "Misc conversions"
                # Miscellaneous conversions
                self.df_pm_cell[["eNodeB","Cell"]] = self.df_pm_cell['ObjectId'].str.split(".",expand=True)
                self.df_pm_cell["Node"]            = self.df_pm_cell['ObjectId']
                self.df_pm_cell['PRBs']            = self.df_pm_cell['dlChannelBandwidth'].map(self.lte_bw_to_prbs)            
                                            
                process_flag = "Convert freqs"
                # Convert arfcns to frequencies
                #self.df_pm_cell['freq_dl_ctr']     = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','FREQ'))
                #self.df_pm_cell['freq_ul_ctr']     = self.df_pm_cell['earfcnul'].apply(self.arfcn_to_freq, args=('UL','FREQ'))
                #self.df_pm_cell['freq_band_id']    = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_ID'))
                #self.df_pm_cell['freq_name']       = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_NAME'))
                #self.df_pm_cell['freq_band_group'] = self.df_pm_cell['earfcndl'].apply(self.arfcn_to_freq, args=('DL','BAND_GROUP'))                  
                                 
                process_flag = "Eliminate NaNs - sets of data"
                self.df_pm_cell[self.prb_list]         = self.df_pm_cell[self.prb_list].replace(np.nan, 0.0)     
                self.df_pm_cell['PRBs']  = self.df_pm_cell['PRBs'].replace(np.nan, 0.0)                   
                self.df_pm_cell[self.pusch_rssi_list]  = self.df_pm_cell[self.pusch_rssi_list].replace(np.nan, 0.0)   
                self.df_pm_cell[self.pucch_rssi_list]  = self.df_pm_cell[self.pucch_rssi_list].replace(np.nan, 0.0) 
                self.df_pm_cell[self.dl_prb_util_list] = self.df_pm_cell[self.dl_prb_util_list].replace(np.nan, 0.0) 

                process_flag = "Determine valid PRB, RSSI data"  
                # Calculate average, 
                #self.df_pm_cell['populated_PRBs']   = (self.df_pm_cell[self.prb_list] != '0' ).sum(axis=1)
                self.df_pm_cell['populated_PRBs']   = (self.df_pm_cell[self.prb_list] != 0.0 ).sum(axis=1)                
                #self.df_pm_cell['populated_PRBs']   = (self.df_pm_cell['PRB_1'] != '0' ).sum(axis=1)   
                

                self.df_pm_cell['valid_pusch_rssi'] = self.df_pm_cell[self.pusch_rssi_list].count(axis=1)
                self.df_pm_cell['valid_pucch_rssi'] = self.df_pm_cell[self.pucch_rssi_list].count(axis=1)                  
                
                process_flag = "Calculate RSSI values - PRB" 
                if self.prb_to_dbm:
                    self.df_pm_cell[self.prb_list] = self.df_pm_cell[self.prb_list].applymap(lambda x: (10* np.log10(int(x)* 0.00000000000005684341886080800 /(900 * self.rops * 1000/40))) if (int(x)>0) else np.NaN)                    
                    self.df_pm_cell['max_prb_rssi'] = self.df_pm_cell[self.prb_list].max(axis=1, skipna=True)
                    self.df_pm_cell['min_prb_rssi'] = self.df_pm_cell[self.prb_list].min(axis=1, skipna=True)
                    self.df_pm_cell['sum_prb_rssi'] = self.df_pm_cell[self.prb_list].sum(axis=1)
                    self.df_pm_cell['avg_prb_rssi'] = self.df_pm_cell[['sum_prb_rssi','PRBs','populated_PRBs']].apply(self.avg_prb_rssi_calc , axis=1)           
                    #self.df_pm_cell['max_prb_rssi'] = 0.0
                    #self.df_pm_cell['min_prb_rssi'] = 0.0
                    #self.df_pm_cell['sum_prb_rssi'] = 0.0
                    #self.df_pm_cell['avg_prb_rssi'] = 0.0                
                else:
                    self.df_pm_cell['max_prb_rssi'] = 0.0
                    self.df_pm_cell['min_prb_rssi'] = 0.0
                    self.df_pm_cell['sum_prb_rssi'] = 0.0
                    self.df_pm_cell['avg_prb_rssi'] = 0.0
                
                process_flag = "Calculate RSSI values - Binned Avg PUSCH"
                self.df_pm_cell['pusch_rssi']             = self.df_pm_cell[self.pusch_rssi_list].apply(self.weighted_avg_rssi_pusch_formula_new, axis=1)
                              
                process_flag = "Calculate RSSI values - Binned Avg PUCCH"
                self.df_pm_cell['pucch_rssi']         = self.df_pm_cell[self.pucch_rssi_list].apply(self.weighted_avg_rssi_pucch_formula_new, axis=1)                              
            
                process_flag = "Calculate DL PRB Util"      
                # Calculate avg DL prb utilization
                self.df_pm_cell['dl_prb_util_pct'] = self.df_pm_cell[self.dl_prb_util_list].apply(self.dl_prb_util_pct_calc, axis=1)                           
                
                process_flag = "Delete unneeded columns"                  
                # Drop unnecessary columns
                self.df_pm_cell = self.df_pm_cell.drop(['DateInTicks','DateInHours','DateInDays'],axis =1)              
            
                 
                    
                self.result_status = "PROCESS COMPLETE"
            
            #except:
            else:
  
                self.result_status = f"PROCESS Failed after: {process_flag}"
        
        # Output final result
        result = {}                  
        # Parameters
        result['customer'] = self.customer
        result['key'] = self.key
        result['technology'] = self.tech
        #result['report_name'] = self.report_name
        # URL & query body
        result['url'] = self.url
        result['body'] = self.body
        # JSON results
        result['JSON'] = self.json   
        result['resp_status_code'] = self.resp_status_code
        result['resp_headers'] = self.resp_headers
        # Debugging values
        result['result_status'] = self.result_status
        # Query contents
        result['result'] = self.df_pm_cell
        result['rows'] = self.rows
        result['columns'] = self.cols  
        
        return result
    
    

# %%


# %%


# %%
