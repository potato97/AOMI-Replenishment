import pandas as pd
import numpy as np
import math
import datetime
from sqlalchemy import create_engine, text


class MainClass:
     
    def query_sales(**kwargs):

        sal_query = 'select * from test_sales'
        
        conn = kwargs['engine'].connect()

        sales = pd.read_sql(sal_query, con=conn)
        conn.close()

        # adding "MONTH" column to dataframe from "BILLDATE"
        sales["MONTH"] = pd.DatetimeIndex(sales['bill_date']).month
        sales["YEAR"] = pd.DatetimeIndex(sales['bill_date']).year

        #creating new dataframe "newsal" with relevant columns of sales
        newsal = sales[['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','name','MONTH','YEAR','qty']].copy()
        #renaming columns to match output names
        newsal.rename(columns={'name':'store','qty':'CL_SAL'}, inplace = True)
    
        
        #filtering dataframes with kwargs values
        newsal = newsal[(newsal['mapa'] == kwargs['filter_value'][0]) & (newsal["dsd"] == kwargs['filter_value'][1]) & (newsal['category'] == kwargs['filter_value'][2])]
        
        if kwargs['filter_value_nr'][0] != None:
            newsal = newsal[newsal["alu"] == kwargs['filter_value_nr'][0]]    
      
        elif kwargs['filter_value_nr'][1] != None:
            newsal = newsal[newsal["icode"] == kwargs['filter_value_nr'][1]]

        elif kwargs['filter_value_nr'][2] != None:
            newsal = newsal[newsal["season"] == kwargs['filter_value_nr'][2]]
    
        # replacing all NaN values of SEASON column with "None"
        newsal['season'] = newsal['season'].replace(np.nan, "NONE")

        #grouping all relevant columns and calculating closing sales, closing stock for each group
        new_df_sales= newsal.groupby(['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','store','MONTH','YEAR'])['CL_SAL'].sum().reset_index()
        new_df_sales = pd.DataFrame(new_df_sales)
    
        return new_df_sales

    def query_stocks(**kwargs):

        sto_query = 'select * from test_stocks'

        conn = kwargs['engine'].connect()

        stock = pd.read_sql(sto_query, con=conn)
        conn.close()

        # replacing all negative values of QTY column with 0
        stock['qty'] = stock['qty'].mask(stock['qty'].lt(0), 0)

        # adding "MONTH" column to dataframe from "ENTDT"
        stock["MONTH"] = pd.DatetimeIndex(stock['entry_date']).month
        stock["YEAR"] = pd.DatetimeIndex(stock['entry_date']).year
        
        #creating new dataframe "newsto" with relevant columns of stocks
        newsto = stock[['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','name','MONTH','YEAR','qty']].copy()
        #renaming columns to match output names
        newsto.rename(columns={'name':'store','qty':'CL_STO'}, inplace = True)

            
        newsto = newsto[(newsto['mapa'] == kwargs['filter_value'][0]) & (newsto["dsd"] == kwargs['filter_value'][1]) & (newsto['category'] == kwargs['filter_value'][2])]

        if kwargs['filter_value_nr'][0] != None:
            newsto = newsto[newsto["alu"] == kwargs['filter_value_nr'][0]]
        
        elif kwargs['filter_value_nr'][1] != None:
            newsto = newsto[newsto["icode"] == kwargs['filter_value_nr'][1]]
          
        elif kwargs['filter_value_nr'][2] != None:
            newsto = newsto[newsto["season"] == kwargs['filter_value_nr'][2]]
    
       
    
        # replacing all NaN values of SEASON column with "None"
        newsto['season'] = newsto['season'].replace(np.nan, "NONE")

        #grouping all relevant columns and calculating closing sales, closing stock for each group
        new_df_stock= newsto.groupby(['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','store','MONTH','YEAR'])['CL_STO'].sum().reset_index()
        new_df_stock = pd.DataFrame(new_df_stock)
        
        return new_df_stock
    
    def cons_data(**kwargs):
        
        new_df_sales=MainClass.query_sales(**kwargs)
        new_df_stock=MainClass.query_stocks(**kwargs)
        #merging sales and stock dfs
        main_df=pd.merge(new_df_sales,new_df_stock,on=['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','store','MONTH','YEAR'])
        
        '''year_mask = main_df[main_df['YEAR'] == this_year
        main_df = main_df.loc[year_mask]'''
        # adding CL_SAL_VAL, CL_STO_VAL, QSTR and VSTR  columns from function "sal_val", "sto_val", "qst" and "vst" respectively
        qstr = []
        vstr = []
        sal_vals = []
        sto_vals = []
        for x,v in main_df.iterrows():
            sales = v['CL_SAL']
            stocks = v['CL_STO']
            rsp = v['mrp']
            qstr.append(MainClass.qst(sales, stocks))
            vstr.append(MainClass.vst(sales, stocks, rsp))
            sal_vals.append(MainClass.sal_val(sales, rsp))
            sto_vals.append(MainClass.sto_val(stocks, rsp))
        main_df['CL_SAL_VAL'] = sal_vals
        main_df['CL_STO_VAL'] = sto_vals
        main_df['QSTR'] = qstr
        main_df['VSTR'] = vstr
        return main_df.to_dict(orient='records')

# function qst to calculate QSTR in %age
# input- function parameters:sales - sales qty, stocks - stock qty
# output- returns qst (in %age) after applying formula
    def qst(sales, stocks):
        total = sales + stocks
        if total == 0:
            qst = 0
        else:
            qst = 100* sales/total
        return round(qst,2)

# function vst to calculate VSTR in %age
# input- function parameters:sales - sales qty, stocks - stock qty, rsp - rsp value
# output- returns vst (in %age) after applying formula
    def vst(sales, stocks,rsp):
        total = stocks*rsp
        if total == 0:
            vst = 0
        else:
            vst = 100* sales*rsp/total
        return round(vst,2)

# function sal_val to calculate closing sales value
# input- function parameters:sales - sales qty, rsp - rsp value
# output- returns closing sales value
    def sal_val(sales, rsp):
        val=sales*rsp
        return val

# function sto_val to calculate closing stock value
# input- function parameters:stocks - stock qty, rsp - rsp value
# output- returns closing stock value
    def sto_val(stocks, rsp):
        val=stocks*rsp
        return val
# function wh_sto to calculate quantites of stock(s) in warehouses
# input- function parameters: stock df
# output- returns stock df with 'WAREHOUSE','WH_STOCK' columns
    def wh_sto(**kwargs):

        sto_df=MainClass.query_stocks(**kwargs)
        #filtering dataframe where "store" is any of the 3 warehouses
        sto_df = sto_df[(sto_df["store"] == 'INVENTIS RETAIL INDIA PVT LTD') | (sto_df['store'] == 'Amazon warehouse') | (sto_df['store'] == 'INVENTIS RETAIL INDIA PVT LTD - NAGASANDRA WH')]
        stor_df=sto_df.groupby(['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','store','MONTH','YEAR'])['CL_STO'].sum().reset_index()
        stor_df=pd.DataFrame(stor_df)
        stor_df.rename(columns={'store':'WAREHOUSE','CL_STO':'WH_STOCK'},inplace=True)
        return stor_df
    
# function avg_sales to calculate mean of sales by icode per store per month
# input- function parameters: icode, store, month, and merged sales and stock df
# output- returns mean of sales for given data
    def avg_sales(icode, store, month, df):
    
        df_new = df[(df['icode'] == icode) & (df['store'] == store) & (df['MONTH'] == month)]

        mean = df_new['CL_SAL'].mean()
        return mean
# function pick to calculate pick quantity by icode per store
# input- function parameters: merged sales and stock df
# output- returns merged sales and stock df with "PICK" column
    def pick(**kwargs):

        new_df_sales=MainClass.query_sales(**kwargs)
        new_df_stock=MainClass.query_stocks(**kwargs)

        main_df=pd.merge(new_df_sales,new_df_stock,on=['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','store','MONTH','YEAR'])


        curr_month = datetime.datetime.now().month
        curr_year = datetime.datetime.now().year
    
        pred_qty = []
        for x, v in main_df.iterrows():
    
            icode = v['icode']
            store = v['store']
            month = v['MONTH']
            mean = MainClass.avg_sales(icode, store, month, main_df)
            pred_qty.append(math.ceil(mean))
        
        main_df['pred_qty'] = pred_qty    
        main_df['PICK'] = np.where(main_df['pred_qty'] <=  main_df['CL_STO'], main_df['pred_qty'] -  main_df['CL_STO'], main_df['pred_qty'] -  main_df['CL_STO'])
        main_df.drop(['pred_qty'], inplace=True ,axis = 1) 
        main_df.rename(columns = {'CL_SAL':'SOLD', 'CL_STO':'SOH'}, inplace = True) 
        # filtering current year and current month
        main_df = main_df[(main_df['YEAR'] == curr_year)  & (main_df['MONTH'] == 5)] ##replace '5' with curr_month when current data available

        wh_df=MainClass.wh_sto(**kwargs)
        final=pd.merge(main_df,wh_df)#, on=['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','MONTH','YEAR'])
        return final.to_dict(orient='records')
    
# function transfers to calculate number of items to be transferred per store based on pick list
# input- function parameters:pick df
# output- returns transfers df with 'FROM', 'TO', 'EXCHG POINT','TRANSFER QTY' columns
    def transfers(**kwargs):
        
        pick_df=MainClass.pick(**kwargs)
        df = pd.DataFrame.from_dict(pick_df)
        tr_df = df[['ICODE','STORE','WAREHOUSE','PICK']].copy()
        #taking entries with positive pick quantities into df 'pos'
        pos=tr_df[(tr_df['PICK'] > 0)]
        #taking entries with negative pick quantities into df 'neg'
        neg=tr_df[(tr_df['PICK'] < 0)]
        neg['PICK'] = neg['PICK'].abs()
        tr_df['TO']="TBD"
        for x1, v1 in neg.iterrows():
            for x2, v2 in pos.iterrows():           
                if v1['ICODE']==v2['ICODE']:
                    if v1['PICK']==v2['PICK']:
                        tr_df["TO"].iloc[x1]=v2["STORE"]
        tr_df=tr_df[(tr_df['PICK'] < 0)]
        tr_df['PICK'] = tr_df['PICK'].abs()
        #renaming columns to match output names
        tr_df.rename(columns={'STORE':'FROM', 'WAREHOUSE':'EXCHG POINT','PICK':'TRANSFER QTY'},inplace=True)
        return tr_df.to_dict(orient='records')
    
# function ranking to calculate ranks per store according to str_dsd
# input- function parameters:merged sales and stock df
# output- returns merged sales and stock df with "AVG_QST" column
    
    def ranking(**kwargs):#VELOCITY COLUMN TO BE ADDED
        
        new_df_sales=MainClass.query_sales(**kwargs)
        new_df_stock=MainClass.query_stocks(**kwargs)
        #merging sales and stock dfs
        main_df=pd.merge(new_df_sales,new_df_stock,on=['icode', 'alu', 'color','size','desc','category','mapa','season','mrp','dsd','store','MONTH','YEAR'])
        qstr = []
    
        for x,v in main_df.iterrows():
            sales = v['CL_SAL']
            stocks = v['CL_STO']
            rsp = v['mrp']
            qstr.append(MainClass.qst(sales, stocks))
        
        main_df['QSTR'] = qstr
    
        new=MainClass.avg_qst(main_df)
        final=MainClass.str_dsd(new)
        final.drop(['desc','alu','size','color','mapa','category','dsd','rsp','season','CL_SAL','CL_STO','QSTR'], axis = 1, inplace = True) 

        final.sort_values(by=['icode','STR_DSD'],ascending = False, inplace=True)
    
        un=final["icode"].unique()

        out=[]
        for i in range(len(un)):

            #number of rows in df        
            temp_df=final[(final["icode"] == un[i])]#selecting all entries with same ICODE
            temp_df["Rank"] = temp_df["STR_DSD"].rank(method='first',ascending = 0)
            
            out.append(temp_df)
        out = pd.concat(out)
 
        return out.to_dict(orient='records')



# function avg_qst to calculate mean of qst by icode per store
# input- function parameters:merged sales and stock df
# output- returns merged sales and stock df with "AVG_QST" column


    def avg_qst(final_data):
        avg_qst = {}
        temp_df = pd.DataFrame()
        temp_new= pd.DataFrame()

        un=final_data["ICODE"].unique()
        print(un)
        new_df= final_data.groupby(['icode','store','MONTH','CL_SAL','CL_STO'])['QSTR'].sum().reset_index()
      


        for i in range(len(un)):

   
            #number of rows in df        
            temp_df=final_data[(final_data["icode"] == un[i])]#selecting all entries with same ICODE
           
            stores = temp_df['store'].unique()
        
            for j in range(len(stores)):
        
                new=temp_df[(temp_df["store"] == stores[j])]
            
                #print(new)
            
                ndf=new['QSTR']
                avg=ndf.mean()
                new['AVG_QST'] = avg
                #adding avg_qst values for each unique ICODE
                temp_new = temp_new.append(new)
        return temp_new
    
# function str_dsd to calculate mean of qst by dsd per store
# input- function parameters:merged sales and stock df
# output- returns merged sales and stock df with "STR_DSD" column

    def str_dsd(final_data):
       
        data=final_data.copy()

        stores = data['store'].unique()
        icode = data['icode'].unique()
        df=data
        dsd = list(df['dsd'].unique())
        
        temp_df = pd.DataFrame()

        for i in dsd:
            dsd_df = data.loc[data['dsd'] == i].copy()
            temp_df = temp_df.append(dsd_df)
            dsd_dict = {}

        for i in icode:
        
            df_temp = temp_df.loc[temp_df['icode'] == i].copy()
            for i in stores:
                df_temp = temp_df.loc[temp_df['store'] == i].copy()
                df2=df_temp['QSTR']
                sum_val=df2.values.sum()
        
                if df2.shape[0]==0:
                    dsd_dict[i] = 0
                else:
                    dsd_dict[i]=round((sum_val/(df2.shape[0])),2)
    
        vals=[]

        for i in df['store']:
            for k,v in dsd_dict.items():
                if i==k:
                    vals=vals+[v]

        df['STR_DSD']=vals
        return df


