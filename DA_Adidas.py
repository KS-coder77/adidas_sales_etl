#!/usr/bin/env python
# coding: utf-8

# In[56]:


import pandas as pd 
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt


# In[57]:


data = pd.read_excel(r"C:\Users\Krupa\Documents\Krups Coding\Kaggle\DA_Adidas\Adidas US Sales Datasets.xlsx")
data.head()


# In[58]:


new_header = data.iloc[3]
data = data[4:]
data.columns = new_header


# In[59]:


data.columns


# In[60]:


col_to_drop = 0
data = data.drop(data.columns[col_to_drop], axis=1)
data = data.reset_index()
data.head()


# In[61]:


def check_data(df):
    summary = [
        [col, df[col].dtype, df[col].count(), df[col].nunique(), df[col].isnull().sum(), df.duplicated().sum()]
        for col in df.columns] 
    
    df_check = pd.DataFrame(summary, columns = ['column', 'dtype', 'instances', 'unique', 'missing_vals', 'duplicates'])
    
    return df_check 


# In[62]:


check_data(data)


# In[63]:


data.info()


# In[64]:


#function to change date columns to datetime 

def convert_to_datetime(df, col_name):
    try: 
        df[col_name] = pd.to_datetime(df[col_name], format = "%Y%m%d")
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None 


# In[65]:


convert_to_datetime(data, 'Invoice Date')


# In[66]:


# add day of week, month and year cols 

def add_day_month_year_cols(df, col_name):
    df['Day of Week'] = df[col_name].dt.dayofweek
    df['Day Name'] = df[col_name].dt.day_name()
    df['Month'] = pd.to_datetime(df[col_name], format = '%m').dt.month
    df['Year'] = pd.to_datetime(df[col_name], format = '%Y').dt.year
    


# In[67]:


add_day_month_year_cols(data, 'Invoice Date')


# In[68]:


data.head()


# In[69]:


data['Year'].unique()


# In[70]:


def search_men(row):
    if "Men's" in row['Product']:
        return True 
    else: 
        return False  
    
def search_women(row):
    if "Women's" in row['Product']:
        return True
    else:
        return False 


# In[71]:


a= data.copy()
a.head()


# In[72]:


a["Menswear"]=a.apply(lambda row: search_men(row), axis=1)
a["Womenswear"]=a.apply(lambda row: search_women(row), axis=1)


# In[73]:


a.head()


# In[74]:


data['Invoice Date'][0]


# In[75]:


data['Retailer ID'].unique()


# In[76]:


data['Sales Method'].unique()


# In[77]:


# save data as a new csv 


# In[78]:


# let's transform this 1-D pandas df into a SQL relational DB

# create the following tables 

# 1. Date 
# cols: invoice date, year, month, day of week and day name, and date (yy-mm)

# 2. Markets 
# cols: retailer ID, region, state, city

# 3. Products 
# cols: product_code (create new), product_type 

#4. Retailers
# cols: retailer_name, reteailer id

# 5. transactions 
# cols: retailer ID, invoice date, product_code, price_per_unit, units_sold, total_sales, op_profit, op_margin, sales_method, 


# In[79]:


data.dtypes


# In[81]:


#change date column to string format to insert into sqlite table 
data['Invoice Date'] = data['Invoice Date'].dt.strftime(f'%Y-%m-%d')


# In[84]:


data['Invoice Date'].dtype


# In[85]:


transactions_df = data.copy()


# In[86]:


data['Retailer ID'].unique()


# In[87]:


data['Retailer'].unique()


# In[88]:


retailer_df = data[['Retailer', 'Retailer ID']].drop_duplicates()
retailer_df


# In[89]:


# confusing to have multiple retailer IDs for each retail company. let's keep the first ID which appears in the dataset for the corresponding retail company
# we have 4 retailer IDs assigned to 6 retail companies-let's fix this

mapping_x ={'Foot Locker':1185732, 'Walmart': 1197831, 'Sports Direct':1128299, 'West Gear': 1189833, "Kohl's": 1197832, 'Amazon': 1128298}

retailer_df['Retailer Code']=retailer_df['Retailer'].replace(mapping_x)


# In[90]:


retailer_df=retailer_df.drop(columns=['Retailer ID']).drop_duplicates()
retailer_df


# In[91]:


prod_df = data[['Product']].drop_duplicates()
prod_df


# In[92]:


#add a column for prod_code  
a=len(prod_df)
prod_df['Product Code'] = [f'Prod{str(i).zfill(3)}' for i in range(1,a+1)]


# In[93]:


prod_df


# In[94]:


market_df = data[['Region', 'State', 'City']].drop_duplicates()
market_df


# In[95]:


x=len(market_df)
market_df['Market Code'] = [f'Mark{str(i).zfill(3)}' for i in range(1,x+1)]


# In[96]:


market_df


# In[97]:


date_df = data[['Invoice Date', 'Year', 'Month', 'Day of Week', 'Day Name']].drop_duplicates()
date_df


# In[98]:


sales_method_df = data[['Sales Method']].drop_duplicates()
sales_method_df


# In[99]:


y=len(sales_method_df)
sales_method_df['Sales Method Code'] = [f'SM{str(i).zfill(3)}' for i in range(1, y+1)]


# In[100]:


sales_method_df


# In[101]:


transactions_df.head()


# In[102]:


#map sales method code to trans df
mapping ={'In-store':'SM001', 'Outlet':'SM002', 'Online':'SM003'}
transactions_df['Sales Method']=transactions_df['Sales Method'].replace(mapping)


# In[103]:


prod_dict = dict(zip(prod_df['Product'], prod_df['Product Code']))
print(prod_dict)


# In[104]:


transactions_df['Product']=transactions_df['Product'].replace(prod_dict)


# In[105]:


mark_dict = dict(zip(market_df['City'], market_df['Market Code']))
print(mark_dict)


# In[106]:


transactions_df['Market Code']=transactions_df['City'].map(mark_dict)


# In[107]:


transactions_df['Retailer Code']=transactions_df['Retailer'].map(mapping_x)


# In[108]:


transactions_df=transactions_df.drop(columns=['index', 'Retailer', 'Retailer ID', 'Region', 'State', 'City','Day of Week', 'Day Name', 'Month', 'Year'])
transactions_df.head()


# In[109]:


transactions_df.columns


# In[110]:


import sqlite3
import os, sys

#create db filepath 

DATABASE_FILEPATH = r"C:\temp\test105.db"

if os.path.exists(DATABASE_FILEPATH):
    os.remove(DATABASE_FILEPATH)

conn = sqlite3.connect(DATABASE_FILEPATH)
cur = conn.cursor()

cur.executescript("""

-- # 1. Date 
-- # cols: invoice date, year, month, day of week and day name, and date (yy-mm)

CREATE TABLE IF NOT EXISTS sales_date(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inv_date TEXT, 
    year INT, 
    month INT, 
    day_of_week INT, 
    day_name TEXT
    );

-- # 2. Markets 
-- # cols: region, state, city

CREATE TABLE IF NOT EXISTS sales_markets(
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    region TEXT, 
    state TEXT, 
    city TEXT,
    market_code VARCHAR
    );

-- # 3. Products 
-- # cols: product_code (create new), product_type 

CREATE TABLE IF NOT EXISTS sales_products(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_type TEXT, 
    product_code VARCHAR
    );

-- # 4. sales_retailer 
-- # cols: retailer, retailer code

CREATE TABLE IF NOT EXISTS sales_retailers(
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    retailer_name TEXT, 
    retailer_code INT
    );
    
-- # 5. sales_method
-- # cols: sales_method, sales_method_code

CREATE TABLE IF NOT EXISTS sales_method(
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    sales_method TEXT, 
    sales_method_code INT
    );

-- # 6. transactions 
-- # cols: retailer ID, invoice date, product_code, price_per_unit, units_sold, total_sales, op_profit, 
--op_margin, sales_method_code, market_code
-- 'Invoice Date', 'Product', 'Price per Unit',
--       'Units Sold', 'Total Sales', 'Operating Profit', 'Operating Margin',
--       'Sales Method', 'Market Code', 'Retailer Code'
       
CREATE TABLE IF NOT EXISTS sales_transactions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,    
    inv_date TEXT,
    product_code VARCHAR,
    price_per_unit INT, 
    units_sold INT,
    total_sales INT,
    op_profit INT, 
    op_margin INT, 
    sales_method_code VARCHAR,
    market_code VARCHAR, 
    retailer_code INT
    )
    
    """)

# insert data into SQL table 
for index, row in transactions_df.iterrows():    
    cur.execute('''
        INSERT INTO sales_transactions (inv_date, product_code, price_per_unit, 
        units_sold, total_sales, op_profit, op_margin, sales_method_code, market_code, retailer_code) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''',
        (row['Invoice Date'], row['Product'], row['Price per Unit'], 
         row['Units Sold'], row['Total Sales'], row['Operating Profit'], row['Operating Margin'],
         row['Sales Method'], row['Market Code'], row['Retailer Code']));

for index, row in date_df.iterrows():
    cur.execute('''
        INSERT OR IGNORE INTO sales_date (inv_date, year, month, day_of_week, day_name) 
        VALUES (?, ?, ?, ?, ? )''', 
        (row['Invoice Date'], row['Year'], row['Month'], row['Day of Week'], row['Day Name']));
    
for index, row in market_df.iterrows():
        cur.execute('''
        INSERT OR IGNORE INTO sales_markets (region, state, city, market_code) 
        VALUES (?, ?, ?, ?)''',
        (row['Region'], row['State'], row['City'], row['Market Code']));
        
for index, row in prod_df.iterrows():    
        cur.execute('''
        INSERT INTO sales_products (product_type, product_code) 
        VALUES (?, ?)''',
        (row['Product'], row['Product Code']));
        
for index, row in sales_method_df.iterrows():    
        cur.execute('''
        INSERT INTO sales_method (sales_method, sales_method_code) 
        VALUES (?, ?)''',
        (row['Sales Method'], row['Sales Method Code']));

for index, row in retailer_df.iterrows():
    cur.execute('''
        INSERT INTO sales_retailers (retailer_name, retailer_code)
        VALUES (?, ?)''',
               (row['Retailer'], row['Retailer Code']))
    
      
# commit changes and close the connection 
conn.commit()
#conn.close()


# In[111]:


#save df's as csv files 

trans_df=pd.read_sql('SELECT * from sales_transactions', conn)
trans_df.to_csv('sales_trans.csv', index=False)

dates_df=pd.read_sql('SELECT * from sales_date', conn)
dates_df.to_csv('sales_date.csv', index=False)

markets_df=pd.read_sql('SELECT * from sales_markets', conn)
markets_df.to_csv('sales_markets.csv', index=False)

products_df=pd.read_sql('SELECT * from sales_products', conn)
products_df.to_csv('sales_prods.csv', index=False)

sales_method_df=pd.read_sql('SELECT * from sales_method', conn)
sales_method_df.to_csv('sales_method.csv', index=False)

retailer_df=pd.read_sql('SELECT * from sales_retailers', conn)
retailer_df.to_csv('sales_retailers.csv', index=False)


# In[112]:


conn.close()


# In[ ]:




