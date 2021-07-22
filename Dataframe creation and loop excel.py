# This code loops through all AEO Price files in a single folder location and creates one harmonized dataframe with all prices
# Reads excel input. Splits data into three different dataframes, manipulates and creates unique fields and concats all
#  dataframes back together

# AEO Price File link: https://www.eia.gov/outlooks/aeo/tables_ref.php
#  -- see "Energy Prices by Sector", tables 3.1 to 3.9


# Field explanations
# Region

Region_dict = {'PRC001': 'New England', 'PRC002': 'Middle Atlantic', 'PRC003': 'East North Central', 'PRC004': 'West North Central', 
              'PRC005': 'South Atlantic', 'PRC006': 'East South Central', 'PRC007': 'West South Central', 'PRC008': 'Mountain',
              'PRC009': 'Pacific'}

#PRC001: 'New England'
#PRC002: 'Middle Atlantic'
#PRC003: 'East North Central'
#PRC004: 'West North Central'
#PRC005: 'South Atlantic'
#PRC006: 'East South Central'
#PRC007: 'West South Central'
#PRC008: 'Mountain'
#PRC009: 'Pacific'

# Currency
#

# Sector
#ba: Residential real
#ca: Commercial real
#da: Industrial real
#ea: Transportation real
#ga: Electric Power real
#ha: Average real price to all users    
#R: Residential nominal
#C: Commercial nominal
#I: Industrial nominal
#T: Transportation nominal
#E: Electric power nominal
#Avg: Average price to all users nominal




# for reading the excel files
import glob
import pandas as pd
import numpy as np

from pandas import Series, DataFrame

# Lookups for regions

Region_list = () 

# Initialize dataframe
DF_AEOprices = []

# Loop that goes through price files and pulls all relevant prices
for file in glob.iglob('AEO21*.xlsx'):
    
    # read excel file. 
    df = pd.read_excel(file,skiplines=16, na_values = '- -').dropna().reset_index().iloc[1:,:-1]
    
    # creates column names for reference
    column_index = [1,2]
    new_column_names = ['UniqueID', 'Fuel']
    old_column_names = df.columns[column_index]
    df.rename(columns=dict(zip(old_column_names, new_column_names)),inplace=True)

    # Splits string in first column into three columns for unique reference
    u = df['UniqueID'].str.split(":|_",n=3,expand = True)
    u1 = u.loc[u[1]!='nom']
    u2 = u.loc[u[1]=='nom']
    u1.insert(1,"Currency Temp","real")
    u1 = u1.iloc[:,:-2]
    u2 = u2.iloc[:,:-1]
    
    # Rename particular columns
    column_index_u1 = [0,1,2]
    new_column_names_u1 = ['Region', 'Currency', 'Sector']
    old_column_names_u1 = u1.columns[column_index_u1]
    u1.rename(columns=dict(zip(old_column_names_u1, new_column_names_u1)),inplace=True)
    
    # Rename particular columns
    column_index_u2 = [0,1,2]
    new_column_names_u2 = ['Region', 'Currency', 'Sector']
    old_column_names_u2 = u2.columns[column_index_u2]
    u2.rename(columns=dict(zip(old_column_names_u2, new_column_names_u2)),inplace=True)
    
    DF_U_Final = u1.append(u2)
    
    # changes data series into float64
    s=df[df.columns[3:]]
    s.astype(np.float64)
    
    # takes "fuel" column from original dataframe
    t=df.iloc[:,2]
    
    # Concats all three tables to one
    DF_CONCAT = pd.concat([DF_U_Final, t, s],axis=1)
    
    # Appends latest dataframe to original
    DF_AEOprices.append(DF_CONCAT)

# Concats latest dataframe to original, initialized dataframe
DF_AEOprices = pd.concat(DF_AEOprices).reset_index()

#DF_AEOprices.replace({'Region': Region_dict})
DF_AEOprices['Region'].replace(Region_dict,inplace=True)
# Strip all leading/lagging spaces from entries in Fuel column
DF_AEOprices['Fuel'] = DF_AEOprices['Fuel'].str.strip() 
DF_AEOprices.head()


# Creates dataframe by querying needed prices
x=('ca','ba')
y=('Natural Gas', 'Electricity')
FilteredQuery = DF_AEOprices[DF_AEOprices['Sector'].isin(x) & DF_AEOprices['Fuel'].isin(y)]
