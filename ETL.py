import pandas as pd
import os
import glob


# class StarSchema:
#     """
#     A class used to transform and save table according to star schema

#     ...

#     Attributes
#     ----------
#     extra_columns_per_dimension_tables : dict
#     fact_table_columns_containing_dimension_name : list

#     Methods
#     -------
#     drop_dimension_table_columns()
#     create_and_save_tables()
#     transform_to_star_schema()
#     """

#####################################Transform Tables###################################
        
def drop_dimension_table_columns(df:pd.core.frame.DataFrame,
                                 dimension_features:list)->list:
#dropping columns already in dimension table

    for key, value in dimension_features.items():
        
        if '_' in key:
            value = skip_foreign_key(key,value)
            df = df.drop(columns=value)
            value.append(skip_id)
        else:
            df = df.drop(columns=value)
            
    return df


        
def create_and_save_tables(df:pd.core.frame.DataFrame,
                           org_df:pd.core.frame.DataFrame,
                           dimension_features:dict,
                           exclude_columns:list,
                           directory:str = './transformed_tables',
                           fact_table_name:str = 'fact_sales'):
    
    create_directory(directory)

    print('\n\n')
    for key, value in dimension_features.items():
        for column in df.columns:
            
            if key in column and column not in exclude_columns:
                value.append(column)
                if column!=key+'ID':
                    del df[column]
                    
        print('\nDIM_{} Table: '.format(key), dimension_features[key])
        
        outname = 'dim_{}.parquet'.format(key)
        save_table_as_parquet(directory, outname, org_df[dimension_features[key]])

        
    print('\n\nFact Table:',df.columns)
    
    outname = fact_table_name+'.parquet'
    save_table_as_parquet(directory, outname, df)

def transform_to_star_schema(df:pd.core.frame.DataFrame, 
                             extra_columns_per_dimension_tables:list,
                             fact_table_columns_containing_dimension_name:list):

    remaining_dimension_features = drop_dimension_table_columns(df, extra_columns_per_dimension_tables)
    
    create_and_save_tables(remaining_dimension_features,
                           df,
                           extra_columns_per_dimension_tables,
                           fact_table_columns_containing_dimension_name)
##########################################################################################################


####################################Global Functions######################################################
def create_directory( directory:str)->None:
    
    if not os.path.exists(directory):
        os.makedirs(directory)
        
def save_table_as_parquet(directory:str, outname:str, df:str)->None:
    
    full_name = os.path.join(directory, outname)
    df.drop_duplicates().to_parquet(full_name)
    print('Table saved:',full_name)

def remove_spaces_and_uppercase_df_columns(df:pd.core.frame.DataFrame)->None:
    
    df.columns = df.columns.str.upper().str.replace(' ', '')

########################################################################################################


#################################Load Tables and merge transformed tables###############################
def convert_date_column(df:pd.core.frame.DataFrame)->None:
    
    date_columns = [column for column in df.columns if "DATE" in column]
    for column in date_columns:
        df[column] = pd.to_datetime(df[column])

def get_transformed_tables(directory:str = './transformed_tables')->tuple:
    #https://www.toolbox.com/tech/big-data/question/can-dimension-tables-be-related-to-each-other-092709/
    
    files = glob.glob(os.path.join(directory, "*.parquet"))

    dimension_tables = {}
    fact_table = None
    
    for file in files:

        df = pd.read_parquet(file, engine='pyarrow')
        convert_date_column(df)

        if 'fact' in file:
            fact_table = df
        else:
            key = file.split("\\")[-1].split('.')[0]
            dimension_tables[key] = df
        
        # print the location and filename
        print('Location:', file)
        print('File Name:', file.split("\\")[-1])

    return fact_table, dimension_tables

def merge_fact_and_dimension_tables(fact_table:pd.core.frame.DataFrame, 
                                    dimension_tables:pd.core.frame.DataFrame)->pd.core.frame.DataFrame:
    
    merged_df = fact_table.copy()

    for key in dimension_tables.keys():
        join_id = key.split('_')[1]+'ID'
        merged_df = pd.merge(merged_df, dimension_tables[key], on=join_id,  how='inner')

    return merged_df
########################################################################################################


##########################################Calculate KPIs################################################
def replace_date_to_month_column(df:pd.core.frame.DataFrame)->pd.core.frame.DataFrame:
    df = df.rename({'ORDERDATE':'MONTH'}, axis=1)
    df['MONTH'] = df['MONTH'].dt.strftime('%b')  
    return df

def get_revenues_sum(df:pd.core.frame.DataFrame,
                     variable:str = 'customersegment'):
    
    variable = variable.upper().replace(' ', '')
    
    if variable=='CUSTOMERSEGMENT':
        return df[[variable, 'REVENUE']].groupby(variable)['REVENUE'].sum().sort_values(ascending=False).reset_index(name='REVENUE')
    elif variable =='MONTH':
        result = df[['ORDERDATE', 'REVENUE']].resample(rule='M', on='ORDERDATE').sum().reset_index()
        
        #replace_date_to_month_column(result)
        #result = result.rename({'ORDERDATE':'MONTH'}, axis=1)
        #result['MONTH'] = result['MONTH'].dt.strftime('%b')    

        return replace_date_to_month_column(result)
    else:
        raise ValueError("Invalid  variable to calculate sum!!")

def orders_per_month(df:pd.core.frame.DataFrame,
                     aggregate_function:str='sum'):
    
    aggregate_function = aggregate_function.lower()
    if aggregate_function=='sum':
        result = df[['ORDERDATE', 'ORDERID']].resample(rule='M', on='ORDERDATE').count()[['ORDERID']].rename({'ORDERID':'ORDERSPERMONTH'}, axis=1).reset_index()
        return replace_date_to_month_column(result)
    
    elif aggregate_function=='average order volume':
        result = df[['ORDERDATE', 'ORDERID']].resample(rule='M', on='ORDERDATE').count()[['ORDERID']].rename({'ORDERID':'ORDERSPERMONTH'}, axis=1)
        result['AVERAGEGORDERVOLUMEPERMONTH'] = result['ORDERSPERMONTH'].expanding().mean()
        result = result.reset_index()
        result = replace_date_to_month_column(result)
        return result[['MONTH', 'AVERAGEGORDERVOLUMEPERMONTH']]
    
    else:
        raise ValueError("Invalid aggregate function to calculate orders per month!!")

def get_top_n_customers( df:pd.core.frame.DataFrame, 
                         customer_table:pd.core.frame.DataFrame,
                         n:int = 10,
                         sort_column:str = 'LIFETIMEREVENUE')->pd.core.frame.DataFrame:
    
    top_customers = None
    sort_column = sort_column.upper().replace(' ', '')
    
    if sort_column =='LIFETIMEREVENUE':
        top_customers = df[['CUSTOMERID', 'REVENUE']].groupby('CUSTOMERID')['REVENUE'].sum().sort_values(ascending=False)[:n].reset_index(name=sort_column)
    elif sort_column =='LIFETIMEORDERVOLUME':
        top_customers = df[['CUSTOMERID', 'ORDERID']].groupby('CUSTOMERID')['ORDERID'].count().sort_values(ascending=False)[:n].reset_index(name=sort_column)
    else:
        raise ValueError("Invalid  sort value!!")
        
    merged_customer_name = pd.merge(customer_table[['CUSTOMERID', 'CUSTOMERNAME']], top_customers, on='CUSTOMERID',  how='right')
    return merged_customer_name

########################################################################################################