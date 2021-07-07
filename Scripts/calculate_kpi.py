import pandas as pd
from .utils import replace_date_to_month_column

def get_revenues_sum(df:pd.core.frame.DataFrame,
                     variable:str = 'customersegment'):
    """
    This function calculates and returns total revenues according to 
    the given column (i.e. customersegment or month)
    
    Args:
        df:pd.core.frame.DataFrame -> passed dataframe
        variable:str -> Column name to calculate total revenue accordingly

    """    
    variable = variable.upper().replace(' ', '')
    
    if variable=='CUSTOMERSEGMENT':
        result = df[[variable, 'REVENUE']].groupby(variable)['REVENUE'].sum().sort_values(ascending=False)
        return result.reset_index(name='REVENUE')
    elif variable =='MONTH':
        result = df[['ORDERDATE', 'REVENUE']].resample(rule='M', on='ORDERDATE').sum().reset_index()    
        return replace_date_to_month_column(result)
    else:
        raise ValueError("Invalid  variable to calculate sum!!")

def orders_per_month(df:pd.core.frame.DataFrame,
                     aggregate_function:str='sum'):
    """
    This function calculates and returns total orders per month according to 
    the given aggtegate function (i.e. sum or averageordervolume)
    
    Args:
        df:pd.core.frame.DataFrame -> passed dataframe
        aggregate_function:str -> agggregate function to calculate orders

    """      
    aggregate_function = aggregate_function.lower().replace(' ', '')
    if aggregate_function=='sum':
        result = df[['ORDERDATE', 'ORDERID']].resample(rule='M', on='ORDERDATE').count()[['ORDERID']].rename({'ORDERID':'ORDERSPERMONTH'}, axis=1).reset_index()
        return replace_date_to_month_column(result)
    
    elif aggregate_function=='averageordervolume':
        result = df[['ORDERDATE', 'ORDERID']].resample(rule='M', on='ORDERDATE').count()[['ORDERID']].rename({'ORDERID':'ORDERSPERMONTH'}, axis=1)
        result['AVERAGEGORDERVOLUMEPERMONTH'] = result['ORDERSPERMONTH'].expanding().mean()
        result = result.reset_index()
        result = replace_date_to_month_column(result)
        return result[['MONTH', 'AVERAGEGORDERVOLUMEPERMONTH']]
    
    else:
        raise ValueError("Invalid aggregate function to calculate orders per month!!")

def get_top_n_customers( df:pd.core.frame.DataFrame,
                         n:int = 10,
                         sort_column:str = 'LIFETIMEREVENUE')->pd.core.frame.DataFrame:
    """
    This function calculates and returns top n customers according to 
    the given sort column (i.e. LIFETIMEREVENUE or LIFETIMEORDERVOLUME)
    
    Args:
        df:pd.core.frame.DataFrame -> passed dataframe
        n:int  -> selects top n customers
        sort_column:str -> column to select top customers

    """   
    top_customers = None
    sort_column = sort_column.upper().replace(' ', '')
    
    if sort_column =='LIFETIMEREVENUE':
        return df[['CUSTOMERID','CUSTOMERNAME', 'REVENUE']].groupby(['CUSTOMERID', 'CUSTOMERNAME'])['REVENUE'].sum().sort_values(ascending=False)[:n].reset_index(name=sort_column)
    elif sort_column =='LIFETIMEORDERVOLUME':
        return df[['CUSTOMERID', 'CUSTOMERNAME', 'ORDERID']].groupby(['CUSTOMERID', 'CUSTOMERNAME'])['ORDERID'].count().sort_values(ascending=False)[:n].reset_index(name=sort_column)
    else:
        raise ValueError("Invalid  sort value!!")
        
    #merged_customer_name = pd.merge(customer_table[['CUSTOMERID', 'CUSTOMERNAME']], top_customers, on='CUSTOMERID',  how='right')
    return merged_customer_name