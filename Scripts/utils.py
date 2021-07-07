import pandas as pd
import os

def read_xlsx(path:str, sheet_name:str)->pd.core.frame.DataFrame:
    """
    Reads and returns the excel file as dataframe from the 
    provided path.
    
    Args:
        path:str -> Path of the dataframe
        sheet_name:str -> Sheet name in the excel

    """
    try:
        return pd.read_excel(path, sheet_name = sheet_name)
    except Exception as error:
        raise Exception('Caught this error: ' + repr(error))

def create_directory(directory:str)->None:
    """
    Given a directory this function checks and 
    creates creates directory if doesn't exist.
    
    """
    try: 
        if not os.path.exists(directory):
            os.makedirs(directory)
    except Exception as error:
        raise Exception('Caught this error: ' + repr(error))

def convert_date_column(df:pd.core.frame.DataFrame)->None:
    """
    This function checks and convert all columns containing
    substring 'DATE' to datetime object.

    """
    date_columns = [column for column in df.columns if "DATE" in column]
    for column in date_columns:
        df[column] = pd.to_datetime(df[column])

def save_table_as_parquet(directory:str, outname:str, df:pd.core.frame.DataFrame, verbose:bool)->None:
    """
    Given a dataframe this function saves the dataframe as parquet file.

    Args:
        directory:str -> output file directory
        outname:str -> output file name
        df:pd.core.frame.DataFrame -> passed dataframe
        verbose:bool ->  decides wheather to print putput

    """   
    full_name = os.path.join(directory, outname)
    df.drop_duplicates().to_parquet(full_name)
    if verbose:
        print('Table saved:{}'.format(full_name))

def remove_spaces_and_uppercase_df_columns(df:pd.core.frame.DataFrame)->None:
    """
    Given a directory this function remove whitespaces in the column names and 
    makes the names all uppercase.
    
    """    
    df.columns = df.columns.str.upper().str.replace(' ', '')
    
def replace_date_to_month_column(df:pd.core.frame.DataFrame, 
                                 convert_column:str = 'ORDERDATE')->pd.core.frame.DataFrame:
    """
    Given a dataframe this function converts the given datetime column to MONTH.

    Args:
        df:pd.core.frame.DataFrame -> passed dataframe
        convert_column:str -> datetime column to convert

    """ 
    df = df.rename({convert_column:'MONTH'}, axis=1)
    df['MONTH'] = df['MONTH'].dt.strftime('%b')  
    return df