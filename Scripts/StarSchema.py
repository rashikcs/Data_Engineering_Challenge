import copy
import glob
import os
import pandas as pd
from Scripts.DataSchemas import DataSchemas
from Scripts.utils import convert_date_column
from Scripts.utils import create_directory
from Scripts.utils import save_table_as_parquet
from Scripts.utils import read_xlsx
from Scripts.utils import remove_spaces_and_uppercase_df_columns
from Scripts.utils import validate_directory

class StarSchema(DataSchemas):
    """
    A class used to transform and save table according to star-schema

    ...

    Attributes
    ----------
    dataframe
    dataframe_name
    dimension_features_without_dimension_name_substring
    fact_table_columns_containing_dimension_name
    save_directory
    
    Methods
    -------
    init_params()
    drop_dimension_table_columns()
    create_and_save_tables()
    transform_table()
    get_transformed_tables()
    get_merged_table()
    """
    
    @property
    def name(self):
        return "StarSchema"

    def init_params(self, 
                    dataframe_xlsx_path: str,
                    xlsx_sheet_name:str,
                    dimension_features_without_dimension_name_substring:dict,
                    fact_table_columns_containing_dimension_name:list)->None:
        """
        This function initializes the schema with appropiate parameters or 
        raise errors otherwise.

        Args:

            dataframe_xlsx_path:str -> Path of the dataframe_name
            xlsx_sheet_name:str     -> Sheet name in the excel

            dimension_features_without_dimension_name_substring:dict -> dimension features which 
                                                                        doesn't contain dimension 
                                                                        name as substring.

            fact_table_columns_containing_dimension_name:list -> fact table features which 
                                                                 contain dimension name as 
                                                                 substring.

        """
        
        self.dataframe =read_xlsx(dataframe_xlsx_path, xlsx_sheet_name)
        self.dataframe_name = xlsx_sheet_name.upper()
        self.dimension_features_without_dimension_name_substring = dimension_features_without_dimension_name_substring
        self.fact_table_columns_containing_dimension_name = fact_table_columns_containing_dimension_name
        self.save_directory = None
        
        remove_spaces_and_uppercase_df_columns(self.dataframe)
        print(self.name, ': parameter Initialized!!')
        
    def drop_dimension_table_columns(self,
                                     df:pd.core.frame.DataFrame)->pd.core.frame.DataFrame:

        """
        This function drops columns from the passed dataframe (i.e. future fact table)
        that's already used in the dimension table
        """
        for key, value in self.dimension_features_without_dimension_name_substring.items():
            df = df.drop(columns=value)
        return df
    
    def add_column_to_dim_table(self, 
                          column_name:str, 
                          dimension_array:list,
                          dimension_name:str,
                          fact:pd.core.frame.DataFrame):
        """
        This function adds column to the dimension table and removes from
        fact table.

        Args:
            column_name:str
            dimension_name:str
            dimension_array:list
            fact:pd.core.frame.Dataframe

        """
        dimension_array.append(column_name)
        if column_name!=dimension_name+'ID':
            del fact[column_name]

    def valid_dimension_column(self, dimension_name:str, column_name:str)->bool:
        """
        This function checks the validity of a given column name 
        and returns a boolean value.

        Args:
            column_name:str
            dimension_name:str

        """
        if dimension_name in column_name and\
           column_name not in  self.fact_table_columns_containing_dimension_name:
            return True
        else:
            return False

    def create_and_save_tables(self,
                               df:pd.core.frame.DataFrame,
                               save_directory:str,
                               verbose:bool = True):
        """
        This function creates dimension tables and fact table
        and saves in the given directory.

        Args:
            df:pd.core.frame.Dataframe 

            save_directory:str   -> directory to save output
            verbose:bool         ->  decides wheather to print putput
        """

        self.save_directory = os.path.join(save_directory, self.dataframe_name, self.name)
        create_directory(self.save_directory)

        dim_features = copy.deepcopy(self.dimension_features_without_dimension_name_substring)
        
        for key, value in dim_features.items():
            for column in df.columns:
                
                if self.valid_dimension_column(key, column):
                    self.add_column_to_dim_table(column, value, key, df)
            
            if verbose:
                print('DIM_{} Table: '.format(key), dim_features[key])
            
            outname = 'dim_{}.parquet'.format(key)
            save_table_as_parquet(self.save_directory, 
                                  outname,
                                  self.dataframe[dim_features[key]],
                                  verbose)

        if verbose:
            print('\nFact Table:',df.columns.values)
        
        outname = 'fact_'+self.dataframe_name+'.parquet'
        save_table_as_parquet(self.save_directory, 
                              outname, 
                              df,
                              verbose)

    def transform_table(self, save_directory:str = 'Output', verbose:bool = True)->None:
        """
        This function transforms the given table according to star schema 
        and saves resulted tables in the given directory.

        Args:
            save_directory:str   -> directory to save output
            verbose:bool          ->  decides wheather to print putput
        """

        temp_df = self.dataframe.copy(deep=True)
        temp_df = self.drop_dimension_table_columns(temp_df)
        self.create_and_save_tables(temp_df, save_directory, verbose)


    def get_transformed_tables(self, 
                               folder_directory:str=None,
                               dataframe_name:str=None,
                               verbose:bool = True)->tuple:
        """
        Fetches and returns the transformed tables from the 
        saved directory. Returns the created tables by the object
        if no directory provided.

        Args:
            folder_directory:str   -> directory to save output
            dataframe_name:str     -> name of the saved table/datafrmae
            verbose:bool           ->  decides wheather to print putput
        """

        try:
            saved_directory = None

            if folder_directory and dataframe_name:
                saved_directory = validate_directory(folder_directory, os.path.join(dataframe_name, self.name))
            else:
                saved_directory = self.save_directory

            files = glob.glob(os.path.join(saved_directory, "*.parquet"))

            assert len(files)>0

        except Exception as error:
            raise Exception('Caught this error: ' + repr(error))
        else:
            dimension_tables = {}
            fact_table = None
            
            for file in files:

                df = pd.read_parquet(file, engine='pyarrow')
                convert_date_column(df)

                if 'fact' in file:
                    fact_table = df
                else:
                    key = file.split(os.sep)[-1].split('.')[0]
                    dimension_tables[key] = df
                
                if verbose:
                    print('Location:', file)
                    print('File Name:', file.split(os.sep)[-1])

            return fact_table, dimension_tables

    def get_merged_table(self, 
                         folder_directory:str=None,
                         dataframe_name:str=None,
                         verbose:bool = True)->pd.core.frame.DataFrame:
        """
        Merges the transformed tables from the 
        saved directory and returns the dataframe.

        Args:
            folder_directory:str   -> directory to save output
            dataframe_name:str     -> name of the saved table/datafrmae
            verbose:bool           ->  decides wheather to print putput
        """
        try:
            fact_table, dimension_tables = self.get_transformed_tables(folder_directory, dataframe_name, verbose)
            merged_df = fact_table.copy()

            for key in dimension_tables.keys():
                join_id = key.split('_')[1]+'ID'
                merged_df = pd.merge(merged_df, dimension_tables[key], on=join_id,  how='inner')

            return merged_df

        except Exception as error:
            raise Exception('Caught this error: ' + repr(error))