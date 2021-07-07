import glob
import os
import pandas as pd
from .DataSchemas import DataSchemas
from .utils import convert_date_column
from .utils import create_directory
from .utils import save_table_as_parquet
from .utils import read_xlsx
from .utils import remove_spaces_and_uppercase_df_columns

class StarSchema(DataSchemas):
    """
    A class used to transform and save table according to star-schema

    ...

    Attributes
    ----------
    dataframe
    dataframe_name
    extra_columns_per_dimension_tables
    fact_table_columns_containing_dimension_name
    fact_table_name
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
                    df_xlsx_path: str,
                    extra_columns_per_dimension_tables:dict,
                    fact_table_columns_containing_dimension_name:list)->None:
        """
        This function initializes the schema with appropiate parameters or 
        raise errors otherwise.

        Args:

            df_xlsx_path:str -> Path of the dataframe

            extra_columns_per_dimension_tables:dict -> dimension features which 
                                                       doesn't contain dimension 
                                                       name as substring.

            fact_table_columns_containing_dimension_name:list -> fact table features which 
                                                                 contain dimension name as 
                                                                 substring.

        """
        self.dataframe = read_xlsx(df_xlsx_path)
        self.dataframe_name = df_xlsx_path.split('/')[-1].split('.')[0].upper()
        self.extra_columns_per_dimension_tables = extra_columns_per_dimension_tables
        self.fact_table_columns_containing_dimension_name = fact_table_columns_containing_dimension_name
        self.fact_table_name = 'fact_'+self.dataframe_name
        self.save_directory = os.path.join('Transformed_tables', self.dataframe_name, self.name)
        print(self.name, ': parameter Initialized!!')
        
    def drop_dimension_table_columns(self)->None:

        """
        This function drops columns from the data frame
        that's already in the dimension table
        """
        for key, value in self.extra_columns_per_dimension_tables.items():
            self.dataframe = self.dataframe.drop(columns=value)
                
            
    def create_and_save_tables(self,
                               org_df:pd.core.frame.DataFrame,
                               verbose:bool = True):
        """
        This function creates dimension tables and fact table
        and saves in the given directory.

        Args:
            org_df:pd.core.frame.DataFrame -> passed dataframe
            verbose:bool ->  decides wheather to print putput
        """

        create_directory(self.save_directory)
        for key, value in self.extra_columns_per_dimension_tables.items():
            for column in self.dataframe.columns:
                
                if key in column and column not in  self.fact_table_columns_containing_dimension_name:
                    value.append(column)
                    if column!=key+'ID':
                        del self.dataframe[column]
            
            if verbose:
                print('DIM_{} Table: '.format(key), self.extra_columns_per_dimension_tables[key])
            
            outname = 'dim_{}.parquet'.format(key)
            save_table_as_parquet(self.save_directory, 
                                  outname, 
                                  org_df[self.extra_columns_per_dimension_tables[key]],
                                  verbose)

        if verbose:
            print('\nFact Table:',self.dataframe.columns.values)
        
        outname = self.fact_table_name+'.parquet'
        save_table_as_parquet(self.save_directory, 
                              outname, 
                              self.dataframe,
                              verbose)

    def transform_table(self, verbose:bool = True)->None:
        """
        This function transforms the given table according to star schema 
        and saves resulted tables in the given directory.
        """

        remove_spaces_and_uppercase_df_columns(self.dataframe)
        org_df = self.dataframe.copy()
        self.drop_dimension_table_columns()
        self.create_and_save_tables(org_df, verbose)

    def get_transformed_tables(self, verbose:bool = True)->tuple:
        """
        Fetches and returns the transformed tables from the 
        saved directory.
        """

        files = glob.glob(os.path.join(self.save_directory, "*.parquet"))

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

    def get_merged_table(self, verbose:bool = True)->pd.core.frame.DataFrame:
        """
        Merges the transformed tables from the 
        saved directory and returns the dataframe.
        """

        fact_table, dimension_tables = self.get_transformed_tables(verbose)
        merged_df = fact_table.copy()

        for key in dimension_tables.keys():
            join_id = key.split('_')[1]+'ID'
            merged_df = pd.merge(merged_df, dimension_tables[key], on=join_id,  how='inner')

        return merged_df