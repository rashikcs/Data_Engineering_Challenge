import pandas as pd
from .StarSchema import StarSchema

class ETL():
    """
    This class acts as a facade to interact with the DataSchenas subclasses.
    ...

    Attributes
    ----------
    schema_obj:object
    schema_name:str
    schema_arguments:dict
    
    Methods
    -------
    init_params()
    transform_table()
    get_transformed_tables()
    get_merged_table()
    """

    schema_obj = None
    schema_name = None
    schema_arguments =  {'starschema':[ 'dataframe_xlsx_path',
                                        'xlsx_sheet_name',
                                        'dimension_features_without_dimension_name_substring',
                                        'fact_table_columns_containing_dimension_name']
                        }


    def __init__(self, schema_name:str):
        self.schema_obj = self.get_schema(schema_name)
        self.schema_name= schema_name
        
    def get_schema(self, schema_name:str)->object:
        """
        returns the Data schema object if found.
        """

        if schema_name.lower()=='starschema':
            return StarSchema()
        else:
            raise NotImplementedError 

    def init_params(self, **kwargs):
        """
        Finds and initializes the schema with appropiate parameters or raise errors otherwise.
        
        Args:
        
            ***Star Schema***
            dataframe_path:str -> Path of the provided xlsx data
            
            dimension_features_without_dimension_name_substring:dict -> dimension features which 
                                                                        doesn't contain dimension 
                                                                        name as substring.
                                                       
            fact_table_columns_containing_dimension_name:list -> fact table features which 
                                                                 contain dimension name as 
                                                                 substring.                                                                 
        """

        invalid_arguments = set(self.schema_arguments[self.schema_name]) - set(kwargs.keys())

        if self.schema_name.lower() == "starschema" and\
           not invalid_arguments:

            self.schema_obj.init_params(kwargs['dataframe_xlsx_path'],
                                        kwargs['xlsx_sheet_name'],
                                        kwargs['dimension_features_without_dimension_name_substring'],
                                        kwargs['fact_table_columns_containing_dimension_name'])
        else:
            raise ValueError("Invalid Parameter detected while initializing the {} parameters.".format(self.schema_name))
            
    def transform_table(self, save_directory:str = 'output', verbose:bool = True)->None:
        """
        Transform table according to the provided scheme.

        Args:
            save_directory:str   -> directory to save output
            verbose:bool         ->  decides wheather to print putput
        """

        self.schema_obj.transform_table(save_directory, verbose)
        
    def get_transformed_tables(self,  
                               folder_directory:str=None,
                               dataframe_name:str=None,
                               verbose:bool = True)->tuple:
        """
        Returns transformed tables (i.e. fact tables, dimension tables)

        Args:
            folder_directory:str   -> directory to save output
            dataframe_name:str     -> name of the saved table/datafrmae
            verbose:bool           ->  decides wheather to print putput
        """

        return self.schema_obj.get_transformed_tables(folder_directory, dataframe_name, verbose)
        
    def get_merged_table(self,  
                         folder_directory:str=None,
                         dataframe_name:str=None,
                         verbose:bool = True)->pd.core.frame.DataFrame:
        """
        Returns merged table from the transformed tables.

        Args:
            folder_directory:str   -> directory to save output
            dataframe_name:str     -> name of the saved table/datafrmae
            verbose:bool           ->  decides wheather to print putput
        """

        return self.schema_obj.get_merged_table(folder_directory, dataframe_name, verbose)