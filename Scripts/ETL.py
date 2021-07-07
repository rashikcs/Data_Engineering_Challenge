import pandas as pd
from .StarSchema import StarSchema

class ETL():
    """
    A class used to transform and save table according to the provided schema

    ...

    Attributes
    ----------
    schema_obj:object
    schema_name:str
    schema_arguments:dict
    
    Methods
    -------
    found_schema()
    get_schema()
    init_params()
    transform_table()
    get_merged_table()
    """

    schema_obj = None
    schema_name = None
    schema_arguments =  {'starschema':[ 'dataframe_path',
                                        'extra_columns_per_dimension_tables',
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

    def found_schema(self, schema_name:str)->bool:
        """
        Returns true if the schema exists in the schema_arguments dict
        """

        if schema_name in self.schema_arguments.keys():
            return True
        else:
            return False
            
    def init_schema(self, **kwargs)->None:
        """
        Finds and initializes parameters of the schema or throw error otherwise.                                                                
        """

        schema_name = kwargs.pop('schema_name') if 'schema_name' in kwargs.keys() else self.schema_name 

        if self.found_schema(schema_name):

            if schema_name!= self.schema_name:
                self.schema_name = schema_name
                self.schema_obj = self.get_schema(schema_name)

            self.init_params()

        else:
            raise ValueError("Schema not found!!")

    def init_params(self, **kwargs):
        """
        Finds and initializes the schema with appropiate parameters or raise errors otherwise.
        
        Args:
        
            ***Star Schema***
            dataframe_path:str -> Path of the provided xlsx data
            
            extra_columns_per_dimension_tables:dict -> dimension features which 
                                                       doesn't contain dimension 
                                                       name as substring.
                                                       
            fact_table_columns_containing_dimension_name:list -> fact table features which 
                                                                 contain dimension name as 
                                                                 substring.                                                                 
        """

        invalid_arguments = set(self.schema_arguments[self.schema_name]) - set(kwargs.keys())

        if self.schema_name.lower() == "starschema" and\
           not invalid_arguments:

            self.schema_obj.init_params(kwargs['dataframe_path'],
                                        kwargs['extra_columns_per_dimension_tables'],
                                        kwargs['fact_table_columns_containing_dimension_name'])
        else:
            raise ValueError("Invalid Parameter detected while initializing the {} parameters.".format(self.schema_name))
            
    def transform_table(self, verbose:bool = True)->None:
        """
        Transform table according to the provided scheme.
        """

        self.schema_obj.transform_table(verbose)
        
    def get_transformed_tables(self, verbose:bool = True)->object:
        """
        Returns transformed tables (i.e. fact tables, dimension tables)
        """

        return self.schema_obj.get_transformed_tables(verbose)
        
    def get_merged_table(self, verbose:bool = True)->pd.core.frame.DataFrame:
        """
        Returns merged table from the transformed tables.
        """

        return self.schema_obj.get_merged_table(verbose)