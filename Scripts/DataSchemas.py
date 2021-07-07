import pandas as pd
from abc import ABC, abstractmethod

class DataSchemas(ABC):
    """
    A class used to define a common API for a set of schema subclasses

    ...

    Attributes
    ----------
    name

    Methods
    -------
    init_params()
    transform_table()
    get_transformed_tables()
    merge_tables()

    """
    @property
    def name(self):
        raise NotImplementedError

    @abstractmethod
    def init_params(self):
        pass

    @abstractmethod
    def transform_table(self):
        pass

    @abstractmethod
    def get_transformed_tables(self):
        pass

    @abstractmethod
    def get_merged_table(self):
        pass