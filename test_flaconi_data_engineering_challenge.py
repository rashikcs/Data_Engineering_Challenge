import pandas as pd
import unittest
from Scripts.calculate_kpi import get_revenues_sum 
from Scripts.calculate_kpi import get_top_n_customers
from Scripts.calculate_kpi import orders_per_month
from Scripts.StarSchema import StarSchema
from Scripts.utils import read_xlsx
from Scripts.utils import remove_spaces_and_uppercase_df_columns

class TestStarSchemaMethods(unittest.TestCase):
    """
    A class which is used to test the methods of StarSchema Class methods.
    ...

    Attributes
    ----------
    original_dataframe:pd.core.frame.DataFrame

    Methods
    -------
    test_get_merged_table_method()
    """


    def setUp(self):
        """ Load the actual merged table."""

        self.original_dataframe = read_xlsx('sales.xlsx')
        remove_spaces_and_uppercase_df_columns(self.original_dataframe)
        self.original_dataframe = self.original_dataframe.sort_values(['PRODUCTID', 'ORDERID']).reset_index(drop=True)

        self.fact_sales_columns = ['PRODUCTID', 'ORDERID', 'ORDERPRIORITY', 'ORDERQUANTITY', 'DISCOUNT', 'SHIPPINGPRICE',
                                   'TOTAL', 'TOTALAFTERDISCOUNT', 'BOXSIZE', 'SHIPPINGCOST','BOXCOST', 'DELIVERYDATE', 
                                   'CUSTOMERID', 'REVENUE']
        self.dim_customer_columns = ['CUSTOMERID', 'CUSTOMERNAME', 'CUSTOMERREGION', 'CUSTOMERSEGMENT']                       
        self.dim_product_columns = ['UNITPRICE', 'PRODUCTID', 'PRODUCTNAME', 'PRODUCTCATEGORY', 'PRODUCTBASEMARGIN']
        self.dim_order_columns = ['ORDERID', 'ORDERDATE']

    def test_get_transformed_tables(self):
        """
        method to check get_transformed_table function of the StarSchema class
        """  
        fact_table, dimension_tables = star.get_transformed_tables(False)

        self.assertListEqual(list(fact_table.columns.values),
                             self.fact_sales_columns)

        self.assertListEqual(list(dimension_tables['dim_CUSTOMER'].columns.values),
                             self.dim_customer_columns) 
        self.assertListEqual(list(dimension_tables['dim_PRODUCT'].columns.values),
                             self.dim_product_columns) 
        self.assertListEqual(list(dimension_tables['dim_ORDER'].columns.values),
                             self.dim_order_columns) 

    def test_get_merged_table_method(self):
        """
        method to check get_merged_table function of the StarSchema class
        """
        pd.testing.assert_frame_equal(self.original_dataframe,
                                      merged_result_output, 
                                      check_like = True)


class Test_calculate_kpi_Functions(unittest.TestCase):
    """
    A class which is used to test the methods of StarSchema Class methods.
    ...

    Attributes
    ----------
    get_total_monthly_revenue:list
    get_total_revenue_by_customer_segment:list

    orders_per_month:list
    avg_order_volume_per_month:list

    top_10_customerid_by_order:list
    top_10_customerid_by_revenue:list

    Methods
    -------
    test_get_revenues_sum()
    test_orders_per_month()
    test_top_10_customers
    """

    get_total_monthly_revenue = [353288.61, 300610.43, 344940.2 , 358513.21, 403075.57, 315658.33,
                                 358101.83, 365501.18, 365658.94, 357781.78, 383444.27, 398882.05]

    get_total_revenue_by_customer_segment =   [2883893.77,  553162.09,  538636.57,  329763.97]

    orders_per_month = [1563, 1439, 1597, 1559, 1607, 1559, 1579, 1586, 1523, 1608, 1578,1584]

    avg_order_volume_per_month = [1563.0, 1501.0, 1533.0, 1539.5, 1553.0, 1554.0, 1557.57, 1561.12, 
                                  1556.89, 1562.0, 1563.45, 1565.17]

    top_10_customerid_by_order = [100922787, 100922788, 100922789, 100922790, 100922791, 100922792,
                                  100922793, 100922794, 100922795, 100922033]

    top_10_customerid_by_revenue = [100922376, 100922723, 100922321, 100922132, 100922322, 100922384,
                                    100922114, 100922375, 100922652, 100922295]

    def test_get_revenues_sum(self):
        """
        method to check get_revenues_sum method of the calculate_kpi module
        """

        result = list(get_revenues_sum(merged_result_output, variable = 'month')['REVENUE'].values)
        self.assertListEqual(result,
                             self.get_total_monthly_revenue)

        result = list(get_revenues_sum(merged_result_output, variable = 'customer segment')['REVENUE'].values)
        self.assertListEqual(result,
                             self.get_total_revenue_by_customer_segment)

    def test_orders_per_month(self):
        """
        method to check orders_per_month method of the calculate_kpi module
        """

        result = list(orders_per_month(merged_result_output, aggregate_function='sum')['ORDERSPERMONTH'].values)
        self.assertListEqual(result,
                             self.orders_per_month)

        result = orders_per_month(merged_result_output, aggregate_function='average order volume')
        result = list(result['AVERAGEGORDERVOLUMEPERMONTH'].round(2).values)
        self.assertListEqual(result,
                             self.avg_order_volume_per_month)

    def test_top_10_customers(self):
        """
        method to check top_10_customers method of the calculate_kpi module
        """

        result = get_top_n_customers(merged_result_output, sort_column = 'lifetimeordervolume')
        result = list(result['CUSTOMERID'].values)
        self.assertListEqual(result,
                             self.top_10_customerid_by_order)

        result = get_top_n_customers(merged_result_output, sort_column = 'LIFETIMEREVENUE')
        result = list(result['CUSTOMERID'].values)
        self.assertListEqual(result,
                             self.top_10_customerid_by_revenue)

if __name__ == "__main__":

    star = StarSchema()
    star.init_params(df_xlsx_path = 'sales.xlsx',
                     extra_columns_per_dimension_tables = {'PRODUCT': ['UNITPRICE'],
                                                           'CUSTOMER': [],
                                                           'ORDER':[]},
                     fact_table_columns_containing_dimension_name = ['ORDERPRIORITY', 'ORDERQUANTITY'])

    star.transform_table(True)
    merged_result_output = star.get_merged_table(False).sort_values(['PRODUCTID', 'ORDERID']).reset_index(drop=True)

    unittest.main()
