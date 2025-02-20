# sudo /usr/bin/python3 /tmp/scoring/validate.py '{\"token\":\"abcd\"}'
import json
import copy
import os
from result_output import ResultOutput
import importlib.util
import sys
import signal
import inspect
import main_script, solution_script 
import logging 
import re
import contextlib
from mongomock import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import DataFrame
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.databricks import AzureDatabricksManagementClient
from pyspark.sql import SparkSession

from databricks import sql
import subprocess

import requests
from requests.auth import HTTPBasicAuth

# configure the logging module, can be used for debugging
os.chdir("/home/labuser/")
logging.basicConfig(filename = 'validate_script.log', level = logging.INFO, filemode = 'w', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
if os.path.exists("/tmp/scoring"):
    os.chdir("/tmp/scoring")
elif os.path.exists("/tmp/clv"):
    os.chdir("/tmp/clv")
    
class Activity:
    """ The class asserts the equality between the expected and actual result.
    main_script.py/user's solution generates the actual_result 
    solution_script.py generates the expected_result

    Attributes
    ----------
    - string messages that report the status of the evaluation
    - input/output dataset/database paths

    Methods
    -------
    alarm_handler(signal_number, frame)
        Raise a time limit exceeded exception
    
    update_result_when_test_fails((test_object,
                             expected_outcome, actual_outcome, reference="N/A")
        Reports the failure of a test case
    
    update_result_when_test_passes(test_object,
                             expected_outcome, actual_outcome, reference="N/A")
        Reports success of a test case

    read_from_file(path)
        Read a file given path 
    
    methods for each test case, where the custom validation input is provided, the actual and expected 
    results are compared
    The methods can handle - plain function returns, file/database operations and OOPs based validation 
    """

    # comment can either be pass/fail
    test_failed = "Test Failed"
    test_passed = "Test Passed"

    # solution_script error
    solution_script_not_found = "An internal error occurred before the validation"
    error_in_solution_script = "An internal error occurred while validating the function/class"
    syntax_error_in_solution_script = "Validation not performed due to an internal error"
    unable_to_reload_modules = "An internal error occurred before the validation"
    
    # main_script errors
    main_script_not_found = "The solution file cannot be found, do not rename files in the Project folder"
    syntax_error_in_main_script = "A syntax error has occurred, please resolve it before submitting for evaluation"
    invalid_import = "An error occurred due to an invalid import statement, try again"
    error_occurred_during_function_execution = "An error occurred during the execution of {}() : {}"
    function_not_found = "{}() not found in user solution"
    time_limit_exceeded = "Time limit exceeded"
    built_in_function_or_module_used = "Rewrite the logic without the use of built-in functions/modules"
    built_in_function_or_module_not_used = "Rewrite the logic with the use of the appropriate built-in functions/modules"
    
    test_case_descriptions = {
            1 : " Verify data extraction from mongodb.",
            2 : " Verify spark dataframe creation.",
            3 : " Verify top 5 customers who palced most orders.",
            4 : " Verify top 5 revenue products.",
            5 : " Verify transforming data to create detailed orders.",
            6 : " Verify Databricks Table resources.",
            7 : " Verify data in Snowflake table.",
            8 : " Verify total revenue by category."               
            }

    def __init__(self):
        # initialize the variables to be used across the testcases
        self.expected_products_collection, self.expected_orders_collection, self.expected_order_details_collection = None,None,None
        self.actual_products_collection, self.actual_orders_collection, self.actual_order_details_collection = None,None,None
        self.expected_products_df, self.expected_orders_df, self.expected_order_details_df = None,None,None
        self.actual_products_df, self.actual_orders_df, self.actual_order_details_df = None,None,None
        self.expected_products_spark_df, self.expected_orders_spark_df, self.expected_orderdetails_spark_df = None,None,None
        self.actual_products_spark_df, self.actual_orders_spark_df, self.actual_orderdetails_spark_df = None,None,None
        self.actual_final_order_details_spark_df = None
        self.expected_final_order_details_spark_df = None
        self.resource_group_name = "Test"
        self.workspace_name = "demoworkspace"
        
        # Ensure the file exists
    def get_databricks_http_path(workspace_url, databricks_token):
        databricks_host = workspace_url  # Directly use the workspace URL

        # Get SQL Warehouses from Databricks
        sql_endpoint_url = f"https://{databricks_host}/api/2.0/sql/warehouses"

        headers = {
            "Authorization": f"Bearer {databricks_token}",
            "Content-Type": "application/json"
        }

        response = requests.get(sql_endpoint_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to retrieve SQL Warehouses: {response.text}")

        warehouses = response.json().get("warehouses", [])
        
        if not warehouses:
            raise Exception("No SQL Warehouses found in Databricks.")

        # Selecting the first available warehouse
        warehouse = warehouses[0]  
        http_path = warehouse["odbc_params"]["path"]

        return databricks_host, http_path


    def get_databricks_connection(workspace_url, databricks_http_path, access_token):
        # Establish connection
        conn = sql.connect(
            server_hostname=workspace_url,
            http_path=databricks_http_path,
            access_token=access_token
        )

        return conn


    # Retrieve Databricks credentials
    workspace_url, databricks_token = solution_script.databricks_details()

    # Get Databricks Host and HTTP Path
    databricks_host, http_path = get_databricks_http_path(workspace_url, databricks_token)

    # Prepare connection parameters
    workspace_full_url = f"https://{workspace_url}"
    access_token = databricks_token

    # Establish connection
    conn = get_databricks_connection(workspace_full_url, http_path, access_token)


    @staticmethod
    def alarm_handler(signal_number, frame):  
        raise Exception(Activity.time_limit_exceeded)

    @staticmethod
    def update_result_when_test_fails(test_object,
                             expected_outcome, actual_outcome, marks=10, marks_obtained=0, reference="N/A"):
        return test_object.update_result(result=0, expected=expected_outcome,
                                             actual=actual_outcome,
                                             comment=Activity.test_failed,
                                             marks=marks,
                                             marks_obtained=marks_obtained,
                                             ref=reference)
   
    @staticmethod
    def update_result_when_test_passes(test_object,
                             expected_outcome, actual_outcome, marks=10, marks_obtained=10, reference="N/A"):
        return test_object.update_result(result=1, expected=expected_outcome,
                                             actual=actual_outcome,
                                             comment=Activity.test_passed,
                                             marks=marks,
                                             marks_obtained=marks_obtained,
                                             ref=reference)   
    
    @staticmethod
    def read_from_file(path):
        try:
            with open(path) as file_reader:
                return file_reader.readlines()
        except:
            return "FileNotFound"
    
    @staticmethod
    def remove_asset_before_next_evaluation(path):
        try:
            os.remove(path)
            logging.info("Removed the asset at {}".format(path))
        except Exception as exception:
            logging.info("Asset at {} does not exist".format(path))
            pass

    @staticmethod
    def report_error_before_validating_tests(test_object, actual_outcome):
        """ In the case of a SyntaxError or module reload error, report all the test 
        cases as failed with the appropriate error message
        """

        number_of_tests = len(json.loads(test_object.result_final())['testCases'])
        for index in range(1, number_of_tests + 1):
            test_object.update_pre_result(description=Activity.test_case_descriptions[index])   
            Activity.update_result_when_test_fails(test_object=test_object,
                            expected_outcome="N/A", 
                            actual_outcome=actual_outcome)
        return test_object

    @staticmethod 
    def execute_query_and_return_result(database_connector, query):
        result = []
        if database_connector:
            cursor = database_connector.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
        return result 

    @staticmethod
    def assert_spark_dataframes_equal(df1: DataFrame, df2: DataFrame) -> bool:
        try:
            # Compare data by sorting and collecting rows
            data1 = df1.orderBy(df1.columns).collect()
            data2 = df2.orderBy(df2.columns).collect()

            if data1 != data2:
                raise AssertionError("DataFrames are not equal")

            return True  # DataFrames are equal

        except AssertionError as e:
 
            return False
    
    # Test Case to validate data extraction from mongodb and pandas dataframe creation
    def testcase_question_one(self, test_object):
        testcase_description = Activity.test_case_descriptions[1]  
        test_object.update_pre_result(description=testcase_description)             
        marks = 10
        marks_obtained = 0
        function = 'extract_data_from_mongodb'
        testcase = 'testcase_question_one'
        expected_outcome = 'Data should be extracted from given collection and converted into pandas dataframe.'
        ref_if_failure_or_exception = "TBD"
        test_passed = False
        
        try:  
            if function in dir(solution_script):
                with contextlib.redirect_stdout(None):
                    self.expected_products_collection,self.expected_orders_collection,self.expected_order_details_collection = solution_script.create_Mongo_Resources()
                    self.expected_products_df, self.expected_orders_df, self.expected_order_details_df = solution_script.extract_data_from_mongodb(self.expected_products_collection,self.expected_orders_collection,self.expected_order_details_collection)
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                      
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3) 
                try:
                    with contextlib.redirect_stdout(None):
                        self.actual_products_collection,self.actual_orders_collection,self.actual_order_details_collection = main_script.create_Mongo_Resources()
                        self.actual_products_df, self.actual_orders_df, self.actual_order_details_df = main_script.extract_data_from_mongodb(self.expected_products_collection,self.expected_orders_collection,self.expected_order_details_collection)
                
                except Exception as exception:
                    return Activity.update_result_when_test_fails(test_object=test_object,
                        expected_outcome=expected_outcome, 
                        actual_outcome=Activity.error_occurred_during_function_execution.format(function, exception))
                finally:
                    signal.alarm(0)
            else:
                logging.error("{} not found in user solution".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                        expected_outcome=expected_outcome, 
                        actual_outcome=Activity.function_not_found.format(function))
            logging.info("Before try")                
            try:
                logging.info("Into try")           
                if (self.actual_products_df is not None and self.actual_orders_df is not None and self.actual_order_details_df is not None):
                    logging.info("Inside if condition")           
                    # Use pandas assert_frame_equal to check equality
                    pd.testing.assert_frame_equal(self.expected_products_df, self.actual_products_df)
                    pd.testing.assert_frame_equal(self.expected_orders_df, self.actual_orders_df)
                    pd.testing.assert_frame_equal(self.expected_order_details_df, self.actual_order_details_df) 
                    test_passed = True  # Set to True if no exception is raised
                else:
                    logging.info("Inside else condition")
                    test_passed = False
            except AssertionError as e:
                test_passed = False  # Remain False if an exception is raised              
            logging.info("After Try")
            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='Data extraction and dataframes creation are as expected.',marks=marks, marks_obtained= marks)
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='Data extraction and dataframes creation is not as expected.',
                                                reference=ref_if_failure_or_exception,marks=marks)             
        
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_one"] = str(e)
            return Activity.update_result_when_test_fails(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome="invalid operation",
                                                reference=ref_if_failure_or_exception)

    # Test Case to validate spark dataframe creation
    def testcase_question_two(self, test_object):    
        testcase_description = Activity.test_case_descriptions[2]  
        test_object.update_pre_result(description=testcase_description)             
        marks = 10
        marks_obtained = 0
        function = 'create_spark_dataframe'
        testcase = 'testcase_question_two'
        expected_outcome = 'Spark dataframe created must be as expected.'
        ref_if_failure_or_exception = "N/A"
        test_passed = False

        try: 
            if function in dir(solution_script):
                self.expected_products_spark_df, self.expected_orders_spark_df, self.expected_orderdetails_spark_df = solution_script.create_spark_dataframe(self.expected_products_df, self.expected_orders_df, self.expected_order_details_df)                
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
            
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3) 
                try:
                    with contextlib.redirect_stdout(None):
                        self.actual_products_spark_df, self.actual_orders_spark_df, self.actual_orderdetails_spark_df = main_script.create_spark_dataframe(self.actual_products_df, self.actual_orders_df, self.actual_order_details_df)
                except Exception as exception:
                    return Activity.update_result_when_test_fails(test_object=test_object,
                        expected_outcome=expected_outcome, 
                        actual_outcome=Activity.error_occurred_during_function_execution.format(function, exception))
                finally:
                    signal.alarm(0)
            else:
                logging.error("{} not found in user solution".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                        expected_outcome=expected_outcome, 
                        actual_outcome=Activity.function_not_found.format(function))

            if (self.actual_products_spark_df is not None and self.actual_orders_spark_df is not None and self.actual_orderdetails_spark_df is not None):
                product_spark_equal = Activity.assert_spark_dataframes_equal(self.expected_products_spark_df, self.actual_products_spark_df)
                order_spark_equal = Activity.assert_spark_dataframes_equal(self.expected_orders_spark_df, self.actual_orders_spark_df)
                order_details_spark_equal = Activity.assert_spark_dataframes_equal(self.expected_orderdetails_spark_df, self.actual_orderdetails_spark_df)  
            else:
                product_spark_equal = order_spark_equal = order_details_spark_equal = False

            if(product_spark_equal and order_spark_equal and order_details_spark_equal):
                test_passed = True
            else:
                test_passed = False

            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='store database has been created.', marks= marks, marks_obtained=marks)
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='store database has not been created.',
                                                reference=ref_if_failure_or_exception,  marks= marks)
            
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_two"] = str(e)
            return Activity.update_result_when_test_fails(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome=Activity.invalid_import,
                                                reference=ref_if_failure_or_exception,  marks= marks)  
        
    # Test case to validate top 5 customers
    def testcase_question_three(self, test_object):   
        testcase_description = Activity.test_case_descriptions[3]  
        test_object.update_pre_result(description=testcase_description)             
        expected_result, actual_result = None, None
        marks = 5
        marks_obtained = 0
        function = 'get_top5_customers_by_orders'
        testcase = 'testcase_question_three'
        expected_outcome = 'Top 5 customers by Orders should be as expected.'
        ref_if_failure_or_exception = "N/A"
        test_passed = False
        
        try:    
            if function in dir(solution_script):                
                expected_result = solution_script.get_top5_customers_by_orders(self.expected_orders_spark_df)                

            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                                 
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3)
                try:
                    with contextlib.redirect_stdout(None):
                        actual_result = main_script.get_top5_customers_by_orders(self.actual_orders_spark_df)    
                except Exception as exception:
                    return Activity.update_result_when_test_fails(
                        test_object=test_object,expected_outcome=expected_outcome, 
                        actual_outcome=Activity.error_occurred_during_function_execution.format(function, exception)
                    )
                finally:
                    signal.alarm(0)
            else:
                logging.error("{} not found in user solution".format(function))
                return Activity.update_result_when_test_fails(
                    test_object=test_object,expected_outcome=expected_outcome, 
                    actual_outcome=Activity.function_not_found.format(function)
                )

            if (actual_result is not None):
                test_passed = Activity.assert_spark_dataframes_equal(expected_result, actual_result)

            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Products collection in store Database is created.', 
                    marks=marks, marks_obtained=marks
                )
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Products collection in store Database is not created.',
                    reference=ref_if_failure_or_exception, marks=marks
                )
                
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_three"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object, expected_outcome=expected_outcome, 
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception, marks=marks
            )

    # Test case to validate top 5 products
    def testcase_question_four(self, test_object):   
        testcase_description = Activity.test_case_descriptions[4]  
        test_object.update_pre_result(description=testcase_description)             
        expected_result, actual_result = None, None
        marks = 5
        marks_obtained = 0
        function = 'calculate_top5_revenue_products'
        testcase = 'testcase_question_four'
        expected_outcome = 'Top 5 revenue products is as expected.'
        ref_if_failure_or_exception = "N/A"
        test_passed = False
        
        try:    
            if function in dir(solution_script):                
                expected_result = solution_script.calculate_top5_revenue_products(self.expected_products_spark_df,self.expected_orderdetails_spark_df)                
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                                 
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3)
                try:
                    with contextlib.redirect_stdout(None):
                        actual_result = main_script.calculate_top5_revenue_products(self.actual_products_spark_df,self.actual_orderdetails_spark_df)    
                except Exception as exception:
                    return Activity.update_result_when_test_fails(
                        test_object=test_object,expected_outcome=expected_outcome, 
                        actual_outcome=Activity.error_occurred_during_function_execution.format(function, exception)
                    )
                finally:
                    signal.alarm(0)
            else:
                logging.error("{} not found in user solution".format(function))
                return Activity.update_result_when_test_fails(
                    test_object=test_object,expected_outcome=expected_outcome, 
                    actual_outcome=Activity.function_not_found.format(function)
                )

            if (actual_result is not None):
                test_passed = Activity.assert_spark_dataframes_equal(expected_result, actual_result)

            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Top 5 revenue products is as expected.', 
                    marks=marks, marks_obtained=marks
                )
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Top 5 revenue product is not as expected.',
                    reference=ref_if_failure_or_exception, marks=marks
                )
                
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_four"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object, expected_outcome=expected_outcome, 
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception, marks=marks
            )

    # Test case to validate transformed spark dataframe
    def testcase_question_five(self, test_object):
        testcase_description = Activity.test_case_descriptions[5]
        test_object.update_pre_result(description=testcase_description)
        marks = 10
        marks_obtained = 0
        function = 'transform_order_details'
        testcase = 'testcase_question_five'
        expected_outcome = "Transformed Spark dataframe should be as expected."
        ref_if_failure_or_exception = "N/A"
        test_passed = False

        try:
            # Validate function existence in solution script
            if function in dir(solution_script):
                expected_result = solution_script.transform_order_details(self.expected_products_spark_df, self.expected_orders_spark_df, self.expected_orderdetails_spark_df)                
                self.expected_final_order_details_spark_df = expected_result
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)

            # Validate function existence in the main script
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3)
                try:
                    with contextlib.redirect_stdout(None):
                        actual_result = main_script.transform_order_details(self.actual_products_spark_df, self.actual_orders_spark_df, self.actual_orderdetails_spark_df)    
                    self.actual_final_order_details_spark_df = actual_result
                except Exception as exception:
                    return Activity.update_result_when_test_fails(
                        test_object=test_object,expected_outcome=expected_outcome, 
                        actual_outcome=Activity.error_occurred_during_function_execution.format(function, exception)
                    )
                finally:
                    signal.alarm(0)
            else:
                logging.error("{} not found in user solution".format(function))
                return Activity.update_result_when_test_fails(
                    test_object=test_object,expected_outcome=expected_outcome, 
                    actual_outcome=Activity.function_not_found.format(function)
                )            

            if (self.actual_final_order_details_spark_df!=None):
                test_passed = Activity.assert_spark_dataframes_equal(self.expected_final_order_details_spark_df, self.actual_final_order_details_spark_df)
            
            if test_passed:
                logging.info("Expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object,
                    expected_outcome=expected_outcome,
                    actual_outcome='Transformed order details is as expected.',
                    marks=marks,
                    marks_obtained=marks
                )
            else:
                logging.info("Expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object,
                    expected_outcome=expected_outcome,
                    actual_outcome='Transformed order details is not as expected.',
                    reference=ref_if_failure_or_exception,
                    marks=marks
                )
        except Exception as e:
            logging.error("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_five"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object,
                expected_outcome=expected_outcome,
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception,
                marks=marks
            )
    def testcase_question_six(self, test_object):   
        testcase_description = Activity.test_case_descriptions[6]  
        test_object.update_pre_result(description=testcase_description)             
        expected_result, actual_result,result = None, None,None
        marks = 5
        marks_obtained = 0
        testcase = 'testcase_question_six'
        expected_outcome = 'ORDER_DETAILS table should be created'
        ref_if_failure_or_exception = "N/A"
        test_passed = False
        
        try:    
            self.expected_connection  = self.conn
            
            check_table_query = "SHOW TABLES LIKE 'ORDER_DETAILS'"

            with self.expected_connection.cursor() as cursor:
                cursor.execute(check_table_query)
                table_exists = cursor.fetchall()
                                 
            
            if (table_exists):
                test_passed = True

            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Table created as expected.', 
                    marks=marks, marks_obtained=marks
                )
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Table creation not as expected.',
                    reference=ref_if_failure_or_exception, marks=marks
                )
                
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_six"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object, expected_outcome=expected_outcome, 
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception, marks=marks
            )
    
    # Testcase to validate the snwoflake resources
    def testcase_question_seven(self, test_object):
        testcase_description = Activity.test_case_descriptions[7]
        test_object.update_pre_result(description=testcase_description)
        marks = 10
        marks_obtained = 0
        testcase = 'testcase_question_seven'
        expected_outcome = "Data insertion to population table."
        ref_if_failure_or_exception = "N/A"
        test_passed = False

        try:
            check_data_query = "SELECT COUNT(*) FROM ORDER_DETAILS"

            with self.expected_connection.cursor() as cursor:
                cursor.execute(check_data_query)
                row_count = cursor.fetchone()[0]  # Fetch the first column (count)

            # Check if data is available
                        
            if row_count > 1:
                test_passed = True

            
            if test_passed:
                logging.info("Expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object,
                    expected_outcome=expected_outcome,
                    actual_outcome='Data Insertion as expected.',
                    marks=marks,
                    marks_obtained=marks
                )
            else:
                logging.info("Expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object,
                    expected_outcome=expected_outcome,
                    actual_outcome='data Insertion is not as expected.',
                    reference=ref_if_failure_or_exception,
                    marks=marks
                )
        except Exception as e:
            logging.error("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_seven"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object,
                expected_outcome=expected_outcome,
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception,
                marks=marks
            )
     
    # Test case to validate revenue by category
    def testcase_question_eight(self, test_object):   
        testcase_description = Activity.test_case_descriptions[8]  
        test_object.update_pre_result(description=testcase_description)             
        expected_result, actual_result = None, None
        marks = 10
        marks_obtained = 0
        function = 'find_total_revenue_by_category'
        testcase = 'testcase_question_eight'
        expected_outcome = 'Revenue for each product category should be as expected.' 
        ref_if_failure_or_exception = "N/A"
        test_passed = False
               
        try:    
            if function in dir(solution_script):                
                expected_result = solution_script.find_total_revenue_by_category(self.expected_connection)    
                logging.info("expected_result from find_total_revenue_by_category method: {}".format(expected_result))            
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                                 
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3)
                try:
                    with contextlib.redirect_stdout(None):
                        actual_result = main_script.find_total_revenue_by_category(self.expected_connection)    
                    logging.info("actual_result from find_total_revenue_by_category method: {}".format(actual_result))                
                except Exception as exception:
                    return Activity.update_result_when_test_fails(
                        test_object=test_object,expected_outcome=expected_outcome, 
                        actual_outcome=Activity.error_occurred_during_function_execution.format(function, exception)
                    )
                finally:
                    signal.alarm(0)
            else:
                logging.error("{} not found in user solution".format(function))
                return Activity.update_result_when_test_fails(
                    test_object=test_object,expected_outcome=expected_outcome, 
                    actual_outcome=Activity.function_not_found.format(function)
                )
            
            if expected_result == actual_result:
                test_passed = True
            else:
                test_passed = False           


            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Total Revenue is as expected.', 
                    marks=marks, marks_obtained=marks
                )
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Total Revenue is not as expected.',
                    reference=ref_if_failure_or_exception, marks=marks
                )
                
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_eight"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object, expected_outcome=expected_outcome, 
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception, marks=marks
            )

import main_script, solution_script 
   
def start_tests(test_object):
    challenge_test = Activity()
    challenge_test.testcase_question_one(test_object)  
    challenge_test.testcase_question_two(test_object)  
    challenge_test.testcase_question_three(test_object)
    challenge_test.testcase_question_four(test_object)
    challenge_test.testcase_question_five(test_object)
    challenge_test.testcase_question_six(test_object)
    challenge_test.testcase_question_seven(test_object)
    challenge_test.testcase_question_eight(test_object)

    result = json.dumps(test_object.result_final())
    return test_object.result_final()
