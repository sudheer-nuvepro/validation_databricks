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
from pyspark.sql.functions import col, explode, desc, split, trim

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
            2 : " Verify removal of specified rows.",
            3 : " Verify data replacement in specified columns.",
            4 : " Verify filtering of data in the dataframe.",
            5 : " Verify Spark dataframe creation.",
            6 : " Verify splitting of genre column data.",
            7 : " Verify merging of dataframe.",
            8 : " Verify Snowflake cursor."
            }

    def __init__(self):
        # initialize the variables to be used across the testcases
        self.expected_titles_collection,self.expected_credits_collection = None, None
        self.actual_titles_collection,self.actual_credits_collection = None, None
        self.expected_titles_df,self.expected_credits_df = None, None
        self.actual_titles_df,self.actual_credits_df = None, None
        self.expected_titles_spark_df,self.expected_credits_spark_df = None, None
        self.actual_titles_spark_df,self.actual_credits_spark_df = None, None
        self.expected_titles_genres_separated_spark_df = None
        self.actual_titles_genres_separated_spark_df = None
        self.expected_transformed_data = None
        self.actual_transformed_data = None

    def get_databricks_http_path(workspace_url, databricks_token):
        databricks_host = workspace_url  # Directly use the workspace URL

        # Get SQL Warehouses from Databricks
        sql_endpoint_url = f"{databricks_host}/api/2.0/sql/warehouses"

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
        marks = 5
        marks_obtained = 0
        function = 'extract_data_from_mongodb'
        testcase = 'testcase_question_one'
        expected_outcome = 'Data should be extracted from given collection and converted into pandas dataframe.'
        ref_if_failure_or_exception = "TBD"
        test_passed = False
        
        try:  
            if function in dir(solution_script):
                with contextlib.redirect_stdout(None):
                    self.expected_titles_collection, self.expected_credits_collection = solution_script.create_Mongo_Resources()
                    self.expected_titles_df, self.expected_credits_df = solution_script.extract_data_from_mongodb(self.expected_titles_collection, self.expected_credits_collection )
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                      
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3) 
                try:
                    with contextlib.redirect_stdout(None):
                        self.actual_titles_collection, self.actual_credits_collection = main_script.create_Mongo_Resources()
                        self.actual_titles_df, self.actual_credits_df = main_script.extract_data_from_mongodb(self.actual_titles_collection, self.actual_credits_collection)
                
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
            try:
                # Use pandas assert_frame_equal to check equality
                pd.testing.assert_frame_equal(self.expected_titles_df, self.actual_titles_df)
                pd.testing.assert_frame_equal(self.expected_credits_df, self.actual_credits_df)
                test_passed = True  # Set to True if no exception is raised
            except AssertionError as e:
                test_passed = False  # Remain False if an exception is raised              
            
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

    # Test Case to removal of specified rows  
        
    # Test case to validate data replacement in imdb_score and tmdb_score columns
    def testcase_question_two(self, test_object):   
        testcase_description = Activity.test_case_descriptions[2]  
        test_object.update_pre_result(description=testcase_description)             
        expected_result, actual_result = None, None
        marks = 10
        marks_obtained = 0
        function = 'fill_mean_value'
        testcase = 'testcase_question_two'
        expected_outcome = 'Missing values in the specified columns should be replaced by their respective mean values.'
        ref_if_failure_or_exception = "N/A"
        test_passed = False
        
        try:    
            if function in dir(solution_script):                
                self.expected_titles_df = solution_script.fill_mean_value(self.expected_titles_df)                

            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                                 
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3)
                try:
                    with contextlib.redirect_stdout(None):
                        self.actual_titles_df = main_script.fill_mean_value(self.actual_titles_df)    
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

            imdb_match = False
            tmdb_match = False

            if self.actual_titles_df is not None:
                # Check if the values in the imdb_score and tmdb_score columns.
                if np.allclose(self.actual_titles_df['imdb_score'], self.expected_titles_df['imdb_score']):
                    imdb_match = True
                if np.allclose(self.actual_titles_df['tmdb_score'], self.expected_titles_df['tmdb_score']):
                    tmdb_match = True

            if imdb_match and tmdb_match:
                test_passed = True

            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Missing values are replaced as expected.', 
                    marks=marks, marks_obtained=marks
                )
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Missing values are not replaced as expected.',
                    reference=ref_if_failure_or_exception, marks=marks
                )
                
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_two"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object, expected_outcome=expected_outcome, 
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception, marks=marks
            )

    # Test case to validate data filtering
    def testcase_question_three(self, test_object):   
        testcase_description = Activity.test_case_descriptions[3]  
        test_object.update_pre_result(description=testcase_description)             
        expected_result, actual_result = None, None
        marks = 10
        marks_obtained = 0
        function = 'filter_df_score_morethan_8'
        testcase = 'testcase_question_three'
        expected_outcome = 'Only titles with imbd and tmdb score more than 8 should be present in the dataframe.'
        ref_if_failure_or_exception = "N/A"
        test_passed = False
        
        try:    
            if function in dir(solution_script):                
                self.expected_titles_df = solution_script.filter_df_score_morethan_8(self.expected_titles_df)    
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                                 
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3)
                try:
                    with contextlib.redirect_stdout(None):
                        self.actual_titles_df = main_script.filter_df_score_morethan_8(self.actual_titles_df)   
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
            logging.info(self.expected_titles_df)
            logging.info(self.actual_titles_df)
            try:
                # Use pandas assert_frame_equal to check equality
                pd.testing.assert_frame_equal(self.expected_titles_df, self.actual_titles_df)
                test_passed = True  # Set to True if no exception is raised
            except AssertionError as e:
                test_passed = False  # Remain False if an exception is raised  
            
            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Data from the titles dataframe is filtered as expected.', 
                    marks=marks, marks_obtained=marks
                )
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object, expected_outcome=expected_outcome, 
                    actual_outcome='Data from titles dataframe is not filtered as expected.',
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

    # Test case to validate spark dataframe creation
    def testcase_question_four(self, test_object):
        testcase_description = Activity.test_case_descriptions[4]
        test_object.update_pre_result(description=testcase_description)
        marks = 10
        marks_obtained = 0
        function = 'create_spark_dataframe'
        testcase = 'testcase_question_four'
        expected_outcome = "Spark Dataframe should be created from the given dataframe."
        ref_if_failure_or_exception = "N/A"
        test_passed = False

        try:
            # Validate function existence in solution script
            if function in dir(solution_script):
                self.expected_titles_spark_df,self.expected_credits_spark_df = solution_script.create_spark_dataframe(self.expected_titles_df,self.expected_credits_df)                
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
                        self.actual_titles_spark_df,self.actual_credits_spark_df = main_script.create_spark_dataframe(self.actual_titles_df,self.actual_credits_df)    
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

            if (self.actual_titles_spark_df is not None and self.actual_credits_spark_df is not None):
            #  compare both the datframes. if both credits and titles are equal the test is passed.
                titles_spark_df_equal = Activity.assert_spark_dataframes_equal(self.expected_titles_spark_df, self.actual_titles_spark_df)
                credits_spark_df_equal = Activity.assert_spark_dataframes_equal(self.expected_credits_spark_df, self.actual_credits_spark_df)
            else:
                titles_spark_df_equal = False
                credits_spark_df_equal = False

            test_passed = titles_spark_df_equal and credits_spark_df_equal
            
            if test_passed:
                logging.info("Expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(
                    test_object=test_object,
                    expected_outcome=expected_outcome,
                    actual_outcome='Spark dataframes are as expected.',
                    marks=marks,
                    marks_obtained=marks
                )
            else:
                logging.info("Expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(
                    test_object=test_object,
                    expected_outcome=expected_outcome,
                    actual_outcome='Spark dataframes are not as expected.',
                    reference=ref_if_failure_or_exception,
                    marks=marks
                )
        except Exception as e:
            logging.error("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_four"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object,
                expected_outcome=expected_outcome,
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception,
                marks=marks
            )

    #Test Case to validate splitting of genre column data
    def testcase_question_five(self, test_object):
        testcase_description = Activity.test_case_descriptions[6]  
        test_object.update_pre_result(description=testcase_description)             
        marks = 15
        marks_obtained = 0
        function = 'get_titles_genres_separated_spark_df'
        testcase = 'testcase_question_five'
        expected_outcome = 'Genre column should be expanded and separate rows should be created for each of the genre.'
        ref_if_failure_or_exception = "TBD"
        test_passed = False
        
        try:  
            if function in dir(solution_script):
                self.expected_titles_genres_separated_spark_df = solution_script.get_titles_genres_separated_spark_df(self.expected_titles_spark_df)
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                      
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3) 
                try:
                    with contextlib.redirect_stdout(None):
                        self.actual_titles_genres_separated_spark_df = main_script.get_titles_genres_separated_spark_df(self.actual_titles_spark_df)
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
            
            if (self.actual_titles_genres_separated_spark_df is not None and self.expected_titles_genres_separated_spark_df is not None):
                test_passed = Activity.assert_spark_dataframes_equal(self.actual_titles_genres_separated_spark_df, self.expected_titles_genres_separated_spark_df)
               
            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='Genre data is split as expected.',marks=marks, marks_obtained= marks)
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='Genre data is not split as expected.',
                                                reference=ref_if_failure_or_exception,marks=marks)             
        
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_five"] = str(e)
            return Activity.update_result_when_test_fails(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome=Activity.invalid_operation,
                                                reference=ref_if_failure_or_exception)
    
    # Testcase to validate merging of dataframes
    def testcase_question_six(self, test_object):
        testcase_description = Activity.test_case_descriptions[6] 
        test_object.update_pre_result(description=testcase_description)             
        marks = 10
        marks_obtained = 0
        function = 'get_transformed_data'
        testcase = 'testcase_question_six'
        expected_outcome = 'Dataframes must be merged as specified.'
        ref_if_failure_or_exception = "N/A"
        test_passed = False

        try:  
            if function in dir(solution_script):
                self.expected_transformed_data = solution_script.get_transformed_data(self.expected_titles_genres_separated_spark_df,self.expected_credits_spark_df)
            else:
                logging.error("{} not found in solution_script".format(function))
                return Activity.update_result_when_test_fails(test_object=test_object,
                    expected_outcome=expected_outcome, actual_outcome=Activity.error_in_solution_script)
                      
            if function in dir(main_script):
                signal.signal(signal.SIGALRM, Activity.alarm_handler)
                signal.alarm(3) 
                try:
                    with contextlib.redirect_stdout(None):
                        self.actual_transformed_data = main_script.get_transformed_data(self.actual_titles_genres_separated_spark_df,self.actual_credits_spark_df)
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
            
            if (self.actual_transformed_data is not None):
                test_passed = Activity.assert_spark_dataframes_equal(self.actual_transformed_data, self.expected_transformed_data)
               
            if test_passed:
                logging.info("expected matches actual for {}".format(testcase))
                return Activity.update_result_when_test_passes(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='Dataframes are merged as expected.',marks=marks, marks_obtained= marks)
            else:
                logging.info("expected DOES NOT match actual for {}".format(testcase))
                return Activity.update_result_when_test_fails(test_object=test_object, expected_outcome=expected_outcome, 
                                                actual_outcome='Dataframes are not merged as expected.',
                                                reference=ref_if_failure_or_exception,marks=marks) 
        
            
        except Exception as e:
            logging.info("{} : Error in validation {}".format(testcase, e))
            test_object.eval_message["testcase_question_six"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object, expected_outcome=expected_outcome, 
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception, marks=marks
            )

    def testcase_question_seven(self, test_object):   
        testcase_description = Activity.test_case_descriptions[7]  
        test_object.update_pre_result(description=testcase_description)             
        expected_result, actual_result,result = None, None,None
        marks = 5
        marks_obtained = 0
        testcase = 'testcase_question_seven'
        expected_outcome = 'Titlesnew table should be created'
        ref_if_failure_or_exception = "N/A"
        test_passed = False
        
        try:
            global expected_connection
            workspaceurl ,access_token = main_script.databricks_details()
            databricks_host, http_path = Activity.get_databricks_http_path(workspaceurl, access_token)          
            expected_connection  = Activity.get_databricks_connection(workspaceurl, http_path, access_token) 
            check_table_query = "SHOW TABLES LIKE 'Titlesnew'"

            with expected_connection.cursor() as cursor:
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
            test_object.eval_message["testcase_question_seven"] = str(e)
            return Activity.update_result_when_test_fails(
                test_object=test_object, expected_outcome=expected_outcome, 
                actual_outcome=Activity.error_occurred_during_function_execution.format(function, e),
                reference=ref_if_failure_or_exception, marks=marks
            )

    #Test Case to validate Snowflake cursor
    def testcase_question_eight(self, test_object):
        testcase_description = Activity.test_case_descriptions[8]
        test_object.update_pre_result(description=testcase_description)
        marks = 10
        marks_obtained = 0
        testcase = 'testcase_question_eight'
        expected_outcome = "Transformed data should be successfully loaded into the Titlesnew"
        ref_if_failure_or_exception = "N/A"
        test_passed = False

        try:
            check_data_query = "SELECT COUNT(*) FROM Titlesnew"

            with expected_connection.cursor() as cursor:
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
