#!/bin/python
import json
from test_cases import Activity, start_tests
import sys 
from utils import set_path_configuration, Preprocess 
import importlib
from result_output import ResultOutput
import logging 
import signal
import os
import astunparse

# configure the logging module, can be used for debugging, a log file named validate_script.log will be created in the path /home/labuser

# os.chdir("/home/labuser/")
# logging.basicConfig(filename = 'validate_script.log', level = logging.INFO, filemode = 'w', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

# change to the validation directory
if os.path.exists("/tmp/scoring"):
    os.chdir("/tmp/scoring")
elif os.path.exists("/tmp/clv"):
    os.chdir("/tmp/clv")

def main():
    
    token = sys.argv[1]
    token = token.replace('{','')
    token = token.replace('}','')
    token = token.split(':')
    token = {"token":token[1]}
    token = json.dumps(token) 

    main_path, main_file_name = sys.argv[2], "main_script"
    solution_path, solution_file_name = sys.argv[3], "solution_script"

    if "result_output" not in sys.modules:
        importlib.import_module("result_output")
    else:   
        importlib.reload(sys.modules["result_output"])

    # Object to store the result of the test case evaluation
    test_object = ResultOutput(token, Activity)
    
    # check if the main file exists, else return     
    if not os.path.isfile(main_path):
        logging.error("Main script does not exist, check script path")

        # always clear the main and solution script in the case of an error
        # Ashwini Commenting
        # Preprocess.clear_main_and_solution_modules()

        """ In the case that the main script, i.e code written by the user is not found, 
        report the absence of the user's solution """
        Activity.report_error_before_validating_tests(test_object, Activity.main_script_not_found)
        return test_object.result_final()

    # check if the solution file exists, else return     
    if not os.path.isfile(solution_path):
        logging.error("Solution script does not exist, check script path")

        # always clear the main and solution script in the case of an error
        # Ashwini Commenting
        # Preprocess.clear_main_and_solution_modules()

        """ In the case that the solution script, i.e solution written by us/provided
         does not exist, report that an internal error has occurred before validation """
        Activity.report_error_before_validating_tests(test_object, Activity.solution_script_not_found)
        return test_object.result_final()

    # Always clear main_script and solution_script and write the latest updates after Preprocessing
    Preprocess.clear_main_and_solution_modules()

    # Preprocess the main script
    main = Preprocess(main_path, main_file_name)
    main_pre_process_response = main.pre_process()
    if main_pre_process_response['status'] == 0:
        logging.error("Main script is not ready for validation, exiting")

        # always clear the main and solution script in the case of an error
        # Ashwini Commenting
        # Preprocess.clear_main_and_solution_modules()

        """ In the case that the main script cannot be preprocessed, i.e code written by the user cannot be parsed, 
        report that all test cases have failed due to a syntax error """
        
        Activity.report_error_before_validating_tests(test_object, Activity.syntax_error_in_main_script)
        return test_object.result_final()

    # Preprocess solution
    solution = Preprocess(solution_path, solution_file_name)
    solution_pre_process_response = solution.pre_process()
    if solution_pre_process_response['status'] == 0:
        logging.error("Solution script is not ready for validation, exiting")

        # always clear the main and solution script in the case of an error
        # Ashwini Commenting
        # Preprocess.clear_main_and_solution_modules()    
        
        """ In the case that the solution script cannot be preprocessed, i.e solution code cannot be parsed, should ideally NOT
        happen, however, checked as a precaution, report that an internal server error has occurred during validation """
        
        Activity.report_error_before_validating_tests(test_object, Activity.syntax_error_in_solution_script)
        return test_object.result_final()

    """ Reload main and solution after the preprocessing is complete, return if there is a timeout
    After writing only functions, classes and imports to main_script and solution_script, it is crucial is reload the modules, to 
    ensure that the contents of main_script and solution_script are accessible in test_cases.py
    Though the required constructs are copied, they will be accessible only after reloading
    """

    try:
        Preprocess.reload_main_and_solution_module_imports()
    except Exception as exception:
        logging.error("Module taking too long to reload, exiting {}".format(exception))
        # always clear the main and solution script in the case of an error
        # Ashwini Commenting
        # Preprocess.clear_main_and_solution_modules()    
        Activity.report_error_before_validating_tests(test_object, 
                "Validation not performed as {}".format(Activity.unable_to_reload_modules))
        return test_object.result_final()

    logging.info("Starting the test validation")
    start_tests(test_object)

    # clear main and solution once validation is complete
    # Ashwini Commenting
    # Preprocess.clear_main_and_solution_modules()   
    return test_object.result_final()
    
if __name__== "__main__" :
    # in VM labs, the result must be printed; this is passed to moodle
    print(main())

   