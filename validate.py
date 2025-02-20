#!/bin/python
import os
import sys
import time
import json

# move into the validation directory
if os.path.exists("/tmp/scoring"):
    os.chdir("/tmp/scoring")
elif os.path.exists("/tmp/clv"):
    os.chdir("/tmp/clv")

SUCCESS_STATUS = "success"
FAILURE_STATUS = "failure"

def remove_the_configuration_repository(config_repository):
    try:
        os.system("sudo rm -rf {}".format(config_repository))
    except:
        pass
        
def clone_configuration_repository():    
    clone_config_repo_status = os.system("git clone https://rishi-nuvepro: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@github.com/Nuvepro-Technologies-Pvt-Ltd/Python-Common-Validation_Configuration.git 2> /dev/null")
    
    # os.system returns 0 if the operation is successful, else raise an Exception
    if clone_config_repo_status != 0:
        raise Exception("Validation Configuration not cloned")
    
    # if os.system returns 0, then report success
    return SUCCESS_STATUS
    
def retry_clone_validation_config_with_timeout(max_retries, timeout_seconds):   
    retries = 0
    start_time = time.time()

    while retries < max_retries:
        try:
            # if it is a success, return SUCCESS_STATUS
            return clone_configuration_repository()
        except Exception as e:

            # if the clone operation fails, the exception thrown will be caught here and retried
            retries += 1

            # Introduce a delay before the next clone attempt
            time.sleep(5)  

            # Check if the timeout has been exceeded
            current_time = time.time()
            if current_time - start_time >= timeout_seconds:
                break
                
    print("Unable to clone the Validation Configuration Repository")
    return FAILURE_STATUS
    
def setup_validation_config_files(token):    
    
    main_file_path = "/home/labuser/Desktop/Project/Assessment.ipynb"
    solution_file_path = "Solution.ipynb" # path to the solution; relative to the path /tmp/clv
    
    validation_command = "sudo python3 validation.py {} {} {}".format(token, main_file_path, solution_file_path) # pass the token, file paths to validation.py
    validation_result = os.system(validation_command)


# pass the token sent from moodle
setup_validation_config_files(sys.argv[1])

# remove the solution folder after validation
# os.chdir("/tmp")
# remove_the_configuration_repository("clv")

