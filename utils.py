#!/bin/python
import json
import os
import astunparse
from result_output import ResultOutput
import importlib.util
import sys
import inspect
from inspect import getmembers, isfunction
import signal
import logging 
import shutil
from os.path import dirname, abspath
import ast
import tokenize

import nbformat as nbf
from nbconvert.exporters import PythonExporter
from nbconvert.preprocessors import TagRemovePreprocessor
from os.path import exists

# configure the logging module, can be used for debugging
# os.chdir("/home/labuser/")
# logging.basicConfig(filename = 'validate_script.log', level = logging.INFO, filemode = 'w', format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
if os.path.exists("/tmp/scoring"):
    os.chdir("/tmp/scoring")
elif os.path.exists("/tmp/clv"):
    os.chdir("/tmp/clv")

def set_path_configuration():
    """ Include the parent directory in sys.path
    Remove __pycache__ before each evaluation run, since the user solution might be updated,
    code must be compiled each time instead of indexing the cached modules
    """
    path_to_parent_and_all_directories = dirname(dirname(abspath(__file__)))
    sys.path.append(path_to_parent_and_all_directories)
    # need to remove the cached modules, since the latest written functions are to be considered

class Preprocess:
    """ An object of Preprocess is instantiated with a file path and name. 
    The file located at path (can be an ipynb or py file) is preprocessed and stored in the Validation folder
    
    ...

    Attributes
    ----------

    path : str
        Location of an .ipynb or .py file
    file_name : str
        Name of the file that stores the result of the Preprocessing in the current working directory
    
    Methods
    -------
    reload_main_and_solution_module_imports()
        Reload the modules main_script and solution_script 

    clear_main_and_solution_modules()
        Clear the modules main_script and solution_script, i.e truncate file length to 0

    process_py_file() 
        Copy a .py file located at self.path to a .py file in the current directory with name self.file_name
    
    process_ipynb_file()
        Convert a .ipynb file located at self.path to a .py file in the current directory with name self.file_name
    
    comment_print_statement(path)
        Comment all print statements in file located at path

    pre_process()
        Preprocess the file located at self.path

    """

    def __init__(self, path, file_name):
        self.path = path
        self.file_name = file_name
    
    
    @staticmethod 
    def reload_main_and_solution_module_imports():
        """ Reload a file so the latest changes or methods/classes added can be indexed
        After the Preprocessing is complete, if there is no error, the module must be reloaded
        """
        
        importlib.reload(sys.modules["solution_script"])
        importlib.reload(sys.modules["main_script"])
        logging.info("Reloaded the main and solution modules")

    
    @staticmethod 
    def clear_main_and_solution_modules():
        """ At the start and end of each evaluation, clear the main and solution scripts """

        with open("main_script.py",'r+') as main_script_handler:
            main_script_handler.truncate(0)
        with open("solution_script.py",'r+') as main_script_handler:
            main_script_handler.truncate(0)
        logging.info("Cleared the main and solution modules")

    
    def process_py_file(self, python_script_path):
        """ Copy the contents of a .py file located at self.path to a file
        in the current working directory with name self.file_name
        """

        try:
            with open(self.path, encoding='utf-8') as py_file_handler:
                python_script = py_file_handler.read()
                with open(python_script_path, 'w', encoding='utf-8') as py_copy_file_handler:
                    py_copy_file_handler.writelines(python_script)
                    return {"status" : 1, "python_script_path" : python_script_path}
        except Exception as exception:
            return {"status" : 0, "message" : "An exception occurred : {}".format(exception)}

    def process_ipynb_file(self, python_script_path): 
        """ Read the convert the contents of a jupyter notebook (.ipynb file) present at self.path into 
        a .py file in the current working directory with name self.file_name
        """

        try:
            with open(self.path, encoding='utf-8') as ipynb_file_handler:
                notebook_nodes = nbf.read(ipynb_file_handler, as_version = 4)
                tag_remove_processor = TagRemovePreprocessor()
                tag_remove_processor.remove_cell_tags = ("remove", )
                python_exporter = PythonExporter()
                python_exporter.register_preprocessor(tag_remove_processor, enabled= True)
                python_script, meta = python_exporter.from_notebook_node(notebook_nodes)
                with open(python_script_path, 'w', encoding='utf-8') as py_file_handler:
                    py_file_handler.writelines(python_script)
                return {"status" : 1, "python_script_path" : python_script_path}
        except Exception as exception:
            return {"status" : 0, "message" : "An exception occurred : {}".format(exception)}

    @staticmethod
    def comment_print_statement(path):
        with open(path, 'r+') as file_handler:
            content = file_handler.read()
            file_handler.seek(0)
            file_handler.truncate()
            file_handler.write(content.replace('print', '#print'))
        
    def pre_process(self):
        """ Preprocessing steps are : 

        1. Copy the contents of a .ipynb/.py file located at self.path to a file in the current working directory
         with the name self.file_name
        2. Use static code analysis to extract only the imports, functions and classes.
        Statements that instantiate an object, invoke a function etc are dropped to avoid any unexpected behavior
        3. If successful extraction of imports, functions/classes, comment all print statements

        Status 0 indicates failure, while 1 indicates success
        """
        
        response_pre_process = {'status' : 0}
        python_script_path = os.getcwd() + "/" + self.file_name + ".py"
        
        if self.path.endswith("ipynb"):
            response_pre_process = self.process_ipynb_file(python_script_path)
        else:
            response_pre_process = self.process_py_file(python_script_path)
        
        if response_pre_process.get('status') == 0:
            logging.error("Unable to process file {} due to {}".format(self.path, response_pre_process.get('message')))
            return response_pre_process

        """ If preprocessing was successful, assign the absolute to the file created in the current directory """
        copied_file_path = response_pre_process['python_script_path']
        
        # extract only functions and imports from the file copied using static code analysis
        extract_obj = ExtractImportsClassesAndFunctionsOnly(copied_file_path)

        response_extract_functions_classes = extract_obj.extract_imports_functions_and_classes()
        if not response_extract_functions_classes \
            or response_extract_functions_classes and response_extract_functions_classes.get('status') == 0:
            logging.error("Unable to extract functions/classes and imports from {} due to {}".format(copied_file_path , response_extract_functions_classes.get("message")))
            return response_extract_functions_classes
        
        # to avoid unexpected behavior, comment all print statements
        # Preprocess.comment_print_statement(copied_file_path)
       
        return response_extract_functions_classes
    
class ExtractImportsClassesAndFunctionsOnly:

    """ Parse a .py file and write back only the imports, functions/classes into the file.
    Drop function invocations, object instantiations etc, which might lead to unexpected behavior
    Report status 0 on failure, 1 on success with the appropriate error

    Attributes
    ----------
    
    file_path : str
        .py file from which imports, functions and classes are to be extracted and copied into
    
    Methods
    -------

    parse_file()
        Parse a file and return the parsed object
        
    extract_imports_functions_and_classes()
        Parse the file, walk through the nodes of the AST, and only the source code
        corresponding to imports, functions and classes back into the file
    """

    def __init__(self, file_path):
        self.file_path = file_path

    def parse_file(self):
        try:
            # read a file based on the encoding - https://docs.python.org/3/library/tokenize.html#tokenize.open
            with tokenize.open(self.file_path) as file_reader:
                return ast.parse(file_reader.read(), filename=self.file_path)   
        except Exception as exception:
            return ""

    def extract_imports_functions_and_classes(self):
        """ Scan the nodes of the Abstract Syntax Tree (AST) and retain
        only imports, classes and functions in the file

        Return : Status of extracting the imports and classes, 0 for failure, 1 for success 
        along with the message
        """

        tree = self.parse_file()
        if tree != "":
            try:
                # extract only the functions/classes and the imports, hence, open the file in write mode
                with open(self.file_path, "w") as file_handler: 
                    for node in tree.body:
                        if isinstance(node, ast.ClassDef) or isinstance(node, ast.FunctionDef) or isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
                            """" unparse is used to get the source code corresponding to an AST node
                                Reference : https://astunparse.readthedocs.io/en/latest/
                            """

                            file_handler.write(astunparse.unparse(node))
                return {'status' : 1, 'message' : "Success in extracting imports, classes/functions"}
            except Exception as exception:
                logging.error("An exception occurred as : {}".format(exception))
                return {'status' : 0, 'message' : "An exception occurred as : {}".format(exception)}
        else:
            logging.error("Tree is empty")
            return {'status' : 0, 'message' : "file cannot be parsed"}