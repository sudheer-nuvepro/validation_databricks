import json,os,time,logging

def log(loglevel, logstr):
   print(logstr)

class ResultOutput:
    counter = 0
    output = ""
    testcases = []
    summary= {
        "totalTests" : 0,
        "Passed" : 0,
        "Failed" : 0,
        "Errored" : 0,
        "eval" : 1
    }
    eval_message = {}

    def __init__(self, args, class_object):
        # logging.info("step 1")
        # print("entered init block")
        testcase_list = []
        time.sleep(1)
        try:
            #logging.info("step 2")
            logging.info("Opening file resultTemplate.json")
            result_resource = open(str(os.path.dirname(os.path.realpath(__file__)).replace('\\','/'))+"/resultTemplate.json")
            logging.info("opening file resultTemplate.json complete")
            logging.info("loading contents of resultTemplate.json")
            self.output=json.load(result_resource)
            #logging.info("step 3")
            logging.info("loaded contents of resultTemplate.json")
            result_resource.close()
            logging.info("closed file resultTemplate.json")
            #logging.info("step 4")
        except Exception as e:
            logging.info(str(e))   

        # try:
        #     json.loads(args)
        # except Exception as e:
        #     logging.info(str(e))
        #     logging.info("Malformed json input arguments")            
        
        try: 
            if "token" in json.loads(args).keys():
            #logging.info("step 5")
                json.loads(args).keys()
                self.output["context"]["token"] = json.loads(args)['token']
                #logging.info("step 6")
            else:
            #logging.info("step 7")
                self.output["context"]["args"] = json.loads(args)
                #logging.info("step 8")
        except Exception as e:
            logging.info(str(e))
            logging.info("Malformed json input arguments") 

        method_list = [attribute for attribute in dir(class_object) if callable(getattr(class_object, attribute)) and attribute.startswith('testcase') is True]
        index = 0
        #print(method_list)
        #logging.info("step 9")
        for testcase_method in method_list:
            template={"index":0,
            "testCase": "",
            "expected": "",
            "actual": "",
            "status": "",
            "comments": "",
            "ref": "",
            "marks": "",
            "marksObtained" : ""
            }
            
            template["index"] = index
            template["testCase"] = "{{"+str(testcase_method)+"_description"+"}}"
            template["expected"] = "{{"+str(testcase_method)+"_expected"+"}}"
            template["actual"] = "{{"+str(testcase_method)+"_actual"+"}}"
            template["status"] = 0
            template["comments"] = "{{"+str(testcase_method)+"_comments"+"}}"
            template["ref"] = "{{" + str(testcase_method)+"_ref"+"}}"
            template["marks"] = "{{" + str(testcase_method)+"_marks"+"}}"
            template["marksObtained"] = "{{" + str(testcase_method)+"_marks_obtained"+"}}"
            testcase_list.append(template)
            index += 1

        self.testcases = testcase_list
        #logging.info("step 11")

    def update_pre_result(self, description="", expected=""):
        self.testcases[self.counter]["testCase"] = description
        self.testcases[self.counter]["expected"] = expected

    def update_result(self, result, expected=" ", actual=" ", comment="", ref="", marks="", marks_obtained=""):
        if actual != "":
            self.testcases[self.counter]["expected"] = expected 
        if comment != "":
            self.testcases[self.counter]["comments"] = comment
        if actual != "":
            self.testcases[self.counter]["actual"] = actual 
        if ref != "":
            self.testcases[self.counter]["ref"] = ref
        
        self.testcases[self.counter]["marks"] = marks        
        self.testcases[self.counter]["marksObtained"] = marks_obtained
        self.testcases[self.counter]["status"] = result
        if result == 1:
            self.summary["Passed"] += 1
        elif result == 0:
            self.summary["Failed"] += 1
        elif result == -1:
            self.summary["Errored"] += 1
            self.summary["eval"] = 0

        self.summary["totalTests"] += 1
        self.counter += 1 
        return 

    def result_final(self):
        self.output["testCases"] = self.testcases
        self.output["summary"]["totalTests"] = self.summary["totalTests"]
        self.output["summary"]["Passed"] = self.summary["Passed"]
        self.output["summary"]["Failed"] = self.summary["Failed"]
        self.output["summary"]["Errored"] = self.summary["Errored"]
        self.output["evaluation"]["status"] = self.summary["eval"]
        self.output["evaluation"]["message"] = self.eval_message
        return json.dumps(self.output, indent=1)
