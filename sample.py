########################################################################################################################
#Script Name: DPGCCEPH_Convert_JSON_To_CSV_HCP.py
#Developer Name: Kuldeeep  Tyagi
#DESCRIPTION: This script will convert the HCP JSON file into csv file with required attributes and logic for CODS Ruleset
#Version: 1.0 by TCS - Initial version
########################################################################################################################
import sys
import json
import csv
import time
import math

timestr = time.strftime("%Y%m%d%H%M%S")
# GCCEPH_INBOUND="/mkt/ephub/srcfiles"
GCCEPH_INBOUND="C:/Users/pramishr/Desktop/data"
#GCCEPH_LOG="/mkt/crml4/log"
json_file_path = GCCEPH_INBOUND + "/POC_HCP_INPUT_FILE_23.json"
f = open(json_file_path) 

output_type='json'
validCrossWalks= ["CODS","MDU","CTLN","MCRM","EMBS","DATAVISION","CMS","PBMD"]
#validCrossWalks= ["CODS"]
list_RequiredField_For_Single_Nested_Data=["HCPUniqueId","ActiveFlag","EffectiveStartDate","EffectiveEndDate","CountryCode","Name","FirstName","MiddleName","LastName","Gender","Source","OriginalSource"]
list_RequiredField_For_Double_Nested_Data=["RegulatoryAction","Email","Phone"]
list_RequiredField_From_CrossWalk = ["CrossWalkUri", "CrossWalkValue","CreateDate"]
list_RequiredField_Custom=["EntityId"]

list_RequiredField_For_VALUES=[]
list_SimpleModels = []
# returns JSON object as  
# a dictionary 

data = json.load(f) 
i = 0

class SimpleJsonStruct:
    crossWalkUri: str
    crossWalkValue: str
    completeAttributeUri : str
    nestedLevel: int
    trimmedAttributeUri: str
    createDate: str

    def __init__(self, completeAttributeUri, nestedLevel, trimmedAttributeUri, crossWalkUri, crossWalkValue, createDate):
        self.trimmedAttributeUri = trimmedAttributeUri
        self.completeAttributeUri = completeAttributeUri
        self.nestedLevel = nestedLevel
        self.crossWalkUri = crossWalkUri
        self.crossWalkValue = crossWalkValue
        self.createDate = createDate
 
    
def checkIfValidSource(sourceNameVal):
    for validsource in validCrossWalks:
        if validsource in sourceNameVal:
            return True
    return False

def getNestedLevel(attribute):
    return math.ceil(attribute.count('/') /2) 

def parseRecursive(keyModel, valueModels, secondLevelKey):
    for valueModel in valueModels:  
        if valueModel["uri"] in keyModel.completeAttributeUri:
            obj = valueModel["value"]
            if(secondLevelKey in obj):
                valueDataModel = obj[secondLevelKey]
                for value in valueDataModel:  
                    if value["uri"] == keyModel.completeAttributeUri:
                        obj = value["value"]
                        return obj
            else:
                return obj
    return None 

def recursiveParser(keyModel, valueModels, currentKey, currentLevel):
    if currentLevel == 1 and currentKey in list_RequiredField_For_Single_Nested_Data:
        return parseRecursive(keyModel, valueModels, 'InvalidKey')
    
    if currentLevel == 2 and currentKey in list_RequiredField_For_Double_Nested_Data:
        
        if currentKey == 'Email':
            return parseRecursive(keyModel, valueModels, 'Email') 
        
        elif currentKey == 'RegulatoryAction':
            return parseRecursive(keyModel, valueModels, 'Flag')  
        
        elif currentKey == 'Phone':
            return parseRecursive(keyModel, valueModels, 'Number')   

        else:
            return None
         
    return None 
    

def parseAndPopulateData(i, fileName, crossWalkPath):
    crossWalkUri = crossWalkPath['uri']
    crossWalkValue = crossWalkPath['value']
    createDate = crossWalkPath['createDate']

    finalData = {}
    flag = False
    for model in list_SimpleModels:
        keyForUri = model.trimmedAttributeUri[0: model.trimmedAttributeUri.index("/")]
        attributeModel = data["attributes"]

        if keyForUri in attributeModel:
            valueModels = attributeModel[keyForUri]
            objValue = recursiveParser(model, valueModels, keyForUri, model.nestedLevel)
            if objValue: 
                flag=True 
                finalData[keyForUri]= (str(objValue))
 
    #Add CrossWalk Related Keys
    for dataVal in list_RequiredField_From_CrossWalk:
        if dataVal == 'CrossWalkUri':
            finalData['CrossWalk'] = crossWalkUri

        elif dataVal == 'CrossWalkValue':
            finalData['CrossWalk Value'] = crossWalkValue

        elif dataVal == 'CreateDate':
            finalData['CreateDate'] = createDate

    #Add EntityId
    uri = data['uri']
    entityIdStartIndex = len('entities/')
    entityId = uri[entityIdStartIndex:]
    finalData['EntityId'] = entityId

    if(finalData and flag):
        with open(fileName,'w') as outfile:
            json.dump(finalData, outfile)


for crossWalkPath in data['crosswalks']: 
    i = i + 1
    if "sourceTable" in crossWalkPath:
        if(checkIfValidSource(crossWalkPath["sourceTable"])):
            fileName= crossWalkPath["sourceTable"] + "_" + crossWalkPath["value"] + "." + output_type
            if "attributes" in crossWalkPath and bool(crossWalkPath["attributes"]):
                for attribute in crossWalkPath['attributes']:  
                    key = '/attributes/'
                    attributeStartKeyIndex = attribute.index(key)
                    attribKey = attribute[attributeStartKeyIndex + len(key) :]
                    obj = SimpleJsonStruct(attribute, getNestedLevel(attribKey), attribKey, crossWalkPath['uri'], crossWalkPath['value'], crossWalkPath['createDate'])
                    list_SimpleModels.append(obj)
                parseAndPopulateData(i, fileName, crossWalkPath)
                list_SimpleModels.clear()      
            else:
                print("Warning: Attributes Missing for with URI " + crossWalkPath["uri"] + " with sourceTable: " + crossWalkPath["sourceTable"] + ". Hence Ignored during Processing.")    
        else:
            print("Warning: Excluding from Source . Not defined as valid source - " + crossWalkPath["sourceTable"])
    else:
        print("Warning: CrossWalk with URI " + crossWalkPath["uri"] + " has no sourceTable Attribute. Hence Ignored during processing.")
            






# Closing file 
f.close() 
      
