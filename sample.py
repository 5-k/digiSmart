  
########################################################################################################################
#Script Name: data_de__profiler_source_simplefied_json.py
#Developer Name: Aditya Singh
#DESCRIPTION: This script converts Nested Json Data from source files and converts them to a target JSON file. 
    #The attributes to be picked up at single nested level and nested level are defined in array below. Add more attributes if you want to get more columns from source
    #The attributes to be picked from CrossWalks are well defined as well
#Version: 1.0 by TCS - Initial version
########################################################################################################################

#Defining Imports
import json
import math
import os
import boto3
import time 

#There are the prefixes of valid sources we want to read from input json. 
#validCrossWalks= ["CODS","MDU","CTLN"]
validCrossWalks= ["CODS","MDU","CTLN","MCRM","EMBS","DATAVISION","CMS","PBMD"]

#Required Fields in Output Json
list_RequiredField_For_Single_Nested_Data=["HCPUniqueId","ActiveFlag","EffectiveStartDate","EffectiveEndDate","CountryCode","Name","FirstName","MiddleName","LastName","Gender","Source","OriginalSource"]
list_RequiredField_For_Double_Nested_Data=["RegulatoryAction","Email","Phone"]
list_RequiredField_From_CrossWalk = ["CrossWalkUri", "CrossWalkValue","CreateDate"]
list_RequiredField_Custom=["EntityId"]
list_SimpleModels = []
outputFileName = ""
logFileName = ""
logDataToFile = True

class SimpleJsonStruct:
    #Struct To Hold relevant Information for Each crosswalk in each Entity
    completeAttributeUri : str
    nestedLevel: int
    trimmedAttributeUri: str

    def __init__(self, completeAttributeUri, nestedLevel, trimmedAttributeUri):
        self.trimmedAttributeUri = trimmedAttributeUri
        self.completeAttributeUri = completeAttributeUri
        self.nestedLevel = nestedLevel

def log(logData):
    if(logDataToFile):
        with open(logFileName,'a') as outfile:
            outfile.write(logData) 
            outfile.write("\n")
    else:
        print(logData)

def checkIfValidSource(sourceNameVal):
    #Function To Check if the attribute Value Is present In Valid Source List Defined above    
    for validsource in validCrossWalks:
        if validsource in sourceNameVal:
            return True
    return False

def getNestedLevel(attribute):
    #Returns the nested Level for each Attribute
    return math.ceil(attribute.count('/') /2) 

def secondLevelParser(keyModel, valueModels, secondLevelKey, keyToFetchForFirstLevel, keyToFetchForSecondLevel):
    #Function Defined for Recursive Parsing. Currently Customized for 2 Levels but TODO: Generic Formatting
    #The FirstLevelKey and SecondLevelKey are always 'Value' But we are passing this from another function so that 
    #in custom cases where need something else like lookupcode in CC, we can do that as well
    for valueModel in valueModels:  
        if valueModel["uri"] in keyModel.completeAttributeUri:
            obj = valueModel[keyToFetchForFirstLevel]
            if(secondLevelKey in obj):
                valueDataModel = obj[secondLevelKey]
                for value in valueDataModel:  
                    if value["uri"] == keyModel.completeAttributeUri:
                        obj = value[keyToFetchForSecondLevel]
                        return obj
            else:
                return obj
    return None 

def recursiveParser(keyModel, valueModels, currentKey, currentLevel, keyToFetch):
    #This is the main Parser which is called for each KeyModel. 
    # KeyModel - An Object from the custom defined class which contains all attributes for one entity for one crosswalk
    # ValueModels - List Of Value for a specific Key
    # CurrentKey - KeyName for which we have values in ValuesModels
    # CurrentLevel - Nested Level of the Entity
    # keyToFetch - This is always usually 'value' because that is what we want, but for custom code logic we may pass this different
    if currentLevel == 1 and currentKey in list_RequiredField_For_Single_Nested_Data:
        for valueModel in valueModels:
            if valueModel["uri"] == keyModel.completeAttributeUri:
                obj = valueModel[keyToFetch]
                
                #Custom Mapping for Specific Attributes

                return obj
    
    elif currentLevel == 2 and currentKey in list_RequiredField_For_Double_Nested_Data:
        
        if currentKey == 'Email':
            return secondLevelParser(keyModel, valueModels, 'Email', keyToFetch, keyToFetch) 
        
        elif currentKey == 'RegulatoryAction':
            return secondLevelParser(keyModel, valueModels, 'Flag', keyToFetch, keyToFetch)  
        
        elif currentKey == 'Phone':
            return secondLevelParser(keyModel, valueModels, 'Number', keyToFetch, keyToFetch)   

        else:
            return None
    elif currentLevel > 2:
        log("Error: Found Level more than 2 attrbute. Unhandled Condition, Attribute Skipped for key: " + currentKey)
        log(currentLevel)
 
         
    return None 
    
def parseAndPopulateData(data, crossWalkPath, output_data):
    #This function takes in the entire entity data and crosswalk(from the same enitityData)
    # Parses Each Objects created as per data struct and then Evaluates the Value for them to add to final Array
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

            #CustomCode For Adding Extra Entity
            if(keyForUri == 'CountryCode'):
                customObjValue = recursiveParser(model, valueModels, keyForUri, model.nestedLevel, 'lookupCode')
                finalData['CountryCodeValue']= (str(customObjValue))

            objValue = recursiveParser(model, valueModels, keyForUri, model.nestedLevel, 'value')
            if objValue: 
                flag=True 
                finalData[keyForUri]= (str(objValue))
 
    #Custom Fields - Add CrossWalk Related Keys
    for dataVal in list_RequiredField_From_CrossWalk:
        if dataVal == 'CrossWalkUri':
            finalData['CrossWalk'] = crossWalkUri

        elif dataVal == 'CrossWalkValue':
            finalData['CrossWalkValue'] = crossWalkValue

        elif dataVal == 'CreateDate':
            finalData['CreateDate'] = createDate

    #Custom Fields - Add EntityId
    uri = data['uri']
    entityIdStartIndex = len('entities/')
    entityId = uri[entityIdStartIndex:]
    finalData['EntityId'] = entityId

    #Only If We have Data, add to Json
    if(finalData and flag):
        output_data.append(finalData)
    
def parseEntity(entityData, output_data):
    #For each element in EntityArray, We find the SourceTable , CrossWalks And Parses them to 
    # identify AttributeKey, NestedLevels, URI to match and creates an Object for each entity
    # Each such object is added to an array for later to be evaluated
    for crossWalkPath in entityData['crosswalks']:  
        if "sourceTable" in crossWalkPath:
            if(checkIfValidSource(crossWalkPath["sourceTable"])):
                
                if "attributes" in crossWalkPath and bool(crossWalkPath["attributes"]):
                    
                    for attribute in crossWalkPath['attributes']:
                        key = '/attributes/'
                        attributeStartKeyIndex = attribute.index(key)
                        attribKey = attribute[attributeStartKeyIndex + len(key) :]
                        obj = SimpleJsonStruct(attribute, getNestedLevel(attribKey), attribKey)
                        list_SimpleModels.append(obj)
                    parseAndPopulateData(entityData, crossWalkPath, output_data)
                    list_SimpleModels.clear()
                else:
                    log("Warning: Attributes Missing for with URI " + crossWalkPath["uri"] + " with sourceTable: " + crossWalkPath["sourceTable"] + ". Hence Ignored during Processing.")    
            else:
                log("Information: Excluding from Source . Not defined as valid source - " + crossWalkPath["sourceTable"])
        else:
            log("Warning: CrossWalk with URI " + crossWalkPath["uri"] + " has no sourceTable Attribute. Hence Ignored during processing.")

def deleteFileIfExists(fileNameToDelete):
    try:    
        os.remove(fileNameToDelete)
    except OSError:
        pass 

def main():
    s3 = boto3.client('s3')
    #Source Json File Path Defined Here
    file = s3.get_object(Bucket='lly-future-state-arch-poc-dev', Key='input_data/HCP_Traverse_3Load.json')
    f = open(file, encoding = "UTF-8") 
    lines = f.read()
    #lines = file['Body'].read().decode('utf-8')
    
    #The source file is assumed to be an array of json but the format does not enclose in array format
    #Hence PrePending and Pospending array brackets to convert to valid Json
    new_lines = "[" + lines + "]"
    
    #Loading Data to Memory as Json
    allData = json.loads(new_lines)

    POC_INBOUND = 's3://lly-future-state-arch-poc-dev/input_data'
    output_type='json'
    ts = time.time() 
    logFileName = POC_INBOUND + "/logFile_" + ts + "." + "txt"
    
    #Post Processing, Dump all data to json
    outputFileName= POC_INBOUND + "/data_de_profiled_simplefied_json" + "." + output_type
    
    #Delete File If Exists
    deleteFileIfExists(logFileName)
    deleteFileIfExists(outputFileName)

    #Simple ListOf Struct Data structure to hold information on crossWalks for Each Entity
    list_SimpleModels = []
    
    #Initializing Output Array Empty
    output_data = []

    #Application Main
    # Load All Data and for eachData Run the Function
    for entity in allData:
        parseEntity(entity, output_data) 

    try:
        os.remove(outputFileName)
    except OSError:
        pass

    with open(outputFileName,'a') as outfile:
        for data in output_data:
            json.dump(data, outfile)
            outfile.write("\n")
            #outfile.write(data)a
            #json.dump(output_data, outfile)
        list_SimpleModels.clear()  

    # Closing file 
    f.close()

if __name__ == "__main__":
    main()
