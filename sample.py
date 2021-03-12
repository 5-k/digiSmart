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
#import boto3 
import collections
from datetime import datetime

class SimpleJsonStruct:
    #Struct To Hold relevant Information for Each crosswalk in each Entity
    key: str
    completeAttributeUri : str
    nestedLevel: int
    trimmedAttributeUri: str
    
    def __init__(self, key, completeAttributeUri, nestedLevel, trimmedAttributeUri):
        self.key = key
        self.trimmedAttributeUri = trimmedAttributeUri
        self.completeAttributeUri = completeAttributeUri
        self.nestedLevel = nestedLevel

class DataJsonFlatten:
    #There are the prefixes of valid sources we want to read from input json. 
    #validCrossWalks= ["CODS"]
    validCrossWalks= ["CODS","MDU","CTLN","MCRM","EMBS","DATAVISION","CMS","PBMD"]
    fileVal = None
    outLogfile = None

    logFileName = "" 
    dictionary_SimpleModels = None

    output_data = [] 
    output_type='json'  
    #Required Fields in Output Json
    list_RequiredField_For_Single_Nested_Data=["HCPUniqueId","ActiveFlag","EffectiveStartDate","EffectiveEndDate","CountryCode","Name","FirstName","MiddleName","LastName","Gender","Source","OriginalSource"]
    list_RequiredField_For_Double_Nested_Data=["RegulatoryAction","Email","Phone"]
    list_RequiredField_From_CrossWalk = ["CrossWalkUri", "CrossWalkValue","CreateDate"]
    list_RequiredField_Custom=["EntityId"]

    #Env Run Variables
    log_data_to_file = True
    is_local_run = True
    print_to_console = True

    fileName = ""
    flePath = ""
    POC_INBOUND = ""
    outputFileName = ""
    
    def __init__(self):
        print("Information: Initialized Script")

    def initDictionary(self): 
        self.dictionary_SimpleModels = {}

    def get_myDictionary(self, dictionary, key):
        return dictionary[key]

    def set_MyDictionary(self, dictionary, key, value):
        values = []
        
        if key in dictionary:
            values = dictionary[key]  
        
        values.append(value)
        dictionary[key] = values
        return dictionary

    def getNowTimestampormatted(self): 
        now = datetime.now()
        timestampStr = now.strftime('%Y-%m-%dT%H-%M-%S') 
        return timestampStr

    def setFileInputDirectory(self):
        dateTimeObj = datetime.now()  
        timestampStr = self.getNowTimestampormatted()
        fileConcatinator = '/' 

        if(self.is_local_run):
            self.POC_INBOUND = 'H:/python'
            self.fileName = 'sample.json'
            self.filePath = self.POC_INBOUND + fileConcatinator + self.fileName
            self.logFileName = self.POC_INBOUND + fileConcatinator + "logFile_" + timestampStr + "." + "txt"
            self.outputFileName = self.POC_INBOUND + fileConcatinator +"data_de_profiled_simplefied_json" + "." + self.output_type
      
        else:  
            POC_INBOUND = 's3://lly-future-state-arch-poc-dev' + fileConcatinator + 'input_data'
            self.fileName = 'POC_HCP_INPUT_FILE_23.json' 
            self.filePath =  POC_INBOUND + fileConcatinator +  self.fileName  
            self.logFileName = self.POC_INBOUND + fileConcatinator + "logFile_" + timestampStr + "." + "txt"
            self.outputFileName = POC_INBOUND + fileConcatinator +  "data_de_profiled_simplefied_json" + "." + self.output_type

    def getInputLines(self):
        lines = ""

        if(self.is_local_run):  
            self.fileVal = open(self.filePath, encoding = "UTF-8") 
            lines = self.fileVal.read()

        else: 
            print("Hello")
            #s3=boto3.client('s3')  
            #inputFolder = 'input_data'
            #file = s3.get_object(Bucket= self.POC_INBOUND, Key= inputFolder + fileConcatinator + self.fileName)
            #lines = file['Body'].read().decode('UTF-8') 
        return lines

    def log(self, logData):
        ts = self.getNowTimestampormatted()

        if(self.log_data_to_file):
            with open(self.logFileName,'a') as self.outLogfile:
                self.outLogfile.write(ts + " " + logData) 
                self.outLogfile.write("\n")
        
        if self.print_to_console:
            print(ts + " " +logData)

    def checkIfValidSource(self, sourceNameVal):
        #Function To Check if the attribute Value Is present In Valid Source List Defined above    
        for validsource in self.validCrossWalks:
            if validsource in sourceNameVal:
                return True
        return False

    def getNestedLevel(self, attribute):
        #Returns the nested Level for each Attribute
        return math.ceil(attribute.count('/') /2) 

    def secondLevelParser(self, keyModel, valueModels, secondLevelKey, keyToFetchForFirstLevel, keyToFetchForSecondLevel):
        #Function Defined for Recursive Parsing. Currently Customized for 2 Levels but TODO: Generic Formatting
        #The FirstLevelKey and SecondLevelKey are always 'Value' But we are passing this from another function so that 
        #in custom cases where need something else like lookupcode in CC, we can do that as well
        dataObject = ""

        for valueModel in valueModels:  
            if valueModel["uri"] in keyModel.completeAttributeUri:
                obj = valueModel[keyToFetchForFirstLevel]
                if(secondLevelKey in obj):
                    valueDataModel = obj[secondLevelKey]
                    for value in valueDataModel:  
                        if value["uri"] == keyModel.completeAttributeUri:
                            obj = value[keyToFetchForSecondLevel]
                            if(len(dataObject) == 0):
                                dataObject = dataObject + obj
                            else:
                                dataObject= "," + dataObject + obj 
                else:
                    return obj
        return dataObject 

    def recursiveParser(self, keyModel, valueModels, currentKey, currentLevel, keyToFetch):
        #This is the main Parser which is called for each KeyModel. 
        # KeyModel - An Object from the custom defined class which contains all attributes for one entity for one crosswalk
        # ValueModels - List Of Value for a specific Key
        # CurrentKey - KeyName for which we have values in ValuesModels
        # CurrentLevel - Nested Level of the Entity
        # keyToFetch - This is always usually 'value' because that is what we want, but for custom code logic we may pass this different
        if currentLevel == 1 and currentKey in self.list_RequiredField_For_Single_Nested_Data:
            for valueModel in valueModels:
                if valueModel["uri"] == keyModel.completeAttributeUri:
                    obj = valueModel[keyToFetch]
                    
                    #Custom Mapping for Specific Attributes

                    return obj
        
        elif currentLevel == 2 and currentKey in self.list_RequiredField_For_Double_Nested_Data:
            
            if currentKey == 'Email':
                return self.secondLevelParser(keyModel, valueModels, 'Email', keyToFetch, keyToFetch) 
            
            elif currentKey == 'RegulatoryAction':
                return self.secondLevelParser(keyModel, valueModels, 'Flag', keyToFetch, keyToFetch)  
            
            elif currentKey == 'Phone':
                return self.secondLevelParser(keyModel, valueModels, 'Number', keyToFetch, keyToFetch)   

            else:
                return None
        elif currentLevel > 2:
            self.log( "Error: Found Level more than 2 attrbute. Unhandled Condition, Attribute Skipped for key: " + currentKey)
            self.log( currentLevel)
    
            
        return None 
        
    def parseAndPopulateData(self, data, crossWalkPath):
        #This function takes in the entire entity data and crosswalk(from the same enitityData)
        # Parses Each Objects created as per data struct and then Evaluates the Value for them to add to final Array
        crossWalkUri = crossWalkPath['uri']
        crossWalkValue = crossWalkPath['value']
        createDate = crossWalkPath['createDate']

        finalData = {}
        flag = False
        finalJsonHasKeys = False

        for key in self.dictionary_SimpleModels:
            valueArrayForKeys = self.dictionary_SimpleModels[key]
            attributeModel = data["attributes"] 
            combinedValueForKey = ''
            valueModels = []

            if key in attributeModel:
                valueModels = attributeModel[key]

            flag=False 
            for valueForKey in valueArrayForKeys: 
                if valueModels and len(valueModels) > 0:
                    valueModels = attributeModel[key]
            
                    #CustomCode For Adding Extra Entity
                    if(key == 'CountryCode'):
                        customObjValue = self.recursiveParser( valueForKey, valueModels, key, valueForKey.nestedLevel, 'lookupCode')
                        finalData['CountryCodeValue']= (str(customObjValue)) 

                    objValue = self.recursiveParser( valueForKey, valueModels, key, valueForKey.nestedLevel, 'value')
                    if objValue and (len(objValue) > 0): 
                        if(combinedValueForKey and len(combinedValueForKey) > 0 ):
                            combinedValueForKey = combinedValueForKey + "," + str(objValue)
                        else:
                            combinedValueForKey = str(objValue)
                        flag=True 

            if flag: 
                finalJsonHasKeys= True
                finalData[key]= combinedValueForKey 

       
        #Custom Fields - Add CrossWalk Related Keys
        for dataVal in self.list_RequiredField_From_CrossWalk:
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
        if(finalData and finalJsonHasKeys):
            self.output_data.append(finalData)
        
    def parseEntity(self, entityData):
        #For each element in EntityArray, We find the SourceTable , CrossWalks And Parses them to 
        # identify AttributeKey, NestedLevels, URI to match and creates an Object for each entity
        # Each such object is added to an array for later to be evaluated
        for crossWalkPath in entityData['crosswalks']:  
            if "sourceTable" in crossWalkPath:
                if(self.checkIfValidSource(crossWalkPath["sourceTable"])):
                    
                    if "attributes" in crossWalkPath and bool(crossWalkPath["attributes"]):
                        
                        for attribute in crossWalkPath['attributes']:
                            key = '/attributes/'
                            attributeStartKeyIndex = attribute.index(key)
                            trimmedURI = attribute[attributeStartKeyIndex + len(key) :]
                            key = trimmedURI[0: trimmedURI.index('/')]
                            obj = SimpleJsonStruct(key, attribute, self.getNestedLevel(trimmedURI), trimmedURI)     
                            self.set_MyDictionary(self.dictionary_SimpleModels, key, obj)  

                        self.parseAndPopulateData(entityData, crossWalkPath) 
                        self.initDictionary()
                    else:
                        self.log("Warning: Attributes Missing for with URI " + crossWalkPath["uri"] + " with sourceTable: " + crossWalkPath["sourceTable"] + ". Hence Ignored during Processing.")    
                else:
                    self.log("Information: Excluding from Source . Not defined as valid source - " + crossWalkPath["sourceTable"])
            else:
                self.log( "Warning: CrossWalk with URI " + crossWalkPath["uri"] + " has no sourceTable Attribute. Hence Ignored during processing.")

    def deleteFileIfExists(self, fileNameToDelete):
        try:    
            os.remove(fileNameToDelete)
        except:
            pass 

    def main(self):
        
        self.setFileInputDirectory()
        lines = self.getInputLines()
        self.initDictionary()

        #The source file is assumed to be an array of json but the format does not enclose in array format
        #Hence PrePending and Pospending array brackets to convert to valid Json
        new_lines = "[" + lines + "]"
        
        #Loading Data to Memory as Json
        allData = json.loads(new_lines)  
         
        #Delete File If Exists
        self.deleteFileIfExists(self.logFileName)
        self.deleteFileIfExists(self.outputFileName)  

        #Application Main
        # Load All Data and for eachData Run the Function
        for entity in allData:
            self.parseEntity(entity)  

        with open(self.outputFileName,'a') as outfile:
            if(self.is_local_run):
                for data in self.output_data:
                    json.dump(data, outfile)
                    outfile.write("\n")
                    #outfile.write(data)a
                    #json.dump(output_data, outfile)
            else:
                print("Hello")
                """
                s3=boto3.client('s3')
                for data in self.output_data:
                    s3.put_object(
                        Body = str(json.dump(data, outfile)),
                        Bucket='lly-future-state-arch-poc-dev',
                        Key='input_data/data_de_profiled_simplefied_json.json'
                    )
                    s3.put_object(
                        Body = outfile.write("\n"),
                        Bucket='lly-future-state-arch-poc-dev',
                        Key='input_data/data_de_profiled_simplefied_json.json'
                    )
                """
                    

           
        # Closing file 
        if(self.fileVal and self.fileVal is not None):
            self.fileVal.close()

            
        if(self.outLogfile and self.outLogfile is not None):
            self.outLogfile.close()

if __name__ == "__main__":
    x = DataJsonFlatten()
    x.main()
