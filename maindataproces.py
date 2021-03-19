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
import collections
from datetime import datetime 
import botocore


class SimpleJsonStruct:
    #Struct To Hold relevant Information for Each crosswalk in each Entity
    key: str
    completeAttributeUri : str
    nestedLevel: int
    trimmedAttributeUri: str
    crossWalkType: str
    sourceTable: str
    crosWalkValue: str

    def __init__(self, key, completeAttributeUri, nestedLevel, trimmedAttributeUri, crossWalkType, sourceTable, crosWalkValue):
        self.key = key
        self.trimmedAttributeUri = trimmedAttributeUri
        self.completeAttributeUri = completeAttributeUri
        self.nestedLevel = nestedLevel
        self.crossWalkType = crossWalkType
        self.sourceTable = sourceTable
        self.crosWalkValue = crosWalkValue


class DataJsonFlatten:
    #There are the prefixes of valid sources we want to read from input json. 
    #validCrossWalks= ["CODS"]
    validCrossWalks=["CODS","CTLN","MDU","MCRM","EMBS","DATAVISION","CMS","PBMD"]
    
    dictionary_SimpleModels = {} 
    output_data = [] 
    logData = []

    #Required Fields in Output Json
    list_RequiredField_For_Single_Nested_Data=["HCPUniqueId","ActiveFlag","EffectiveStartDate","EffectiveEndDate","CountryCode","Name","FirstName","MiddleName","LastName",
    "Gender","Source","OriginalSource", "Specialities"]
    list_RequiredField_For_Double_Nested_Data=["RegulatoryAction","Email","Phone"]
    list_RequiredField_From_CrossWalk = ["CrossWalkUri", "CrossWalkValue","CreateDate"]
    list_RequiredField_Custom=["EntityId"]

    #Env Run Variables
    log_data_to_file = True
    print_to_console = True 
    is_local_run = True
    
    '''
        Expected Data Format For Read Line By Line = True
            [
                {},
                {},
                {}
            ]
            The Square Brackets is missing - would still run
            The comma at end if missing would still run
            Each Line should have entire entity data
        
        Expected Data Format For Read Line By Line = False
                {},{},{}
                {},
                {},
                {}
            
            The Square Brackets should not come
            Data can be at multiple Lines, Comma seperated
    '''
    read_line_byLine = False

    #Updated RunTime Variables
    fileName = "" 
    POC_INBOUND = ""
    outputFilePath = "" 
    logFilePath = "" 
    outputFileName = "" 
    logFileName = ""
    fileVal = None
    outLogfile = None 
    final_output_string_for_s3 = ''
    final_log_string_for_s3 = ''
    s3=None
    #static string
    fileConcatinator = '/' 
    multipleValueSeperator = '|'
    bucket_name = 'lly-future-state-arch-poc-dev'
    output_type='json' 

    def __init__(self):
        print("Information: Initialized Script")

    def initDictionary(self): 
        self.dictionary_SimpleModels = {} 

    def initVariables(self): 
        self.initDictionary()
        self.output_data = [] 

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
        timestampStr = self.getNowTimestampormatted() 

        if(self.is_local_run):
            self.POC_INBOUND = 'H:/python'
            self.fileName = 'sample.json'
            self.filePath = self.POC_INBOUND + self.fileConcatinator + self.fileName
            self.logFileName = "logFile_" + timestampStr + "." + "txt"
            self.logFilePath = self.POC_INBOUND + self.fileConcatinator + self.logFileName
            self.outputFileName = "data_de_profiled_simplefied_json" + "." + self.output_type
            self.outputFilePath = self.POC_INBOUND + self.fileConcatinator + self.outputFileName
        else:  
            print("")
            #SingleLineComment
            POC_INBOUND = self.bucket_name + self.fileConcatinator + 'input_data'
            self.fileName = 'Source_file.json' 
            self.filePath =  POC_INBOUND + self.fileConcatinator +  self.fileName  
            self.logFileName = "logFile_" + timestampStr + "." + "txt"
            self.logFilePath = "logs" + self.fileConcatinator + self.logFileName
            self.outputFileName = "data_de_profiled_simplefied_json" + "." + self.output_type
            self.outputFilePath = "Data" + self.fileConcatinator +  self.outputFileName
            #SingleLineComment

    def getFile(self):
        lines = ""

        if(self.is_local_run):  
            self.fileVal = open(self.filePath, encoding = "UTF-8") 
            return self.fileVal

        else:    
            #SingleLineComment
            self.s3=boto3.client('s3')  
            inputFolder = 'input_data'
            self.file = self.s3.get_object(Bucket= self.bucket_name, Key= inputFolder + self.fileConcatinator + self.fileName)
            return self.file

            #SingleLineComment
        return lines

    def log(self, logData):
        ts = self.getNowTimestampormatted()

        if(self.log_data_to_file):
            self.logData.append(ts + " " +logData)
        
        if self.print_to_console:
            print(ts + " " +logData)

    def getOutputAsStringLineSeperated(self, inputArray, shouldDump):
        str = ''
        for obj in inputArray :
            if(shouldDump):
                str = str +  json.dumps(obj)  +  os.linesep
            else: 
                str = str +  obj  +  os.linesep
        return str

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
    
    def combineMultipleInfo(self, objValue, combinedValueForKey):
        if objValue and (len(objValue) > 0): 
            if(combinedValueForKey and len(combinedValueForKey) > 0 ):
                combinedValueForKey = combinedValueForKey + self.multipleValueSeperator + str(objValue)
            else:
                combinedValueForKey = str(objValue)
        return combinedValueForKey


    def getAddress(self, crossWalkPath, data):
        addressList = data['attributes']['Address']
        
        crossWalkType = crossWalkPath['type']
        crossWalkSourceTable = crossWalkPath['sourceTable']
        crossWalkValue = crossWalkPath['value']

        addressCombined = ''

        for address in addressList:
            startObjectCrosswalks = address['startObjectCrosswalks']
            for startObjectCrosswalk in startObjectCrosswalks:
                addressCrossWalkType = startObjectCrosswalk['type']
                addressCrossWalkSourceTable = startObjectCrosswalk['sourceTable']
                addressCrossWalkValue = startObjectCrosswalk['value']

                if(crossWalkType == addressCrossWalkType and  crossWalkSourceTable == addressCrossWalkSourceTable and crossWalkValue == addressCrossWalkValue):
                    if(None == addressCombined or len(addressCombined) == 0):
                        addressCombined =  address['label']
                    else: 
                        addressCombined = addressCombined + self.multipleValueSeperator + address['label']


        return addressCombined

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
                    
                    if(key == 'Specialities'):
                        customObjValue = self.recursiveParser( valueForKey, valueModels, key, valueForKey.nestedLevel, 'label')
                        combinedValueForKey = self.combineMultipleInfo(customObjValue, combinedValueForKey)
                        finalData[key]= combinedValueForKey
                        finalJsonHasKeys = combinedValueForKey and len(combinedValueForKey) > 0
                        continue

                    objValue = self.recursiveParser( valueForKey, valueModels, key, valueForKey.nestedLevel, 'value')
                    if objValue and (len(objValue) > 0): 
                        if(combinedValueForKey and len(combinedValueForKey) > 0 ):
                            combinedValueForKey = combinedValueForKey + self.multipleValueSeperator + str(objValue)
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

        addressValue = self.getAddress(crossWalkPath, data)
        
        if(addressValue and len(addressValue) > 0):
            finalData['Address'] = addressValue
        
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
                            if('Address' in attribute):
                                print(attribute)
                            key = '/attributes/'
                            attributeStartKeyIndex = attribute.index(key)
                            trimmedURI = attribute[attributeStartKeyIndex + len(key) :]
                            key = trimmedURI[0: trimmedURI.index('/')]
                            obj = SimpleJsonStruct(key, attribute, self.getNestedLevel(trimmedURI), trimmedURI, crossWalkPath['type'],crossWalkPath['sourceTable'], crossWalkPath['value'])
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

    def writetoFile(self, output_string, filePath, fileName): 
        if(None == output_string or len(output_string) == 0):
            return

        if(self.is_local_run): 
            with open(filePath,'a') as outfile:
                outfile.write(output_string) 
        else:  
            #SingleLineComment"
            self.s3.put_object(Body=output_string, Bucket=self.bucket_name, Key=  fileName)  
            #SingleLineComment
    
    def appendToS3File(self, count):
        timeStamp = self.getNowTimestampormatted() 
        fileName = 'Data/' + 'Data_SingleNested' + timeStamp +'_part00' + str(count % 1000) + '.json'
        self.writetoFile(self.final_output_string_for_s3, fileName ,  fileName)
        self.final_output_string_for_s3 = ''

    def runTransformation(self, new_lines):
        try: 
            #Loading Data to Memory as Json 
            allData = json.loads(new_lines)  
            self.initVariables()

            #Application Main
            # Load All Data and for eachData Run the Function

            if(self.read_line_byLine):
                self.parseEntity(allData)  
            else:
                for entity in allData:  
                    self.parseEntity(entity)   
        
            output_string = self.getOutputAsStringLineSeperated(self.output_data, True)  
            
            if(self.is_local_run):
                self.writetoFile(output_string, self.outputFilePath,  self.outputFilePath) 
            else:
                self.final_output_string_for_s3 = self.final_output_string_for_s3 + output_string 
             
        finally:  
            log_output_string = self.getOutputAsStringLineSeperated(self.logData, False) 
            if(self.is_local_run):
                self.writetoFile(log_output_string,  self.logFilePath, self.logFilePath) 
            else:
                self.final_log_string_for_s3 = self.final_log_string_for_s3 + log_output_string 
            

    def main(self):
        try:
            self.setFileInputDirectory()
            
            self.log('Deleting Existing Output File')
            #Delete File If Exists
            self.deleteFileIfExists(self.logFilePath)
            self.deleteFileIfExists(self.outputFilePath)  
            self.log('Deleting Existing Output File Completed')

            self.log('Is this Local Run = ' + str(self.is_local_run))
            self.log('Is Read Line By Line = ' + str(self.read_line_byLine)) 

            file = self.getFile()

            if(self.read_line_byLine):
                if(self.is_local_run):
                    lines = self.fileVal.readlines()
                    i = -1
                    for line in lines: 
                        i = i + 1
                        self.log('Iterating over line')
                        self.log(str(i))
                        if(line and len(line) > 10):
                            line = line[:line.rindex('}') + 1]
                            self.runTransformation(line)

                else:
                   for i,line in enumerate(file['Body'].iter_lines()):
                        line1 = line.decode('utf-8')  
                        self.log('Iterating over line')
                        self.log(str(i))
                        if(i % 1000 == 0): 
                            self.appendToS3File(i)
                            '''
                            logFileExistingData = ''
                            try:
                                logFileExistingFile = s3.get_object(Bucket= self.bucket_name, Key= self.logFilePath)
                                logFileExistingData = logFileExistingFile['Body'].read().decode('UTF-8')  
                            except botocore.exceptions.ClientError as e:
                                logFileExistingData = '' 
                            
                            self.final_log_string_for_s3 = logFileExistingData + self.final_log_string_for_s3
                            self.writetoFile(self.final_log_string_for_s3, self.logFilePath,  self.logFilePath) 
                            '''
                            self.final_log_string_for_s3 = '' 
                         

                        if(line1 and len(line1) > 10):
                            line1 = line1[:line1.rindex('}') + 1]
                            self.runTransformation(line1)
                    
            else:
                if(self.is_local_run):
                    lines = self.fileVal.read()
                    lines = "[" + lines + "]"
                    self.runTransformation(lines)
                else:
                    lines = file['Body'].read().decode('UTF-8')  
                    lines = "[" + lines + "]"
                    self.runTransformation(lines)

            # Closing file 
            if(self.fileVal and self.fileVal is not None):
                self.fileVal.close()

            if(self.outLogfile and self.outLogfile is not None):
                self.outLogfile.close()

        finally: 
            if(self.is_local_run):
                print('End Of Program')
            else:
                self.appendToS3File()
                #self.writetoFile(self.final_output_string_for_s3, self.outputFilePath,  self.outputFilePath) 
                #self.writetoFile(self.final_log_string_for_s3,  self.logFilePath, self.logFilePath)  
             
 
if __name__ == "__main__":
    x = DataJsonFlatten()
    x.main()
