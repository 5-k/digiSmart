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

#Source Json File Path Defined Here
GCCEPH_INBOUND="H:/python"

#Source File Name Defined Here
json_file_path = GCCEPH_INBOUND + "/sample.json" 
f = open(json_file_path, encoding="UTF-8") 

#The source file is assumed to be an array of json but the format does not enclose in array format
#Hence PrePending and Pospending array brackets to convert to valid Json
newData = "[" + f.read() +"]" 

#Output type Defined as Json. Can be CSV as well. Need to handle case in case we need csv data
output_type='json'

#There are the prefixes of valid sources we want to read from input json. 
#validCrossWalks= ["CODS","MDU","CTLN"]
validCrossWalks= ["CODS","MDU","CTLN","MCRM","EMBS","DATAVISION","CMS","PBMD"]

#Required Fields in Output Json
list_RequiredField_For_Single_Nested_Data=["HCPUniqueId","ActiveFlag","EffectiveStartDate","EffectiveEndDate","CountryCode","Name","FirstName","MiddleName","LastName","Gender","Source","OriginalSource"]
list_RequiredField_For_Double_Nested_Data=["RegulatoryAction","Email","Phone"]
list_RequiredField_From_CrossWalk = ["CrossWalkUri", "CrossWalkValue","CreateDate"]
list_RequiredField_Custom=["EntityId"]

#Simple ListOf Struct Data structure to hold information on crossWalks for Each Entity
list_SimpleModels = []
# returns JSON object as  
# a dictionary 

#Loading Data to Memory as Json
allData = json.loads(newData)  

#Initializing Output Array Empty
output_data = []

#Struct To Hold relevant Information for Each crosswalk in each Entity
class SimpleJsonStruct:
    completeAttributeUri : str
    nestedLevel: int
    trimmedAttributeUri: str

    def __init__(self, completeAttributeUri, nestedLevel, trimmedAttributeUri):
        self.trimmedAttributeUri = trimmedAttributeUri
        self.completeAttributeUri = completeAttributeUri
        self.nestedLevel = nestedLevel
 
#Function To Check if the attribute Value Is present In Valid Source List Defined above    
def checkIfValidSource(sourceNameVal):
    for validsource in validCrossWalks:
        if validsource in sourceNameVal:
            return True
    return False

#Returns the nested Level for each Attribute
def getNestedLevel(attribute):
    return math.ceil(attribute.count('/') /2) 

#Function Defined for Recursive Parsing. Currently Customized for 2 Levels but TODO: Generic Formatting
#The FirstLevelKey and SecondLevelKey are always 'Value' But we are passing this from another function so that 
#in custom cases where need something else like lookupcode in CC, we can do that as well
def secondLevelParser(keyModel, valueModels, secondLevelKey, keyToFetchForFirstLevel, keyToFetchForSecondLevel):
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

#This is the main Parser which is called for each KeyModel. 
# KeyModel - An Object from the custom defined class which contains all attributes for one entity for one crosswalk
# ValueModels - List Of Value for a specific Key
# CurrentKey - KeyName for which we have values in ValuesModels
# CurrentLevel - Nested Level of the Entity
# keyToFetch - This is always usually 'value' because that is what we want, but for custom code logic we may pass this different
def recursiveParser(keyModel, valueModels, currentKey, currentLevel, keyToFetch):
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
        print("Error: Found Level " + str(currentLevel) + " attrbute. Unhandled Condition, Attribute Skipped")
 
         
    return None 
    
#This function takes in the entire entity data and crosswalk(from the same enitityData)
# Parses Each Objects created as per data struct and then Evaluates the Value for them to add to final Array
def parseAndPopulateData(data, crossWalkPath):
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
    
#For each element in EntityArray, We find the SourceTable , CrossWalks And Parses them to 
# identify AttributeKey, NestedLevels, URI to match and creates an Object for each entity
# Each such object is added to an array for later to be evaluated
def parseEntity(entityData):
    
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
                    parseAndPopulateData(entityData, crossWalkPath)
                    list_SimpleModels.clear()
                else:
                    print("Warning: Attributes Missing for with URI " + crossWalkPath["uri"] + " with sourceTable: " + crossWalkPath["sourceTable"] + ". Hence Ignored during Processing.")    
            else:
                print("Information: Excluding from Source . Not defined as valid source - " + crossWalkPath["sourceTable"])
        else:
            print("Warning: CrossWalk with URI " + crossWalkPath["uri"] + " has no sourceTable Attribute. Hence Ignored during processing.")
    

#Application Main
# Load All Data and for eachData Run the Function
for entity in allData[0]:
    parseEntity(entity)

#Post Processing, Dump all data to json
fileName= "data_de_profiled_simplefied_json" + "." + output_type
with open(fileName,'w') as outfile:
    json.dump(output_data, outfile)
    list_SimpleModels.clear()  

# Closing file 
f.close() 
      
