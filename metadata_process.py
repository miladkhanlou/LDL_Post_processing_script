from dataclasses import replace
from fileinput import filename
import pandas as pd
import xml.etree.ElementTree as ET
import glob
from os import listdir , sep, path
import os

###### Getting a Directory path with Data/"CollectionName" ######
Paths = "Data/"
OBJ_paths = []
for filenames in os.listdir(Paths):
    OBJ_paths.append(format(os.path.join(Paths, filenames)))
###### Getting a Collection name ######
files = listdir('csv/')
files.sort()
direct = []
for file in files:
    if file.endswith(".csv"):
        direct.append(file)

#################### 2) Getting data and fill the file column if files exist in the Data directory ########################
def input_directory(directory, OBJS):
    Collection = directory.split(".")[0]
    print(Collection)
    LDLdf = pd.DataFrame(pd.read_csv(directory))
    LDLdf.rename(columns= {'PID' : 'id'},  inplace = True)
    coll_name = []
    coll_num = []
    fileName = []
    id_to_list = LDLdf["id"].tolist() ###Putting the elements of id column to a list###
    for IDs in id_to_list:
        splitted_IDs= IDs.split(':')
        coll_name.append(splitted_IDs[0])
        coll_num.append(splitted_IDs[1])
    for s in range(len(coll_name)):
        fileName.append("{}_{}_OBJ".format(coll_name[s], coll_num[s]))

    ObjFiles = [] #getting the names of the OBJ FILES 
    fileformat = "" #getting the file type of OBJ FILES
    
    FILES = os.listdir(OBJS)
    for file in FILES:
        if "OBJ" in file:
            ObjFiles.append(file.split(".")[0])
            fileformat =  ".{}".format(file.split(".")[1])

    #Filling the file_column list to fill the file column:
    file_column = []
    for fileNames in fileName:
        if fileNames in ObjFiles:
            file_column.append("Data/{}/{}{}".format(Collection,fileNames,fileformat))
        else:
            file_column.append("")

    LDLdf["file"] = file_column
    del fileformat
    LDLdf["parent_id"] = ""
    LDLdf["field_weight"] = ""
    LDLdf["field_member_of"] = ""
    LDLdf["field_model"] = "Language"
    LDLdf["field_resource_type"] = "some test Value, Should not be empty" #something we can drive from by checking the file extension of obj 
    LDLdf.drop("field_date_captured", inplace=True ,axis= 1, errors='ignore')
    LDLdf.drop("field_is_preceded_by", inplace=True ,axis= 1,errors='ignore')
    LDLdf.drop("field_is_succeeded_by", inplace=True ,axis= 1,errors='ignore')
    return LDLdf

#CREATE OUTPUT CSV
def run_name_change():
    for csvs, OBJs in zip(direct, OBJ_paths):
        data = input_directory(csvs,OBJs)
        
        nameChange = data.to_csv("/Users/mfatol1/Documents/islandora_workbench/input_data/Presentation/csv/output/{}".format(csvs), index=False)
        
    return nameChange
run_name_change()


#################### 2) fill field_member_of, parent_id, field_weight column ########################

def inputrdf(RDF_dir, dir):
    data = glob.glob("{}/*.rdf".format(RDF_dir))
    # collection_name = RDF_dir.split(".")[0]
    tags = [] #getting none-splitted
    val = [] #adding values to
    tag_name = [] #ALL the Tags in the rdf
    attrib = []
    text = []
    weightList= []
    data.sort()
    for dirs in data:
        rdf = ET.parse("{}".format(dirs))
        itter = rdf.iter()
        for inner in itter:
            tags.append(inner.tag)
            val.append(inner.attrib)
            text.append(inner.text)

    for tag in tags:
        split_tags = tag.split('}')
        tag_name.append(split_tags[1]) # ALL THE TAGS
    for vals in val:
        attrib.append(list(vals.values()))
    for num in range(len(tags)):
        if "isSequenceNumberOf" in tags[num]:
            weightList.append(text[num])
        else:
            weightList.append("")
    mylist = list(zip( tag_name, attrib, weightList))
    mylist_to_list = [list(i) for i in mylist] ##Extra(To make each element from tuple to list)##
    splitting = []
    for each in mylist_to_list:
        if each[0] == ("RDF"):
            splitting.append(each)
        if each[0] == ("hasModel"):
            splitting.append(each)
        if each[0] == ("isConstituentOf"):
            splitting.append(each)
        # if each[0] == ("isPageOf"):
        #     splitting.append(each)
        if each[0] == ("isSequenceNumber"):
            splitting.append(each)
        if each[0] == ("isPageNumber"):
            splitting.append(each)
        if each[0] == ("isSection"):
            splitting.append(each)
        if each[0] == ("isMemberOf"):
            splitting.append(each)
        if each[0] == ("deferDerivatives"):
            splitting.append(each)
        if each[0] == ("generate_ocr"):
            splitting.append(each)
    new = [ones for ones in mylist_to_list if ones not in splitting] #only keeps Description, isSequenceNumberOf and isMemberOfCollection
    weight = []
    field_member_of = []
    parrent = []
    count = []
    for q in new:
        if "isPageOf" in q[0]:
            print(q)
            count.append(q)
    print(len(count))
    print("------------------------------------------------------------------------------")
    for r in range(len(new)):
        if r+1 > (len(new)):
            break   
        else:
            if "Description" in new[r][0]:
                if "isPageOf" in new[r+1][0]:
                    collectionName = RDF_dir.split("/")[1]
                    nameofnumber = new[r+1][1][0]
                    ParentNumber = nameofnumber.split(":")[2]
                    parrent.append("{}:{}".format(collectionName, ParentNumber))
                    weight.append(new[r+1][2])
                    
                if "Description" in new[r+1][0]:
                    collectionName = RDF_dir.split("/")[1]
                    parrent.append("{}:COLLECTION".format(collectionName))
                    weight.append("")
                                        
                if "isSequenceNumberOf" in new[r+1][0]:
                    collectionName = RDF_dir.split("/")[1]
                    nameofnumber = new[r+1][0]
                    ParentNumber = nameofnumber.split("_")[1]
                    parrent.append("{}:{}".format(collectionName, ParentNumber))
                    weight.append(new[r+1][2])
                                      
                if "isMemberOfCollection" in new[r+1][0]:
                    Collection = new[r+1][1][0].split("/")[1]
                    field_member_of.append(Collection)
                    parrent.append(Collection)
                    weight.append("")

                if "isMemberOfCollection" not in new[r+1][0]:
                    field_member_of.append("")

    #Collection:
    print("collection RDF directory: {}".format(RDF_dir)) #directory of data
    print("collection csv: {}".format(dir)) #directory of csv
    #info:
    print("number of Meta list: ({})".format(len(new))) #LENGH OF "new" LIST CONTAINING ALL 2 TAGS
    print("Lenght of field_member_of(collections): ({})".format(len(field_member_of))) #Lenght of field_member_of(collections)
    print("Lenght of weight(child numbers): ({})".format(len(weight))) #Lenght of field_member_of(collections)
    print("Lenght of parrent names: ({})".format(len(parrent))) #Lenght of parrent names
    print("--------------------------------------------------------------------------------------------------------------------")

    LDL2 = pd.read_csv("csv/output/{}.csv".format(RDF_dir.split("/")[1]))
    LDL2df = pd.DataFrame(LDL2)     
    LDL2df["parent_id"] = parrent    
    LDL2df["field_weight"] = weight
    LDL2df["field_edtf_date_created"] = ""
    LDL2df["field_linked_agent"] = ""

    

    parentChild = LDL2df.to_csv("/Users/mfatol1/Documents/islandora_workbench/input_data/Presentation/csv/output/{}".format(dir), index=False)
    return parentChild

def run():
    for path, dir in zip(OBJ_paths, direct):
        input = inputrdf(path, dir)
    return input
run()