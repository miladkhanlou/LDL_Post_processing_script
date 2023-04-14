# Post processing script for cleaning metadata 
This script cleans csv data generated from the LSU fork of [xml2workbench.py](https://github.com/lsulibraries/xml2workbench/) and adds columns of new data to prepare content for ingest with [islandora_workbench](https://github.com/mjordan/islandora_workbench). Migrating of collections from the Louisiana Digital Library website into an Islandora 8 instance. Running the script will 'clean' the metadata, it also adds new fields to preserve relationshps between entities, such as parent/compound or newspape/issue/page. The output csv file can be processed using Islandora Workbench to upload content to the Islandora 8 instance specified in your Islandora Workbench config file. 

## Requirements:

A csv file processed by [xml2workbench.py](https://github.com/lsulibraries/xml2workbench/) see our fork of Rosie LaFaive's xml2workbench for instructions

Downloaded zip files from the LDL. We have used [islandora_datastream_crud](https://github.com/mjordan/islandora_datastream_crud) to download datastreams from our islandora 7.x server. MODS, RELS-EXT (RDF), and OBJ datastreams (.mp3, .mp4, .jp2*, .pdf) (jp2s must be converted to PNG, we suggest using imagemagick's convert)

### instructions:

- Create a "Data" folder in your Post-Processing-Tool-For-LDL folder:
  -```cd Post-Processing-Tool-For-LDL```
  - ```mkdir Data```
  - ```mkdir csv```
  - ```unzip -d Data path-to-your/datafiles.zip```
  - ```cd Data/datafiles/```
  - ```cp *.csv ../csv```
- if your data includes jp2 files, convert them to PNG:
  - ```for f in *.jp2; do convert "$f" -type truecolor "${f%.*}.png"; done;```
  - ```cd ..```
- Run the python script to clean the csv metadata and add filepath and rdf-relationship fields
  - ```python3 metadata_process.py```

## functionalities of the the python script:
### 1) Creating needed columns and dropping the unwanted fields from Metadata.
- a) In the first step, the script gets the metadata CSV, uses the existence of OBJ files in the directory, and processes them to generate the correct pattern for the file column according to the existence of those OBJ data for each node.
- b) Next, it will create needed columns such as a file, field_weight, and field_member_of and add those file directory patterns to the file columns for each node(webpage).
- c) Also, it drops unwanted columns that are not in Drupal default. The existence of these columns will stop the workbench process, as these fields do not exist in Drupal.

### 2) Processing the RDF files to write down the relationships(parent and children pages)
- 1) With looping into the file directory and RDF files in the folder, the script uses tags, attributes, and texts in RDF and cleans their name in an order that drupal can understand them.
- 2) Using three main tags inside the RDFs, which determine the relationship between nodes, the parent_id column can be filled out. Having information about relationships is crucial in the Workbench data ingestion stage.  
- 3) Also, by looping through  RDFs we get the correct information about field_weight, which indicates the order of children's pages.
- 4) Also, by looping through  RDFs, we get the correct information about field_weight, which indicates the order of children's pages.
