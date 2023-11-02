# A.  Post processing script for cleaning metadata 
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
  
  or
  - ```for f in *.jp2; do opj_decompress -i "$f" -OutFor PNG -o "${f%.*}.png"; done;```
  
- Run the python script to clean the csv metadata and add filepath and rdf-relationship fields
  - ```python3 metadata_process.py```
  </br>

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





# B.  ETL pipeline for LDL contents

You should follow steps to download LDL metadata. There are 2 main ways to do so. One requires server access, the other uses a shell script to curl down datastreams from the LDL website.


### 1)  Extract your Datasteam files (rdf, xml, obj)to a directory for the post processing later.
- ***A. Getting PidList:***</br>
     - Use lsu vpn </br>
     - ssh to lsu subnet machine</br>
     - ssh to dgi-ingest server (hosted on aws)</br>
     - `drush -u 1 islandora_datastream_crud_fetch_pids --collection="hnoc-aww:collection" --pid_file=/tmp/collection-data/hnoc-aww:collection.txt`</br>
     - `drush -u 1 idcrudfd --pid_file=/tmp/collection-data/hnoc-aww\:collection.txt --datastreams_directory=/tmp/collection-data/ds --dsid=OBJ`</br>
     - How do we identify compounds, and get their children?</br>
        - Example: https://ingest.louisianadigitallibrary.org/islandora/object/hnoc-aww%3A40/manage/</br>
        - Search for content type, Use machine name from link{1}/collection: compoundCModel, sp_pdf, sp_large_image_cmodel</br>

- ***B) Downloading the Datastream files:*** </br>
     - target collection => hnoc-awww:collection</br>
     - visit https://ingest.louisianadigitallibrary.org/islandora/object/hnoc-aww:collection/manage</br>
     - PIDS written to file /tmp/hnoc-aww-namespaced.txt 
     - after login via server and nav to drupal root (aka /var/www/data)
     - `drush -u 1 islandora_datastream_crud_fetch_pids --namespace=hnoc-aww --pid_file=/tmp/hnoc-aww-namespaced.txt`
     - `mkdir /tmp/hnoc`
     - Run these drush commands in sequence to download OBJ, RDF and MODS files:</br>
     ```
        drush -u 1 idcrudfd --pid_file=/tmp/hnoc-aww-namespaced.txt --datastreams_directory=/tmp/hnoc --dsid=OBJ </br>
        drush -u 1 idcrudfd --pid_file=/tmp/hnoc-aww-namespaced.txt --datastreams_directory=/tmp/hnoc --dsid=RELS-EXT </br>
        drush -u 1 idcrudfd --pid_file=/tmp/hnoc-aww-namespaced.txt --datastreams_directory=/tmp/hnoc --dsid=MODS </br>
     ```

     - compress the datastreams for export:</br>
     ```
        cd /tmp/hnoc</br>
        zip hnoc-data.zip *  
     ```
     - move the file to the /tmp directory so it is accessible</br>
        `mv hnoc-data.zip /tmp/`</br>
     - log out of the ingest server, back into the computer on the subnet `ctl-D`</br>
     - scp the file:</br>
        `cp dgi-ingest:/tmp/hnoc-data.zip ~/Downloads`</br>
     - CTL+D (to log out of subnet machine.)</br>
     - copy to other computes as needed.   </br>
        `cp Work:/tmp/hnoc-data.zip ~/Downloads/`</br></br>
            
### 2) Debug and Run xml2workbench tool:</br>
xml2workbench is a python script that extracts metadata from xml files, for each node in old Digital Library website. It scripted by Rosie Le Faive. However, the original python script will not work on the Louisiana Digital Library contents, adn it needs to be edited according to our need, so it could be run on all XML files to expot the metadata.</br
- ***Edit the code:*** </br>
Edit the code to fix Nan-type errors in order to have the code work on creating the right metadata fields according to LDL fields (Documentation about the steps to fix the code is on (https://github.com/Miladkhanlou/XML-to-Metadata-Tool)</br>
- ***Change the input directory:*** </br>
- ***Run the xml2workbench.py*** </br></br>

### 3) Run the metadata process tool:</br>
The metadata_process is a customized python script for processing, cleaning and fixing data on each the dataset.</br>
To learn more about how script work,s please visit: (https://github.com/Miladkhanlou/Post-Processing-Tool-For-LDL)</br></br>
### 4) Install, configure and run Islandora Workbench ingest tool: </br>
After post processing metadata, now every thing is ready to run Islandora workbench to ingest data into the new Digital Library website.
See documentation on how to install and configure isladnora workbench ingestion tool and running using bash (https://github.com/Miladkhanlou/Islandora_workbench_Ingest_tool)</br>
