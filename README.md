# Introduction 
The purpose of this Python script is to export tenth milepoint data to CSV formats for emergency vehicles. 

# Getting Started
This process starts with the Tenth_Mill_All feature class and generates a stand-alone table in SQL to hold the requested information - RT_UNIQUE, Route, Road Name, Milepoint, Latitude, Longitude, County and City.  Subsets of this dataset are also generated representing even milepoints and half-milepoints.  Also generated is a dataset of turnaround points at crossover sections along divided highways with the same data format.
1.  Create Turnaround route layer
2.  Select nodes where section indicates crossover
3.  Locate along route to get milepoint along 000 or 010 route for each node
4.  Execute stored procedure to populate POI tables in SQL Server.
5.  Build csv tables
6.  Zip county files and write to storage location:
    -   D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs
    -   Zipped files are uploaded to BOX through existing separate process associated with weekly HIS extracts.
7.  If process fails, an email is sent to GIS Team SDE Administrator Lead and Backup.


# Dependencies
* Python 3.6.5
* ArcGIS Pro 2.2.4
* Python modules:
    -   arcpy
    -   pyodbc
    -   pandas
    -   os, shutil
    -   zipfile
    -   smtplib

# Installation & Operation
*   This process script runs as a scheduled task on the PRD SDE server (KTC1PP-SNGI001A), using the kytc\gis.autoprocessprod account, which has access to the shared folder on \GIS.  The operational script to produce these KMzs is located in the following directory:
    
    - ...D:\Scripts\Internal_Tasks\Interstate_Pkwy_MM_KMZs\Interstate_Parkway_KMZGenerator.py# Milepoint_POI_Generator
