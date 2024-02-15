#
# ================================================================================================================
# Name:        Export_County_Tenth_Mile_POIs.py
# Purpose:     To export tenth milepoint data to CSV formats for emergency vehicles.
# Author:      Teri Dowdy
# Created:     2021-03-01
# Modified:    2023-10-12
# Arcpy:       3.7
# Description: This process starts with the Tenth_Mill_All feature class and generates a stand-alone table in SQL to
#     hold the requested information - RT_UNIQUE, Route, Road Name, Milepoint, Latitude, Longitude, County and City.
#     Subsets of this dataset are also generated representing even milepoints and half-milepoints.  Also generated is
#     a dataset of turnaround points at crossover sections along divided highways with the same data format.
# Copyright: (c) Kentucky Transportation Cabinet @2021
# ================================================================================================================

""" Notes:  This script performs the following tasks in order:
        1.  Create Turnaround route layer
        2.  Select nodes where section indicates crossover
        3.  Locate along route to get milepoint along 000 or 010 route for each node
        4.  Execute stored procedure to populate POI tables in SQL Server.
        5.  Build csv tables
        6.  Zip county files and write to storage location:
                D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs
                Zipped files are uploaded to BOX through existing separate process associated with weekly HIS extracts.
        7.  If process fails, an email is sent to GIS Team SDE Administrator Lead and Backup.
"""

import arcpy
import pyodbc
import pandas as pd
import os, shutil
import zipfile
import smtplib

# Define variables
sdeCNXN = "path to SDE"
roadsFC = sdeCNXN + "\\KYALLRDS_M"
nodesFC = sdeCNXN + "\\NODES"
crossoverFC = sdeCNXN + "\\CROSSOVERS"
turnTABLE = sdeCNXN + "\\TURNAROUND_POI"
outLOC = "<path to putput dataset"

# ================================================================================================================
# Instantiate the ODBC connection to KYTCVectorLoader_HIS geodatabase and define connection cursor
# ---------------------------------------------------------------------------------------------------------------
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; '
                      'servername; '
                      'DATABASE=master; '
                      'Trusted_Connection=yes')
cur = conn.cursor()
conn.autocommit = True

# ================================================================================================================
# FUNCTIONS
# ----------------------------------------------------------------------------------------------------------------
#  FUNCTION: sendEmail
# ----------------------------------------------------------------------------------------------------------------
def sendEmail(eSubject, eMessage):
    # Import modules necessary for sending email
    from email.message import EmailMessage
    # Generate email message
    msg = EmailMessage()
    msg['From'] = "noreply email address"
    msg['To'] = "SDE Admins"
    msg['CC'] = "other interested parties"
    msg['Subject'] = eSubject
    msg.set_content(eMessage)
    # Send email message via SMTP server
    smtpServer = smtplib.SMTP('SMTP.address', 25)
    smtpServer.send_message(msg)
    smtpServer.quit()

# ================================================================================================================

# ================================================================================================================
# MAIN
# ================================================================================================================
try:
    # 1. Create Turnaround route layer
    whereCLAUSE = "RT_UNIQUE LIKE '%-000' OR RT_UNIQUE LIKE '%-010'"
    selRDS = arcpy.SelectLayerByAttribute_management(roadsFC, "NEW_SELECTION",whereCLAUSE)
    spatialREF = arcpy.Describe(roadsFC).spatialReference
    rdsLYR = arcpy.CreateFeatureclass_management("in_memory", selRDS, "POLYLINE", "", "DISABLED", "DISABLED",
                                                 spatialREF, "", "0", "0", "0")
    print("1. Turnaround route layer created.")

    # 2. Select nodes where section indicates crossover
    #    Select from NODES feature class where section is crossover
    whereCLAUSE = "NODE_DESCR LIKE '%  -03%' OR NODE_DESCR LIKE '%  -04%' OR NODE_DESCR LIKE " \
                  "'%  -05%' OR NODE_DESCR LIKE '%  -06%'"
    selNODES = arcpy.SelectLayerByAttribute_management(nodesFC, "NEW_SELECTION",whereCLAUSE)
    spatialREF = arcpy.Describe(nodesFC).spatialReference
    nodesLYR = arcpy.CreateFeatureclass_management("in_memory", selNODES, "POINT", "", "DISABLED", "DISABLED",
                                                   spatialREF, "", "0", "0", "0")
    print("2. Nodes at crossovers selected.")

    # 3. Locate along route to get milepoint along 000 or 010 route for each node
    arcpy.Delete_management(turnTABLE)
    out_event_properties = "RT_UNIQUE POINT MILEPOINT"
    arcpy.LocateFeaturesAlongRoutes_lr(selNODES,selRDS,"RT_UNIQUE","0 Feet",turnTABLE,out_event_properties,
                                   "FIRST","DISTANCE","ZERO","FIELDS","M_DIRECTON")
    print("3. Locate along route process completed.")

    # 4. Execute stored procedure to populate POI tables in SQL Server.
    conn.execute("EXEC [KYTCVectorLoader_HIS].[dbo].[uspCreate_MILEPOINT_POI_Tables] ")
    print("4. SQL stored procedure completed.")

    # 5. Build csv tables
    csvDIR = r"D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs"
    # delete old POI files
    shutil.rmtree(csvDIR)
    os.mkdir(csvDIR)

    # Generate POI CSV files
    print("5. Building CSV tables...")
    cntyLIST = conn.execute("SELECT Cnty_Name_UC FROM [KYTCVector_Reference].[dbo].[LOOKUP_COUNTY]").fetchall()

    for c in cntyLIST:
        # generate files for all tenth milepoints
        whereCLAUSE = "WHERE COUNTY = '" + c[0] + "'"
        outFILE = r'D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs\\' + c[0] + '_Milepoint_POI_All.csv'
        query = "SELECT * FROM [KYTCVectorLoader_HIS].[dbo].[MILEPOINT_POI_ALL]" + whereCLAUSE
        df = pd.read_sql(query, conn)
        print(df.to_string())
        df.to_csv(outFILE, index=False)

        # generate files for even milepoints
        outFILE = r'D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs\\' + c[0] + '_Milepoint_POI_Mile.csv'
        query = "SELECT * FROM [KYTCVectorLoader_HIS].[dbo].[MILEPOINT_POI_MILE]" + whereCLAUSE
        df = pd.read_sql(query,conn)
        print(df.to_string())
        df.to_csv(outFILE, index=False)

        # generate files for 1/2 milepoints
        outFILE = r'D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs\\'+ c[0] + '_Milepoint_POI_HalfMile.csv'
        query = "SELECT * FROM [KYTCVectorLoader_HIS].[dbo].[MILEPOINT_POI_HALFMILE]" + whereCLAUSE
        df = pd.read_sql(query, conn)
        print(df.to_string())
        df.to_csv(outFILE, index=False)

        # generate files for turnaround milepoints
        outFILE = r'D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs\\' + c[0] + '_Milepoint_POI_Crossovers.csv'
        query = "SELECT * FROM [KYTCVectorLoader_HIS].[dbo].[MILEPOINT_POI_CROSSOVERS]" + whereCLAUSE
        df = pd.read_sql(query, conn)
        print(df.to_string())
        df.to_csv(outFILE, index=False)

        # 6. Zip county files
        os.chdir(csvDIR)
        zF = zipfile.ZipFile(c[0] + '_Milepoint_POIs.zip', mode='w')
        zF.write(c[0] + '_Milepoint_POI_All.csv')
        zF.write(c[0] + '_Milepoint_POI_Mile.csv')
        zF.write(c[0] + '_Milepoint_POI_HalfMile.csv')
        zF.write(c[0] + '_Milepoint_POI_Crossovers.csv')
        zF.close()
        for root, dirs, files in os.walk(csvDIR):
            for currentFile in files:
                if currentFile.lower().endswith('.csv'):
                    os.remove(os.path.join(root, currentFile))
        print(c[0] + " County completed.")
except:
    print("Process failed.  Email sent.")
    # 7. Send email of completion
    subjectTEXT = "Tenth Milepoint POI Extract Process FAILED."
    bodyText="\n\n============================================================================= \
          \n\nThe weekly extract of 10th Milepoint CSV POI files has failed to complete. \
        \n\nReview data exports in D:\GIS_Update_Process\KYTCVectorLoader_HIS\Data\Milepoint_POIs folder on PROD 01A \
        \n\nand connections for possible triggers. \
        \n\n============================================================================= "
    sendEmail(subjectTEXT, bodyText)
print("EOF - Process completed.")
