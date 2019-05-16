#! /usr/bin/python3.5

'''
Process         : Python Template For Database Accesses.
Author          : Suganth Mohan

'''


# >>>>>>>>>>>>>>>>>>>>>>> IMPORT STATEMENTS <<<<<<<<<<<<<<<<<<<<<<<<<<<<
# USED TO PERFORM SYSTEM FUNCTIONS
import sys , os as linux

# TO READ ZIP FILES
import zipfile as zippy

# USED FOR EXCEPTION HANDLING AND BRIEFING
import linecache

# USED IN LOGGER FOR DETAILED PRINT STATEMENT
import inspect

# USED TO RETRIEVE PARSED ARGUMENTS
import argparse

# REGEX COMPARE
import re as Regex

# READ CSV INPUT FILE
import pandas as pd

# USED TO GET MySQLDB Data
import MySQLdb

# USED FOR SENDING EMAIL
import smtplib

# USED FOR DATE TIME MANIP
import datetime

# USED FOR XML FILE PARSING
import xml.etree.ElementTree as ET

# USED TO RAISE WARNINGS, INSTEAD OF ERRORS
import warnings

#USED TO CREATE A DICTIONARY WITH ITS ORDER MAINTAINED
from collections import OrderedDict 

# USED FOR RECORDING BASH OUTPUT
import subprocess

# USED FOR MULTIPROCESSING
import multiprocessing


# TO IGNORE THE MYSQL WARNINGS
warnings.filterwarnings('ignore', category=MySQLdb.Warning)



# >>>>>>>>>>>>>>>>>>>>>>> INITIALIZE GLOBAL VARIABLES <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def globalSet():

    global now
    now                 = datetime.datetime.now()

    global From,To,mysql_host_name,mysql_user_name,mysql_password,mysql_database,query

    From                =  ""
    To                  = [""]

    mysql_host_name     = ''
    mysql_user_name     = ''
    mysql_password      = ''
    mysql_database      = ''

    query = {}

    query['']   = ""


    # USE PERCENTAGE IN THE LITERAL SENSE AS SUCH ,
    # 60% => 60/100 => 0.6
    global TotalCoresUsage

    TotalCoresUsage = 70/100 # A.K.A 70%


    

# >>>>>>>>>>>>>>>>>>>>>>> USAGE DEMO <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def demo():

    print("""

    USAGE :

    python35 """+linux.path.basename(sys.argv[0])+""" --Arg1=Arg1Value {optional} --Arg2=Arg2Value {required} --Arg3=Arg3Value {required} 

    """)

    sys.exit(0)

if not len(sys.argv) > 1 :
    demo()


# >>>>>>>>>>>>>>>>>>>>>>> ARGUMENT PARSING <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def args():

    parser = argparse.ArgumentParser()

    parser.add_argument('-A','--Arg1',type=int,help='I am Argument 1',required=False)
    parser.add_argument('-B','--Arg2',type=str,help='I am Argument 2',required=True)
    parser.add_argument('-C','--Arg3',type=str,help='I am Argument 3',required=True)


    args = parser.parse_args()

    Tlog("PARSING ARGUMENTS COMPLETED")

    return args.Arg1,args.Arg2,args.Arg3

# >>>>>>>>>>>>>>>>>>>>>>> EMAIL MODULE USED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
# ADD 2 ARGUMENTS
#       First  One - Subject of Email to Send
#       Second one - Content of the Email to Send.

def Email(Subject_,Content_):

    SERVER = "localhost"

    # Prepare actual message
    message = 'From: {}\nTo: {}\nSubject: {}\n\n{}'.format(From," ,".join(To),Subject_, Content_)

    # Send the mail
    server = smtplib.SMTP(SERVER)
    server.sendmail(From, To, message)
    server.quit()



# >>>>>>>>>>>>>>>>>>>>>>> EXCEPTION BRIEFER USED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def ExceptionBrief():

    # CREATE EXCEPTION REPORT
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    return 'EXCEPTION CAPTURED : ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)




# >>>>>>>>>>>>>>>>>>>>>>> DEFINE THE USED SUB-ROUTINES <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def Tlog(printer_):

    # GET THE CURRENT TIME OF NOW
    now = datetime.datetime.now()

    print("\n\t[INFO] ::"+str(now).split('.')[0]+"::"+str(__file__)+"::"+str(inspect.currentframe().f_back.f_lineno)+"::"+str(printer_)+"\n")



# >>>>>>>>>>>>>>>>>>>>>>> CREATE A CONNECTION TO THE DATABASE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbConnect():

        global mysql_host_name,mysql_user_name,mysql_password,mysql_database
    
        Database = MySQLdb.connect(host=mysql_host_name, user=mysql_user_name, passwd=mysql_password, db=mysql_database)

        Tlog( "Connected to Database :: " + mysql_database )

        return Database


# >>>>>>>>>>>>>>>>>>>>>>> CLOSE THE CONNECTION TO THE DATABASE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbDisConnect(database_):

        database_.close()

        Tlog( "Disconnected to Database :: " + mysql_database )



# >>>>>>>>>>>>>>>>>>>>>>> FIND THE CORRECT LAYOUT FOR THE ZIP FILE USED <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def CommitChanges(database_):

        database_.commit()

        Tlog( "Committed Changes to Database :: " + mysql_database )


# >>>>>>>>>>>>>>>>>>>>>>> UNZIP THE FILE AND RETURN THE FILE ARGUMENTS <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def unzipFile(zipFile_):

    # INITIALIZE THE UNZIP COMMAND HERE
    cmd = "unzip -o " + zipFile_ + " -d "+outputDir

    Tlog("UNZIPPING FILE "+zipFile_)

    # GET THE PROCESS OUTPUT AND PIPE IT TO VARIABLE
    log = subprocess.Popen(cmd.split(' '),stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # GET BOTH THE ERROR LOG AND OUTPUT LOG FOR IT
    stdout,stderr = log.communicate()

    # FORMAT THE OUTPUT
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')

    if stderr != "" :
        Tlog("ERROR WHILE UNZIPPING FILE \n\n\t"+stderr+'\n')
        sys.exit(0)

    # INITIALIZE THE TOTAL UNZIPPED ITEMS
    unzipped_items = []

    # DECODE THE STDOUT TO 'UTF-8' FORMAT AND PARSE LINE BY LINE
    for line in stdout.split('\n'):

        # CHECK IF THE LINE CONTAINS KEYWORD 'inflating'
        if Regex.search(r"inflating",line) is not None:

            # FIND ALL THE MATCHED STRING WITH REGEX
            Matched = Regex.findall(r"inflating: "+outputDir+"(.*)",line)[0]

            # SUBSTITUTE THE OUTPUT BY REMOVING BEGIN/END WHITESPACES
            Matched = Regex.sub('^\s+|\s+$','',Matched)

            # APPEND THE OUTPUTS TO LIST
            unzipped_items.append(outputDir+Matched)



    # RETURN THE OUTPUT
    return unzipped_items






# >>>>>>>>>>>>>>>>>>>>>>> PREPARE SQL RAW QUERIES WITH VALUES <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def sqlPrepare(Query_,*Inputs_):

        # CHECK IF VALUES MATCH REPLACE STRING BEFORE EXECUTION
        if Query_.count('?') != len(Inputs_):
            warnings.warn("NUMBER OF INPUTS != NUMBER OF ENTRIES('?') IN QUERY")

        for value in Inputs_:
                Query_=Query_.replace('?',value,1)

        return Query_






# >>>>>>>>>>>>>>>>>>>>>>> CREATE THE CALL STATEMENT FOR THE PROCEDURE VALUE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def generateProcedureCall(Layout_,ProcedureName_,DatabaseName_,MappedDict_):


    # CHECK IF DATABASE NAME IS PROVIDED
    if not DatabaseName_:

        Tlog("[ERROR] DATABASE NAME IS EMPTY DATABASE_NAME : ("+DatabaseName_+")")

        sys.exit(0)



    # CHECK IF PROCEDURE NAME IS PROVIDED
    if not ProcedureName_:

        Tlog("[ERROR] PROCEDURE NAME IS EMPTY PROCEDURE_NAME : ("+ProcedureName_+")")

        sys.exit(0)



    # CHECK IF DATAMAP IS EMPTY, THEN WARN
    if not MappedDict_:
        
        warnings.warn("[ERROR] DATAMAP IS EMPTY")



    Query = "CALL " + DatabaseName_ + "." + ProcedureName_ + "("


    # ITERATE THROUGHT THE LAYOUTS
    for Field_Name in Layout_:
       
        # CHANGE TYPE TO null IF THE VALUE IS NULL/EMPTY OR NOT DEFINED
        if not MappedDict_[Field_Name] or MappedDict_[Field_Name] == "" or  MappedDict_[Field_Name] == 'null':

            # ADD IT AS AN NORMAL VALUE
            Query+= " " + "null"  +" ,"

        elif Regex.search(r"nb_return_value",Field_Name,Regex.IGNORECASE) is not None:

            # SET THE OUTPUT PARAMETER HERE
            Query+= " " + "@" + str(MappedDict_[Field_Name]) + " ,"

        else:

            # ADD IT AS AN NULL VALUE
            Query+= "'"+str(MappedDict_[Field_Name])+"' ,"
    
    Query = Regex.sub(",$",')',Query)

    return Query


# >>>>>>>>>>>>>>>>>>>>>>> GET THE PROCEDURE IN ARGUMENTS <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def getProcedureMetaData(Connection_,Procedure_,DatabaseName_):

    global query

    # CREATE A CURSOR FOR RETRIEVING THE DATABASE INFORMATION
    my_cursor = Connection_.cursor()

    # PREPARE THE SQL QUERY
    my_query = sqlPrepare(query['BlahBlahBlah'],DatabaseName_,Procedure_)

    # EXECUTE THE QUERY
    my_cursor.execute(my_query)

    # GET THE DATA FROM CURSOR
    datum = my_cursor.fetchall()

    # CLOSE CURSOR TO AVOID THE DEPENDENCY ISSUES
    my_cursor.close()

    # PROCEDURE ORDER MAINTAINED LIST OF ITEMS
    Ordered_Procedure_Params = ()

    # FORMAT THE RETRIEVED PROCEDURE WHICH IS IN BINARY FORMAT
    for item in datum[0][0].decode('utf-8').split('\n'):

            # REMOVE ANY EXCESS STARTING/ENDING SPACES
            item = Regex.sub('^\s+|\s+$','',item)
            
            # CONVERT MANY SPACES TO ONE SPACE
            item = Regex.sub('\s+',' ',item)

            # CONVERTED THE LINE INTO A LIST
            Splitted_List = item.split(' ') 
           
            # CHECK IF IT IS ANY EMPTY/ INAPPROPRIATE CHARACTER PRESENT LIMITING IT TO 2 SIZES
            if len(Splitted_List) >= 3:

                # ASSIGN THE FIELD NAME TO THE VALUE
                Field_Name = Splitted_List[1]

                # STORE THE FIELD NAME INTO THE PROCEDURE ORDERED PARAMS TUPLE
                Ordered_Procedure_Params = Ordered_Procedure_Params + (Field_Name,)

    return Ordered_Procedure_Params



# >>>>>>>>>>>>>>>>>>>>>>> CREATE ORDERED DICTIONARY FOR THE FIELDS <<<<<<<<<<<<<<<<<<<<<<<<<<<<
# Ordered_Fields_ CAN HANDLE => LIST,TUPLES AND KEYS OF ORDERED DICTIONARY
def generateReverseMap(Ordered_Fields_,To_Order_Dictionary_,RecordType_):


    # CREATE AN ORDERED DICTIONARY
    NewDictionary = OrderedDict()
    

    # ITERATE THROUGH EACH FIELD NAME PRESENT IN THE KEYS LIST
    for FieldName in Ordered_Fields_:

        # SKIP IF IT CONTAINS USER DEFINED FIELDS SUCH AS nb_file_name,nb_agency_type
        if FieldName in list(To_Order_Dictionary_[RecordType_]['Fields'].keys()):


            NewDictionary[FieldName] = dict()

            # GET THE TOTAL NUMBER OF REQUIRED KEYS SUCH AS POS, DEC, LEN 
            Required_Fields = To_Order_Dictionary_[RecordType_]['Fields'][FieldName].keys()

            for Required_Field in Required_Fields:


                NewDictionary[FieldName][Required_Field] = To_Order_Dictionary_[RecordType_]['Fields'][FieldName][Required_Field]
        else:

            NewDictionary[FieldName] = dict()

            # CREATE DUMMY FIELD VALUE FOR FIELDS NOT PRESENT IN LAYOUT
            NewDictionary[FieldName]['Pos'] = 'null'

    To_Order_Dictionary_[RecordType_]['Fields'] = NewDictionary

    return To_Order_Dictionary_


# >>>>>>>>>>>>>>>>>>>>>>> USED TO GET THE TOTAL LINES IN THE FILE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def get_file_len(filePath_):

    with open(filePath_) as FileHandle:
        for FileCount, Dummy in enumerate(FileHandle):
            pass

    return FileCount


# >>>>>>>>>>>>>>>>>>>>>>> USED TO SPLIT THE FILE AND PROCESS WITH THE SPLITTED COUNT <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def splitAndProcess(FilesList_):



    # ITERATE THROUGHT FILES LIST
    for readFile in FilesList_:


        # CREATE A COMBINED PARSER
        CombinedDataParser = []


        # SINCE ONLY SINGLE ARGUMENT CAN BE PARSED FOR MULTI-PROCESSING, IT IS PARSED AS A DICTIONARY
        DataParserArgs  =   {
                            'inputFile'         : readFile,
                            'convertedSuffix'   : convertedSuffix,
                            'Layout'            : Layout_Dictionary 
                            }
        
        # THEN APPEND THE DICTIONARY TO THE LIST
        CombinedDataParser.append(DataParserArgs)


        # LIMIT THE TOTAL CPU USAGE BASED ON THE TOTAL CORES AVAILABLE AND THEN ALLOCATING THOSE CORES FOR THAT PROCESS
        # I HAVE SET THE LIMIT TO TOTAL CPU USAGE : 70 PERCENTAGE (NOTE : NOT EACH CORE) OF THE CPU SO THAT THE PROCESS DOES NOT GET AFFECTED 

        TotalProcessCount = round( multiprocessing.cpu_count() * TotalCoresUsage )


        # INITIATE EACH PROCESS COUNT
        with multiprocessing.Pool(TotalProcessCount) as process:
            
            # GIVE THE FIRST ARGUMENT OF FUNCTION NAME AND SECOND ARGUMENT AS LIST OF INPUTS
            process.map(DataParsing,CombinedDataParser)



def DataParsing(DataParserArgs):


        
        # GET THE ARGUMENTS FROM FUNCTION
        inputFile_      = DataParserArgs['inputFile']
        suffix_         = DataParserArgs['convertedSuffix']
        Layout_         = DataParserArgs['Layout']





        # GET THE FILE NAME SUFFIX
        logFileSuffix = linux.path.basename(inputFile_)

        logFileSuffix = logFileSuffix[ 0 : logFileSuffix.find('.') ]

        # CREATE LOG FILE NAME
        logFileName    = logDir + logFileSuffix + "_" + FileDate.replace('-','') + "_" + suffix_ + ".log"

        # CREATE FILE HANDLE FOR THE LOG FILE
        LogFileHandle = open(logFileName,'w')






        # CREATE ERROR LOG FILE NAME
        ErrorLogFileName = logDir + logFileSuffix + "_" + FileDate.replace('-','') + "_errors_" + suffix_ + ".log"


        # CREATE ERROR LOG FILE HANDLE
        ErrorLogFileHandle = open(ErrorLogFileName,'w')

        # WRITE HEADER FORMATTING FOR THE ERRORS OCCURRED
        ErrorLogFileHandle.write("QUERY ERROR REASON|CALL STATEMENT USED\n")





        # CREATE DATABASE CONNECTION HERE
        Database = DbConnect()

        # CREATE THE CURSOR HERE
        Database_Cursor = Database.cursor()


        # CREATE FILE HANDLE TO HANDLE THE INPUTS
        inputFileHandle = open(inputFile_+suffix_)

        # ITERATE OVER THE INPUT FILES
        for LineNo,Line in enumerate(inputFileHandle.readlines()):

            print("Line Number ::: ",LineNo)
            
            # PARSE AND GET RECORD TYPE HERE
            Record_Type  = Line.split('|')[0]

            DataMap = {}

            for FieldName in Layout_Dictionary[Record_Type]['Fields']:

                    # FORMAT THE STRING HERE
                    Value    = Line.split('|')[Position]

                    # CONVERT MULTIPLE SPACES TO ONE SPACE
                    Value    = Regex.sub('\s+',' ',Value)

                    # CHECK IF CONTAINS ONLY EMPTY SPACE, ADD IT AS NULL
                    if Value == ' ' or Value is None or Value == "" :
                        Value = 'null'
                
                    # REMOVE UNNECESSARY STARTING AND ENDING SPACES
                    Value    = Regex.sub('^\s+|\s+$','',Value)

                    # ADD ESCAPE SEQUENCE FOR VALUES WITH apostrophe (')
                    Value    = Value.replace("'","\\'")

                    # ADD ESCAPE SEQUENCE FOR VALUES WITH comma (,)
                    Value    = Value.replace(",","\\,")

                    # FINALLY ASSIGN THE VALUE AFTER THE CHECKS
                    DataMap[FieldName] = Value


            Query = generateProcedureCall(Layout_ = Layout_Dictionary[Record_Type]['Fields'],ProcedureName_ = Layout_Dictionary[Record_Type]['Procedure'] ,DatabaseName_ = mysql_database,MappedDict_ = DataMap)

            # WRITE QUERY TO LOG
            LogFileHandle.write("\nLine Number ::: "+str(LineNo)+"\n")
            LogFileHandle.write(Query+"\n")

            # SET AUTOCOMMIT TO TRUE FOR FASTER PROCESSING
            Database.autocommit(True)

            # EXECUTE THE QUERY IN DATABASE ONLY IF THE EXECUTE MODE IS SET TO 1
            if ExecuteMode == 1:

                # USE TRY AND CATCH BLOCK COMMAND TO STOP THE PYTHON PROGRAM FROM EXITTING

                # SET THE PROGRAM TO EXECUTE THE COMMAND AND CHECK FOR ERRORS
                try :

                    # IF THE CURSOR EXECUTE IS TRUE
                    Database_Cursor.execute(Query)

                except MySQLdb.Error as mysql_error:

                    # WRITE THE ERROR QUERY TO ERROR LOGS FOR POST-PROCESSING REFERENCE
                    # FORMAT :: QUERY ERROR REASON|CALL STATEMENT
                    print("[ERROR] : "+str(mysql_error))
                    ErrorLogFileHandle.write(str(mysql_error)+"|"+Query+"\n")

    
                    
        Database_Cursor.close()

        # CLOSE THE INPUT FILE HANDLE
        inputFileHandle.close()

        # CLOSE THE LOG FILE HANDLE
        LogFileHandle.close()

        # CLOSE THE ERROR LOG FILE HANDLE
        ErrorLogFileHandle.close()

        # CLOSE THE DATABASE CONNECTION
        DbDisConnect(database_ = Database)



def main():

    # GET INITIAL ARGUMENTS NECESSARY
    Arg1,Arg2,Arg3=args()

    # INITIALIZE
    globalSet()

    # UNZIP THE FILE HERE
    ListOfUnzippedFiles = unzipFile(zipFile_ = Arg3)

    # CREATING A CONNECTION TO THE DATABASE
    DbConnection = DbConnect()

    # CREATES THE LIST OF PARAMETERS PARSED INTO PROCEDURE WHILE MAINTAINING THE ORDER.
    Ordered_Procedure_Params = getProcedureMetaData(Connection_ =  DbConnection, Procedure_ = Layout_Dictionary[RecordType]['Procedure'],DatabaseName_ = mysql_database)


    Layout_Dictionary = generateReverseMap(Ordered_Fields_ = Ordered_Procedure_Params, To_Order_Dictionary_ = Layout_Dictionary, RecordType_ = RecordType )
 
    DbDisConnect(database_ = DbConnection)


    # PARSE DATA HERE
    splitAndProcess(FilesList_ = ListOfUnzippedFiles)


# >>>>>>>>>>>>>>>>>>>>>>> DECLARE THE MAIN FUNCTION HERE  <<<<<<<<<<<<<<<<<<<<<<<<<<<<
if __name__=="__main__":
    main()
