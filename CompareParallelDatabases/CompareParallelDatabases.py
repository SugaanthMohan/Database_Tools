#! /usr/bin/python

'''
Process                     :   COMPARE SIMILAR TABLES IN DATABASES
Author                      :   Suganth Mohan
Created                     :   Oct 19 2018
Last Modified Date          :   - 

Version Control:
_______________

pandas      ==  0.23.0
smtplib     ==  Default Package : Python 2.7.12
sys         ==  Default Package : Python 2.7.12
os          ==  Default Package : Python 2.7.12
linecache   ==  Default Package : Python 2.7.12
inspect     ==  Default Package : Python 2.7.12
argparse    ==  Default Package : Python 2.7.12
re          ==  Default Package : Python 2.7.12
ftplib      ==  Default Package : Python 2.7.12
datetime    ==  Default Package : Python 2.7.12

'''

# >>>>>>>>>>>>>>>>>>>>>>> IMPORT STATEMENTS <<<<<<<<<<<<<<<<<<<<<<<<<<<<
# USED FOR SENDING EMAIL
import smtplib

# USED TO PERFORM SYSTEM FUNCTIONS
import sys , os as linux

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

# USED FOR LOGGING DATE
import datetime

# USED TO GET DB DATA
import MySQLdb

# USED TO GET DATA FROM CONFIG FILE
import configparser

# USED TO INITIATE GARBAGE COLLECTION
import gc

# USED FOR GARBAGE COLLECTION TIME WAIT
from time import sleep

# >>>>>>>>>>>>>>>>>>>>>>> INITIALIZE GLOBAL VARIABLES <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def globalSet():


        # EMAILING LIST
        global From,To,now,SaveLocation,SchemaLocation

        From            =  "CompareParallelDatabases@gmail.com"
        To              = ["<YourEmailHere@YourMailServicer.com>"]
        now             = datetime.datetime.now()
        SaveLocation    = "/tmp/SaveFiles/"
        SchemaLocation  = "<SavedSchemaLocations>"



        # DATABASE 1 LIST
        global GlobDbObj_1,mysql_host_name_1,mysql_user_name_1,mysql_password_1,mysql_database_1,mysql_port_number_1

        mysql_host_name_1 = mysql_user_name_1 = mysql_password_1  = mysql_database_1  =  ""

        GlobDbObj_1     = []


    
        # DATABASE 2 LIST
            
        global GlobDbObj_2,mysql_host_name_2,mysql_user_name_2,mysql_password_2,mysql_database_2,mysql_port_number_2

        mysql_host_name_2 = mysql_user_name_2 = mysql_password_2  = mysql_database_2  =  ""

        GlobDbObj_2     = []
        




        # GET OPTIONS
        global DifferedTableNames,OneToMany,Override,Test_comp,Performance

        # SET DEFAULT VALUE OF OPTIONS TO False
        DifferedTableNames=OneToMany=Override=Test_comp=False



        # QUERY LIST

        global query

        query = {} 

        # ORIGINAL QUERIES TO USE

        query['GET_TABLES_IN_DATABASE']     =   "SHOW TABLES IN ? "

        query['DESC_TABLE']                 =   "DESC ? "

        query['SELECT_TABLE']               =   "SELECT * FROM ? "

        query['SELECT_TABLE_LIMIT']         =   "SELECT * FROM ? LIMIT ?,?"

        query['CHECK_COUNT']                =   "SELECT COUNT(*) FROM ? "



        # TEST QUERY COMPATIBILITY FOR THE DATABASE


        query['TEST_VERSION']               =   """
                                                SELECT VERSION();
                                                """

        query['TEST_CREATE_TABLE']          =   """
						CREATE TEMPORARY TABLE TableCompareTest (
						    item_name VARCHAR(50),
						    sort_num INT
						);
                                                """

        query['TEST_DESC']                  =   """
                                                DESC TableCompareTest;
                                                """

        query['TEST_INSERT']                =   """
                                                INSERT INTO TableCompareTest
							(item_name,sort_num)
						VALUES
							("Temp1",10),
							("Temp2",20),
							("Temp3",30);
                                                """

        query['TEST_SELECT']                =   """
                                                SELECT * FROM TableCompareTest;
                                                """

        query['TEST_DROP']                  =   """
                                                DROP TEMPORARY TABLE TableCompareTest;
                                                """


        global ErrorsList


        ErrorsList = []


# >>>>>>>>>>>>>>>>>>>>>>> USAGE DEMO <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def demo():

        print("""

        USAGE :

        python35 """+linux.path.abspath(sys.argv[0])+""" --configFile="""+linux.path.abspath(sys.argv[0]).replace("py","ini")+"""

        SAMPLE :

        python35 """+linux.path.abspath(sys.argv[0])+""" --configFile="""+linux.path.abspath(sys.argv[0]).replace("py","ini")+"""

        """)

        sys.exit(0)



if not len(sys.argv) > 1 :
       demo()





# >>>>>>>>>>>>>>>>>>>>>>> ARGUMENT PARSING <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def Clear_Memory():

        gc.collect()

        Tlog('Memory Allocation Freeing.. Please wait 10 seconds. ')

        sleep(9)



# >>>>>>>>>>>>>>>>>>>>>>> ARGUMENT PARSING <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def args():

        parser = argparse.ArgumentParser()

        parser.add_argument('-c','--configFile',type=str,help="FORMAT : INI\nCONTAINS ALL REQUIRED SCHEMA DETAILS",required=True)

        args = parser.parse_args()

        Tlog("PARSING ARGUMENTS COMPLETED")

        return args.configFile


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

     now = datetime.datetime.now()

     print("\n\t[INFO] ::"+str(now).split('.')[0]+"::"+str(__file__)+"::"+str(inspect.currentframe().f_back.f_lineno)+"::"+str(printer_)+"::\n")




# >>>>>>>>>>>>>>>>>>>>>>> MYDIE MODULE USED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def mydie(exitCont_):

        Tlog("*** ERROR OCCURRED *** : ROLL BACK PROCEDURES EXECUTING BELOW")

        Tlog(exitCont_)

        Tlog("*** ROLL BACK *** : CLOSING DB CONNECTION 1")

        DbDisConnect1()

        Tlog("*** ROLL BACK *** : CLOSING DB CONNECTION 2 ")

        DbDisConnect2()

        Email(Subject_ =  __file__+" - RUN TIME ERROR AT : "+str(now), Content_ = exitCont_)

        sys.exit(0)


# >>>>>>>>>>>>>>>>>>>>>>> DATABASE CONNECTION 1 CREATED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbConnect1():
        """
        DB 1 CONNECTION IS HERE
        """

        if len(mysql_port_number_1) == 0: 
            Database = MySQLdb.connect(host=mysql_host_name_1, user=mysql_user_name_1, passwd=mysql_password_1, db=mysql_database_1)
        else:
            Database = MySQLdb.connect(host=mysql_host_name_1, user=mysql_user_name_1, passwd=mysql_password_1, db=mysql_database_1,port=int(mysql_port_number_2))

        
        Tlog("Connected to Database : "+mysql_database_1+" @"+mysql_host_name_1)

        GlobDbObj_1.append(Database)

        return Database


# >>>>>>>>>>>>>>>>>>>>>>> DATABASE CONNECTION 2 CREATED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbConnect2():
        """
        DB 2 CONNECTION IS HERE
        """

        if len(mysql_port_number_2) == 0:
            Database = MySQLdb.connect(host=mysql_host_name_2, user=mysql_user_name_2, passwd=mysql_password_2, db=mysql_database_2)
        else:
            Database = MySQLdb.connect(host=mysql_host_name_2, user=mysql_user_name_2, passwd=mysql_password_2, db=mysql_database_2,port=int(mysql_port_number_2))


        Tlog("Connected to Database : "+mysql_database_2+" @"+mysql_host_name_2)

        GlobDbObj_2.append(Database)

        return Database


# >>>>>>>>>>>>>>>>>>>>>>> DATABASE DIS-CONNECTION 1 CREATED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbDisConnect1():
        """
        DB 1 DIS-CONNECTION IS HERE
        """

        for database in GlobDbObj_1:
                database.close()
                Tlog("Disconnected from Database : "+mysql_database_1+" @"+mysql_host_name_1)
                GlobDbObj_1.remove(database)




# >>>>>>>>>>>>>>>>>>>>>>> DATABASE DIS-CONNECTION 2 CREATED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbDisConnect2():
        """
        DB 2 DIS-CONNECTION IS HERE
        """

        for database in GlobDbObj_2:
                database.close()
                Tlog("Disconnected from Database : "+mysql_database_2+" @"+mysql_host_name_2)
                GlobDbObj_2.remove(database)



# >>>>>>>>>>>>>>>>>>>>>>> EXECUTE QUERIES HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def sqlCursorExecute(DbObj_,Query):
    
    # CREATE A CURSOR
    cursor=DbObj_.cursor() or mydie("\t[Error] Unable to Create Cursor\n")

    Tlog("[INFO] Executing Query : "+str(Query)+"")

    # EXECUTE THE SELECT QUERY

    cursor.execute(Query) 

    if cursor.rowcount != 0 :

        data = cursor.fetchall()
        cursor.close()
        return  data

        # CLOSE CURSOR TO AVOID DEPENDENCY ISSUES
        

    else:
        # CLOSE CURSOR TO AVOID DEPENDENCY ISSUES
        cursor.close()




# >>>>>>>>>>>>>>>>>>>>>>> EXECUTE THE TEST QUERIES HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def sqlDataFrameExecute(DbObj,Query):
    
    df = pd.read_sql(Query, con=DbObj)

    return df

# >>>>>>>>>>>>>>>>>>>>>>> DATABASE DIS-CONNECTION 2 CREATED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def TestQueryStructure(connection_):
    """
    USED TO TEST TEMPORARY QUERIES TO CHECK THE SYNTAX.

    TestQueryStructure(connection_)

    connection_ => Is the live connection to database 
    """
    
    # DO VERSION SELECT
    sqlCursorExecute(DbObj_ = connection_,Query = query['TEST_VERSION'])
    Tlog(' SELECT VERSION -> OK ')

    # CREATE A TEMPORARY TABLE
    sqlCursorExecute(DbObj_ = connection_,Query = query['TEST_CREATE_TABLE'])
    Tlog(' CREATE TABLE QUERY -> OK ')

    # DESCRIBE THE TEMPORARY TABLE
    sqlCursorExecute(DbObj_ = connection_,Query = query['TEST_DESC'])
    Tlog(' DESCRIBE TABLE QUERY -> OK')

    # INSERT DATA TO THE TEMPORARY TABLE
    sqlCursorExecute(DbObj_ = connection_,Query = query['TEST_INSERT'])
    Tlog(' INSERT INTO TABLE QUERY -> OK')

    # SELECT DATA FROM THE TEMPORARY TABLE
    sqlCursorExecute(DbObj_ = connection_,Query = query['TEST_SELECT'])
    Tlog(' SELECT FROM TABLE QUERY -> OK')

    # DROP THE TEMPORARY TABLE
    sqlCursorExecute(DbObj_ = connection_,Query = query['TEST_DROP'])
    Tlog(' DROP TABLE QUERY -> OK')




# >>>>>>>>>>>>>>>>>>>>>>> EXECUTE THE TEST QUERIES HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def prepareQuery(*values,BaseQuery_):
    
    # REPLACE VALUES IN THE BASE QUERY
    for value in values:
        BaseQuery_ = BaseQuery_.replace('?',str(value),1)

    return BaseQuery_


# >>>>>>>>>>>>>>>>>>>>>>> GET THE LIST OF PARAMETERS HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def get_Config_Params(ini_):
        """
        PARSE THE CONFIG FILE HERE :

        get_Config_Params(ini_)
        ini_    =>  is the file name along with location to be parsed

        """

        global mysql_host_name_1,mysql_user_name_1,mysql_password_1,mysql_database_1,mysql_port_number_1
        global mysql_host_name_2,mysql_user_name_2,mysql_password_2,mysql_database_2,mysql_port_number_2
        global DifferedTableNames,OneToMany,Override,Test_comp,Performance
        
        # CHECK IF THE FILE EXISTS
        if linux.path.isfile(ini_) is not True:
            mydie("Config File : "+ini_+" does not exist!")


        getconfig           = configparser.ConfigParser()
        getconfig.read(ini_)
        getConfigInfo       = getconfig['Configs']

        mysql_host_name_1   = getConfigInfo['HOST_NAME_1']
        mysql_user_name_1   = getConfigInfo['USER_NAME_1']
        mysql_password_1    = getConfigInfo['PASSWORD_1']
        mysql_database_1    = getConfigInfo['DATABASE_NAME_1']
        mysql_host_name_2   = getConfigInfo['HOST_NAME_2']
        mysql_user_name_2   = getConfigInfo['USER_NAME_2']
        mysql_password_2    = getConfigInfo['PASSWORD_2']
        mysql_database_2    = getConfigInfo['DATABASE_NAME_2']

        if getconfig.has_option('Configs','PORT_NUMBER_1'):
            mysql_port_number_1 = getConfigInfo['PORT_NUMBER_1']

        if getconfig.has_option('Configs','PORT_NUMBER_2'):
            mysql_port_number_2 = getConfigInfo['PORT_NUMBER_2']


        # GET THE OPTIONS INFO
        DifferedTableNames  = eval(getConfigInfo['DIFFERED_TABLE_NAMES'])
        OneToMany           = eval(getConfigInfo['ONE_TO_MANY'])
        Override            = eval(getConfigInfo['OVERRIDE'])
        Test_comp           = eval(getConfigInfo['TEST_COMPATIBILITY'])
        Performance         = int(getConfigInfo['PERFORMANCE_LIMIT'])




## TODO 
# >>>>>>>>>>>>>>>>>>>>>>> MATCH ALL THE TABLES BELOW <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def MatchAllTables(dataframe1_, dataframe2_,connection1_,connection2_):
    
    pass









# >>>>>>>>>>>>>>>>>>>>>>> MATCH ALL THE TABLES BELOW <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def CompareTableContent(DataFrame_,connection1_,connection2_):
    
    global Performance,ErrorsList

    for TableDetails in DataFrame_.values:
        
        firstTable  = TableDetails[0].split('|')[0]
        secondTable = TableDetails[0].split('|')[1]

        Tlog("COMPARING TABLE CONTENTS BETWEEN : " + firstTable + " <=> " + secondTable)

        QueryNow = prepareQuery(firstTable,BaseQuery_ = query['CHECK_COUNT'])
    
        count1 = int(sqlCursorExecute(DbObj_ = connection1_ ,Query = QueryNow)[0][0])

        QueryNow = prepareQuery(secondTable,BaseQuery_ = query['CHECK_COUNT'])
        
        count2 = int(sqlCursorExecute(DbObj_ = connection2_ ,Query = QueryNow)[0][0])

        Tlog(firstTable + " : " + str(count1) +" | "+ secondTable + " : " + str(count2))




        # LOAD SCHEMA IF TABLE IS EMPTY
        if linux.path.exists(SchemaLocation + mysql_database_1 + "_" + firstTable + "_schema.csv" ) is True :

            table_features = pd.read_csv(SchemaLocation + mysql_database_1 + "_" + firstTable + "_schema.csv")
            
        else:

            QueryNow = prepareQuery(firstTable,BaseQuery_ = query['DESC_TABLE'])
        
            table_features = pd.read_sql(QueryNow, con=connection1_)                    
        
        # DO LISTINGS AGAIN 
        Table_Keys_List     = table_features[ (table_features['Key'] != '') & (table_features['Type'] != 'timestamp') ]['Field'].values.tolist()
        

        Table_Not_Keys_List = table_features[ (table_features['Key'] == '') & (table_features['Type'] != 'timestamp') ]['Field'].values.tolist()

        if len(Table_Keys_List) == 0:

            ErrorsList.append("[ERROR] TABLE " + firstTable + " HAS NO KEYS DEFINED FOR IT. SCHEMA ALSO NOT FOUND !!")


        # CLEAR MEMORY OF TABLE FEATURES LIST
        del table_features




        # LOAD SCHEMA IF TABLE IS EMPTY
        if linux.path.exists(SchemaLocation + mysql_database_2 + "_" + secondTable + "_schema.csv" ) is True :

            table_features = pd.read_csv(SchemaLocation + mysql_database_2 + "_" + secondTable + "_schema.csv")
            
        else:

            QueryNow = prepareQuery(secondTable,BaseQuery_ = query['DESC_TABLE'])
        
            table_features = pd.read_sql(QueryNow, con=connection2_)                    
        
        # DO LISTINGS AGAIN 
        Table_Keys_List     = table_features[ (table_features['Key'] != '') & (table_features['Type'] != 'timestamp') ]['Field'].values.tolist()
        

        Table_Not_Keys_List = table_features[ (table_features['Key'] == '') & (table_features['Type'] != 'timestamp') ]['Field'].values.tolist()

        if len(Table_Keys_List) == 0:

            ErrorsList.append("[ERROR] TABLE " + firstTable + " HAS NO KEYS DEFINED FOR IT. SCHEMA ALSO NOT FOUND !!")


        # CLEAR MEMORY OF TABLE FEATURES LIST
        del table_features



        if count1 == count2:

            if count1 == 0:

                Tlog("[WARNING] BOTH TABLES ARE EMPTY Table 1 : "+ firstTable +"  AND Table 2 : " + secondTable)
                
                continue

            if count1 > Performance :

                Tlog(" [WARNING] : COUNT EXCEEDS PERFORMANCE LIMIT FOR TABLES : "+firstTable+" VS "+secondTable+" !")
                Tlog(" USING DIVIDE AND CONQUER FOR TABLES : " + firstTable + " VS " + secondTable + " ! ")

                # USE DIVIDE METHOD TO CHECK AND CONTRIBUTE
                Tlog(" [ WARNING ] : TABLE : "+ firstTable + " HAS HIGHER COUNT THAN PERFORMANCE LIMIT : " + str(count1))

                # CALCULATE THE NUMBER OF ITERATIONS,SPLITTING AND REMAINDER QUERY
                
                Total_Iterations = int(count1/Performance)
                Excess_remain    = count1%Performance

                upper_limit=0
    
                Tlog("TOTAL ITERATIONS NEEDED BASED ON PERFORMANCE IS : "+str(Total_Iterations) )

                for iteration_number in range(0,Total_Iterations+1):

                    if iteration_number > Total_Iterations is not True:
                        upper_limit = Performance * Total_Iterations

                    if ( upper_limit + Excess_remain ) == count1:

                        # GET THE FIRST TABLE FIELD'S DATA
                        QueryNow = prepareQuery(firstTable,upper_limit,Excess_remain,BaseQuery_ = query['SELECT_TABLE_LIMIT'])

                        dataframe_db1 = pd.read_sql(QueryNow, con=connection1_)

                        # PREPARE THE QUERY TO RETRIEVE THE SAME DATA BASED ON THE KEYS

                        QueryNow = prepareQuery(secondTable,BaseQuery_ = query['SELECT_TABLE'])

                        counter = 1

                        for column_name in Table_Keys_List:
                    
                            if counter == 1 :

                                Tlog("PREPARING KEYS FOR THE COLUMN : " + column_name)

                                item = tuple(list(set(dataframe_db1[column_name].apply(str).values.tolist())))
                                
                                counter = counter + 1
                                
                                QueryNow = QueryNow + " WHERE " + column_name + " IN " + str(item).replace("',)","')")
        
                            else :

                                Tlog("PREPARING KEYS FOR THE COLUMN : " + column_name)

                                item = tuple(list(set(dataframe_db1[column_name].apply(str).values.tolist())))

                                counter = counter + 1
                                
                                QueryNow = QueryNow + " AND " + column_name + " IN " + str(item).replace("',)","')")

                        # RETRIEVE THE DATA FROM THE PREPARED QUERY

                        dataframe_db2 = pd.read_sql(QueryNow, con=connection2_)

                        # REMOVE EXCESS REMAIN CREATED BY IN-VALUES QUERY

                        Tlog("REMOVING EXCESS REMAIN PROCESS STARTED")

                        # CREATE FIRST DATASET SUPER KEY
                        dataframe_db1['SUPERKEY'] = dataframe_db1[Table_Keys_List].applymap(str).apply(lambda x: '|'.join(x), axis=1)
                        
                        # CREATE SECOND DATASET SUPER KEY
                        dataframe_db2['SUPERKEY'] = dataframe_db2[Table_Keys_List].applymap(str).apply(lambda x: '|'.join(x), axis=1)

                        dataframe_db2 = dataframe_db2[dataframe_db2['SUPERKEY'].isin(dataframe_db1['SUPERKEY'])]
           
                        dataframe_db1.drop(columns=['SUPERKEY'], axis=1)
                        dataframe_db2.drop(columns=['SUPERKEY'], axis=1)
 
                        Tlog("REMOVING EXCESS REMAIN PROCESS COMPLETED")

                        # DECLARE SUFFIX LIST HERE

                        suffix = ['_table1', '_table2']

                        merged_data_frame = dataframe_db1.merge(dataframe_db2, on=Table_Keys_List, how='outer', copy=False, suffixes=suffix, indicator=True)
    
                        # CLEAR MEMORY OF DIFFERENCED LIST
                        del [[dataframe_db1,dataframe_db2]]

                        # PRINT MISSING TABLE RECORDS
                        
                        if merged_data_frame[ merged_data_frame['_merge'] != 'both' ].empty is not True:

                            print(  merged_data_frame[ merged_data_frame['_merge'] != 'both' ]  )
                            
                            merged_data_frame[ merged_data_frame['_merge'] != 'both' ].to_csv(SaveLocation + "MissingTableRecords_"+mysql_database_1+"_"+firstTable+"VS"+mysql_database_2+"_"+secondTable+"_"+str(upper_limit)+".csv", sep=',', encoding='utf-8', index=False, header=True)

                            ErrorsList.append(" [ERROR] Missing Table Records found for Table 1 : " + firstTable + " : " + str(count1) + " and Table 2 : "+ secondTable + " : " + str(count2) )

                        else:

                            Tlog("No Missing data has been found !!!")
                      
                        if Table_Not_Keys_List :

                            for column_name in Table_Not_Keys_List:

                                Tlog('Comparing data for the column : ' + column_name + ' for Tables : ' +firstTable+' <=> '+ secondTable)

                                if merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() ) ) & (merged_data_frame['_merge'] == 'both') ].empty is True:

                                    Tlog("No Difference in data found for COLUMN : " + str(column_name))
                    
                                else:

                                    ErrorsList.append(" [ERROR] Data Difference found for Table 1 : " + firstTable + " and Table 2 : "+ secondTable + " for column : " + column_name)

                                    print( merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() ) ) & (merged_data_frame['_merge'] == 'both')  ] )

                                    merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() )  )  & (merged_data_frame['_merge'] == 'both') ].to_csv(SaveLocation + "DataDifferences_"+mysql_database_1+"_"+str(column_name)+"_"+firstTable+"VS"+mysql_database_2+"_"+secondTable+"_"+str(upper_limit)+".csv", sep=',', encoding='utf-8', index=False, header=True)

                        else:
                            
                            Tlog("[WARNING] :: TABLE HAS TIMESTAMP OR KEY FIELDS ONLY AND NO DATA FIELDS")

                            ErrorsList.append("[WARNING] :: TABLE HAS TIMESTAMP OR KEY FIELDS ONLY AND NO DATA FIELDS IN COMPARISON for table 1 : " + firstTable + " and Table 2 : "+ secondTable + " for column : " + column_name)
                    

                        # CLEAR DYNAMIC MEMORY  HERE
                        del merged_data_frame

                        # CLEAR USED FOR DATAFRAMES
                        Clear_Memory()
                        
                    else:

                        # GET THE FIRST TABLE FIELD'S DATA
                        QueryNow = prepareQuery(firstTable,upper_limit,Performance,BaseQuery_ = query['SELECT_TABLE_LIMIT'])

                        dataframe_db1 = pd.read_sql(QueryNow, con=connection1_)

                        # PREPARE THE QUERY TO RETRIEVE THE SAME DATA BASED ON THE KEYS

                        QueryNow = prepareQuery(secondTable,BaseQuery_ = query['SELECT_TABLE'])

                        counter = 1

                        for column_name in Table_Keys_List:
                    
                            if counter == 1 :

                                Tlog("PREPARING KEYS FOR THE COLUMN : " + column_name)

                                item = tuple(list(set(dataframe_db1[column_name].apply(str).values.tolist())))
                                
                                counter = counter + 1
                                
                                QueryNow = QueryNow + " WHERE " + column_name + " IN " + str(item).replace("',)","')")
        
                            else :

                                Tlog("PREPARING KEYS FOR THE COLUMN : " + column_name)

                                item = tuple(list(set(dataframe_db1[column_name].apply(str).values.tolist())))

                                counter = counter + 1
                                
                                QueryNow = QueryNow + " AND " + column_name + " IN " + str(item).replace("',)","')")

                        # RETRIEVE THE DATA FROM THE PREPARED QUERY

                        dataframe_db2 = pd.read_sql(QueryNow, con=connection2_)

                        # REMOVE EXCESS REMAIN CREATED BY IN-VALUES QUERY

                        Tlog("REMOVING EXCESS REMAIN PROCESS STARTED")

                        # CREATE FIRST DATASET SUPER KEY
                        dataframe_db1['SUPERKEY'] = dataframe_db1[Table_Keys_List].applymap(str).apply(lambda x: '|'.join(x), axis=1)
                        
                        # CREATE SECOND DATASET SUPER KEY
                        dataframe_db2['SUPERKEY'] = dataframe_db2[Table_Keys_List].applymap(str).apply(lambda x: '|'.join(x), axis=1)

                        dataframe_db2 = dataframe_db2[dataframe_db2['SUPERKEY'].isin(dataframe_db1['SUPERKEY'])]
           
                        dataframe_db1.drop(columns=['SUPERKEY'], axis=1)
                        dataframe_db2.drop(columns=['SUPERKEY'], axis=1)
 
                        Tlog("REMOVING EXCESS REMAIN PROCESS COMPLETED")

                        # DECLARE SUFFIX LIST HERE

                        suffix = ['_table1', '_table2']

                        merged_data_frame = dataframe_db1.merge(dataframe_db2, on=Table_Keys_List, how='outer', copy=False, suffixes=suffix, indicator=True)
    
                        # CLEAR MEMORY OF DIFFERENCED LIST
                        del [[dataframe_db1,dataframe_db2]]

                        # PRINT MISSING TABLE RECORDS
                        
                        if merged_data_frame[ merged_data_frame['_merge'] != 'both' ].empty is not True:

                            print(  merged_data_frame[ merged_data_frame['_merge'] != 'both' ]  )

                            merged_data_frame[ merged_data_frame['_merge'] != 'both' ].to_csv(SaveLocation + "MissingTableRecords_"+mysql_database_1+"_"+firstTable+"VS"+mysql_database_2+"_"+secondTable+"_"+str(iteration_number)+".csv", sep=',', encoding='utf-8', index=False, header=True)

                            ErrorsList.append(" [ERROR] Missing Table Records found for Table 1 : " + firstTable + " : " + str(count1) + " and Table 2 : "+ secondTable + " : " + str(count2) )

                        else:
                            Tlog("No Missing data has been found !!!")
                      
                        if Table_Not_Keys_List :

                            for column_name in Table_Not_Keys_List:

                                Tlog('Comparing data for the column : ' + column_name + ' for Tables : ' +firstTable+' <=> '+ secondTable)

                                if merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() )  )  & (merged_data_frame['_merge'] == 'both') ].empty is True:

                                    Tlog("No Difference in data found for COLUMN : " + str(column_name))
                    
                                else:

                                    ErrorsList.append(" [ERROR] Data Difference found for Table 1 : " + firstTable + " and Table 2 : "+ secondTable + " for column : " + column_name)

                                    print( merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() )  )  & (merged_data_frame['_merge'] == 'both') ] )

                                    merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() )  )  & (merged_data_frame['_merge'] == 'both') ].to_csv(SaveLocation + "DataDifferences_"+mysql_database_1+"_"+str(column_name)+"_"+firstTable+"VS"+mysql_database_2+"_"+secondTable+"_"+str(iteration_number)+".csv", sep=',', encoding='utf-8', index=False, header=True)
                        else:
                            
                            Tlog("[WARNING] :: TABLE HAS TIMESTAMP OR KEY FIELDS ONLY AND NO DATA FIELDS")

                            ErrorsList.append("[WARNING] :: TABLE HAS TIMESTAMP OR KEY FIELDS ONLY AND NO DATA FIELDS IN COMPARISON for table 1 : " + firstTable + " and Table 2 : "+ secondTable + " for column : " + column_name)
                    
                            

                        # CLEAR DYNAMIC MEMORY  HERE
                        del merged_data_frame

                        # CLEAR USED FOR DATAFRAMES
                        Clear_Memory()
                        
            else:
               
                # GET THE FIRST DATABASE TABLE CONTENTS
                
                QueryNow = prepareQuery(firstTable,BaseQuery_ = query['SELECT_TABLE'])
        
                dataframe_db1 = pd.read_sql(QueryNow, con=connection1_)

                QueryNow = prepareQuery(secondTable,BaseQuery_ = query['SELECT_TABLE'])
        
                dataframe_db2 = pd.read_sql(QueryNow, con=connection2_)

                if dataframe_db1.equals(dataframe_db2) is True:
                    
                    Tlog(" Tables are equal : " + firstTable + " <=> " + secondTable)

                else :

                    Tlog(" [ERROR] Tables are not equal : " + firstTable + " <=> " + secondTable)


                    # DECLARE SUFFIX LIST HERE

                    suffix = ['_table1', '_table2']

                    merged_data_frame = dataframe_db1.merge(dataframe_db2, on=Table_Keys_List, how='outer', copy=False, suffixes=suffix, indicator=True)

                    # CLEAR MEMORY OF DIFFERENCED LIST
                    del [[dataframe_db1,dataframe_db2]]

                    # PRINT MISSING TABLE RECORDS
                    
                    if merged_data_frame[ merged_data_frame['_merge'] != 'both' ].empty is not True:

                        print(  merged_data_frame[ merged_data_frame['_merge'] != 'both' ]  )

                        merged_data_frame[ merged_data_frame['_merge'] != 'both' ].to_csv(SaveLocation + "MissingTableRecords_"+mysql_database_1+"_"+firstTable+"VS"+mysql_database_2+"_"+secondTable+".csv", sep=',', encoding='utf-8', index=False, header=True)

                        ErrorsList.append(" [ERROR] Missing Table Records found for Table 1 : " + firstTable + " : " + str(count1) + " and Table 2 : "+ secondTable + " : " + str(count2) )

                    else:
                        Tlog("No Missing data has been found !!!")
                  
                    if Table_Not_Keys_List :

                        for column_name in Table_Not_Keys_List:

                            Tlog('Comparing data for the column : ' + column_name + ' for Tables : ' +firstTable+' <=> '+ secondTable)
                            
                            # ADD CHECK SAYING THE COLUMNS ARE EQUAL AND IF NOT EQUAL THEN PRINT THE DATA
                            
                            if merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() )  )  & (merged_data_frame['_merge'] == 'both') ].empty is not True:

                                print( merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() )  )  & (merged_data_frame['_merge'] == 'both') ] )

                                merged_data_frame[  (merged_data_frame[ column_name+suffix[0] ] != merged_data_frame[ column_name+suffix[1] ]) & (  (merged_data_frame[ column_name+suffix[0]].notnull()) | ( merged_data_frame[ column_name+suffix[1]].notnull() ) )  & (merged_data_frame['_merge'] == 'both')  ].to_csv(SaveLocation + "DataDifferences_"+mysql_database_1+"_"+str(column_name)+"_"+firstTable+"VS"+mysql_database_2+"_"+secondTable+".csv", sep=',', encoding='utf-8', index=False, header=True)

                                ErrorsList.append(" [ERROR] Data Difference found for Table 1 : " + firstTable + " and Table 2 : "+ secondTable + " for column : " + column_name)

                            else:

                                Tlog('NO DIFFERENCES FOUND for the column : ' + column_name + ' for Tables : ' +firstTable+' <=> '+ secondTable)
                                
                    else:
                        
                        Tlog("[WARNING] :: TABLE HAS TIMESTAMP OR KEY FIELDS ONLY AND NO DATA FIELDS")
                        
                        ErrorsList.append("[WARNING] :: TABLE HAS TIMESTAMP OR KEY FIELDS ONLY AND NO DATA FIELDS IN COMPARISON for table 1 : " + firstTable + " and Table 2 : "+ secondTable + " for column : " + column_name)

                    # CLEAR DYNAMIC MEMORY  HERE
                    del merged_data_frame

                    # CLEAR USED FOR DATAFRAMES
                    Clear_Memory()


                    
                
                
        else:
        
            Tlog(" [ERROR] COUNT MISMATCH DETECTED :: SKIPPING TABLE : " + str(firstTable) + " <=> " + str(secondTable))

            ErrorsList.append(" [ERROR] COUNT MISMATCH DETECTED :: SKIPPING TABLE : " + str(firstTable) + " <=> " + str(secondTable))
        





# >>>>>>>>>>>>>>>>>>>>>>> VERIFY THE STRUCTURAL INTEGRITY OF TABLE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def verifyStructuralIntegrity(table1, table2, connection1_, connection2_):
    
    # CROSS VERIFY THE STRUCTURAL INTEGRITY 

    QueryNow = prepareQuery(table1,BaseQuery_=query['DESC_TABLE'])
    
    TabStruct_1 = pd.read_sql(QueryNow, con=connection1_)

    QueryNow = prepareQuery(table2,BaseQuery_=query['DESC_TABLE'])
    
    TabStruct_2 = pd.read_sql(QueryNow, con=connection2_)


    result = TabStruct_1.equals(TabStruct_2)

    # CLEAR FRAME MEMORY ALLOCATED
    del [[TabStruct_1,TabStruct_2]]

    return result
 







# >>>>>>>>>>>>>>>>>>>>>>> MATCH THE TABLES BY NAME BELOW  <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def MatchTablesByName(dataframe1_, dataframe2_, connection1_, connection2_):

    global ErrorsList 
    
    # VERIFY THE TABLES NAMES LIST
    
    Position = 0
    CompareDataFrame_ = pd.DataFrame(columns=['ComparisonList'])
 
    # GET THE COLUMN NAME OF THE SINGLE COLUMN DATAFRAME
    column_1=dataframe1_.columns.values[0]
    column_2=dataframe2_.columns.values[0]

    # ITERATE THROUGH THE FIRST DATABASE TABLES
    for indexNumber_in_1,row_in_1 in dataframe1_.iterrows():
        
        # ITERATE THROUGH THE SECOND DATABASE TABLES
        for indexNumber_in_2,row_in_2 in dataframe2_.iterrows():
       
            # IF THE TABLE NAMES MATCH
            if row_in_1[column_1] == row_in_2[column_2]:
                
                # VERIFY IF THEIR STRUCTURE MATCHES ALSO
                if verifyStructuralIntegrity(table1 = row_in_1[column_1] ,table2 = row_in_2[column_2], connection1_ = connection1_, connection2_ = connection2_) is True :

                    Tlog("TABLE INTEGRITY VERIFICATION : "+ row_in_1[column_1] +" - OK ")

                    # INSERT INTO EXISTING COLUMN
                    CompareDataFrame_.loc[Position] = row_in_1[column_1] + "|" + row_in_2[column_2]
                
                    Position = Position + 1

                else:
                
                    Tlog("TABLE INTEGRITY VERIFICATION : "+ row_in_1[column_1] +" - NOT OK ")
                    
                    ErrorsList.append("TABLE INTEGRITY VERIFICATION : "+ row_in_1[column_1] +" - NOT OK ")
    
    if Position == 1 : mydie("NO MATCHING TABLES FOUND !!!")

    Position=column_1=column_2=indexNumber_in_1=row_in_1=indexNumber_in_2=row_in_2=""

    # CLEAR USED SPACES BY VARIABLES.
    del Position,column_1,column_2,indexNumber_in_1,row_in_1,indexNumber_in_2,row_in_2

    # CLEAR USED DATAFRAMES
    Clear_Memory()

    return CompareDataFrame_
     
        


# >>>>>>>>>>>>>>>>>>>>>>> DEFINE THE MAIN SUB-ROUTINES USED HERE <<<<<<<<<<<<<<<<<<<<<<<<<<<<

def main():


        # 1.) 	INITIALIZE THE GLOBALS FOR THE SCRIPT.
        globalSet()

        # 2.)   GET THE ARGUMENTS LIST
        configFile=args()
        
        get_Config_Params(ini_=configFile) 

        # 3.) 	CHECK THE VERSION AND TYPE OF DATABASES TO VERIFY THE QUERY COMPATABLILITY    
        # CREATE FIRST CONNECTION
        Database1=DbConnect1()

        # CREATE SECOND CONNECTION
        Database2=DbConnect2()
        
        if Test_comp is True:
            
            Tlog("TESTING QUERY SYNTAX : ON")

            # TEST DATABASE 1 
            Tlog("Testing Queries in :: "+mysql_database_1+" @"+mysql_host_name_1)
            TestQueryStructure(connection_ = Database1)

            # TEST DATABASE 2
            Tlog("Testing Queries in :: "+mysql_database_2+" @"+mysql_host_name_2)
            TestQueryStructure(connection_ = Database2)

        # 4.) 	GET THE LIST OF TABLES FROM DATABASES.
        # 5.) 	FIND THE MATCHING TABLE NAMES AND PRINT THEM.
        # 6.) 	PRINT THE MATCHING TABLE NAMES.
        # 7.) 	CHECK IF THE MATCHED TABLES HAVE THE MATCHING STRUCTURE WITH THEM.
        
        # GET TABLES LIST FROM DATABASE 1
        QueryNow = prepareQuery(mysql_database_1,BaseQuery_=query['GET_TABLES_IN_DATABASE'])

        tables_in_db1 = pd.read_sql(QueryNow, con=Database1)

        Tlog("COMPLETED RETRIEVING TABLES FROM DATABASE 1 : " + mysql_database_1)

        # GET TABLES LIST FROM DATABASE 2
        QueryNow = prepareQuery(mysql_database_2,BaseQuery_=query['GET_TABLES_IN_DATABASE'])

        tables_in_db2 = pd.read_sql(QueryNow, con=Database2)

        Tlog("COMPLETED RETRIEVING TABLES FROM DATABASE 1 : " + mysql_database_1)

        # 8.) 	CHECK IF ANY UNMATCHED TABLES HAVE SIMILAR STRUCTURE BUT DIFFERENT NAMES.( Include args option DifferredTableNames = True/False )
        if DifferedTableNames is True:

            Tlog('COMPARING WITH DIFFERED TABLES NAME CONDITION')

            ToCompareTables = MatchAllTables(dataframe1_ = tables_in_db1, dataframe2_ = tables_in_db2, connection1_ = Database1 ,connection2_ = Database2)

        else:

            Tlog('COMPARING WITH MATCHING TABLES NAME CONDITION')

            ToCompareDataFrame_ = MatchTablesByName(dataframe1_ = tables_in_db1, dataframe2_ = tables_in_db2, connection1_ = Database1, connection2_ = Database2)
       
        # TODO 
        # 9.) 	IF MORE THAN ONE MATCH IS FOUND THEN, TRY USING ONE TO ONE (OR) ONE TO MANY APPROACH ( Include args option OneToMany = True/False )
        # 10.) 	IF ONE TO MANY OPTION IS SET TO FALSE THEN INORDER TO FIND THE CLOSEST MATCH TABLE, DO BELOW CHECKS
        # 	    1.) FIND NEAREST MATCHING WITH COUNT OF DATA
    
 
        del [[tables_in_db1,tables_in_db2]]



        # 11.) CHECK IF COUNT OF TABLE EXCEEDS PARSING LIMIT
        CompareTableContent(DataFrame_ = ToCompareDataFrame_, connection1_ = Database1, connection2_ = Database2)

        # CLEAR USED FOR DATAFRAMES
        Clear_Memory()

        # CLOSE THE DB CONNECTION 2 
        DbDisConnect2()

        # CLOSE THE DB CONNECTION 1
        DbDisConnect1()

        now = str(datetime.datetime.now()).split('.')[0]

        if len(ErrorsList) == 0:

            Tlog("No Issues Found.\n")

            Email(Subject_ =  "DATA COMPARISON BETWEEN - "+mysql_database_1+" @"+mysql_host_name_1+" VS "+mysql_database_2+" @"+mysql_host_name_2+": "+str(now), Content_ = " No Issues Found.")
        
        
        else :

            Tlog( "Issue's Found are Added Below ::\n\n" + str("\n".join(ErrorsList)) )

            Email(Subject_ =  "DATA COMPARISON BETWEEN - "+mysql_database_1+" @"+mysql_host_name_1+" VS "+mysql_database_2+" @"+mysql_host_name_2+": "+str(now), Content_ = "\nIssues Found BELOW :: \n" + "\n\t".join(ErrorsList))


# >>>>>>>>>>>>>>>>>>>>>>> DECLARE THE MAIN FUNCTION ERROR CATCH MECHANISM HERE  <<<<<<<<<<<<<<<<<<<<<<<<<<<<

if __name__=="__main__":

        main()
