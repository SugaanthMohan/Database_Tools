#! /usr/bin/python3.5

# USED TO GET MySQLDB Data
import MySQLdb

# REGEX COMPARE
import re as Regex

#USED TO CREATE A DICTIONARY WITH ITS ORDER MAINTAINED
from collections import OrderedDict

# USED TO PRINT WARNING STATEMENTS INSTEAD OF EXIT
import warnings


# >>>>>>>>>>>>>>>>>>>>>>> CREATE A GLOBAL INITIALIZATION OF VARIABLES <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def globalSet():

    global mysql_host_name,mysql_user_name,mysql_password,mysql_database,query

    mysql_host_name     = ''
    mysql_user_name     = ''
    mysql_password      = ''
    mysql_database      = ''

    query = {}

    query['getProcedureMetaData']   = "SELECT PARAM_LIST FROM mysql.proc WHERE DB = '?' AND SPECIFIC_NAME ='?'"





# >>>>>>>>>>>>>>>>>>>>>>> PREPARE SQL RAW QUERIES WITH VALUES <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def sqlPrepare(Query_,*Inputs_):

        # CHECK IF VALUES MATCH REPLACE STRING BEFORE EXECUTION
        if Query_.count('?') != len(Inputs_):
            warnings.warn("NUMBER OF INPUTS != NUMBER OF ENTRIES('?') IN QUERY")

        for value in Inputs_:
                Query_=Query_.replace('?',str(value),1)

        return Query_


# >>>>>>>>>>>>>>>>>>>>>>> CREATE A CONNECTION TO THE DATABASE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbConnect():

        global mysql_host_name,mysql_user_name,mysql_password,mysql_database

        Database = MySQLdb.connect(host=mysql_host_name, user=mysql_user_name, passwd=mysql_password, db=mysql_database)

        print( "Connected to Database :: " + mysql_database )

        return Database


# >>>>>>>>>>>>>>>>>>>>>>> CLOSE THE CONNECTION TO THE DATABASE <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def DbDisConnect(database_):

        database_.close()

        print( "Disconnected to Database :: " + mysql_database )



# >>>>>>>>>>>>>>>>>>>>>>> GET THE PROCEDURE IN ARGUMENTS <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def getProcedureMetaData(Connection_,Procedure_,DatabaseName_):

    global query

    # CREATE A CURSOR FOR RETRIEVING THE DATABASE INFORMATION
    my_cursor = Connection_.cursor()

    # PREPARE THE SQL QUERY
    my_query = sqlPrepare(query['getProcedureMetaData'],DatabaseName_,Procedure_)

    # EXECUTE THE QUERY
    my_cursor.execute(my_query)

    # GET THE DATA FROM CURSOR
    datum = my_cursor.fetchall()

    # CLOSE CURSOR TO AVOID THE DEPENDENCY ISSUES
    my_cursor.close()

    # PROCEDURE ORDER MAINTAINED LIST OF ITEMS
    Ordered_Procedure_Params = OrderedDict()

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
                Ordered_Procedure_Params[Field_Name] = ""

    return Ordered_Procedure_Params


def main():
	
	# INITIALIZE THE GLOBAL VALuES
	globalSet()

	# OPEN THE DATABASE CONNECTION
	DBCon = DbConnect()
	
	# I AM NOT THAT LAME TO GIVE A STUDENT DETAILS EXAMPLE, JUST FOR EDUCATIONAL PURPOSES AND A QUICK SMILE ;-), DONT JUDGE  
	procedure = "get_student_details"

	Positional_Dictionary = getProcedureMetaData(Connection_ = DBCon,Procedure_ = procedure,DatabaseName_ = mysql_database)

	# SOME DATAMAP IN DICTIONARY HERE WHICH CAN BE USED TO PARSE THE DATA 
	# INTO SEPARATE CALL STATEMENT AND EXECUTE THE CALLSTATEMENT TO DB 
	# ALONG WITH THE COMMIT STATEMENTS.

	# CLOSE THE DATABASE CONNECTION
	DbDisConnect(database_ = DBCon)


if __name__ == '__main__':
	main()
