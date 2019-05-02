#! /usr/bin/python3.5

import warnings

# >>>>>>>>>>>>>>>>>>>>>>> PREPARE SQL RAW QUERIES WITH VALUES <<<<<<<<<<<<<<<<<<<<<<<<<<<<
def sqlPrepare(Query_,*Inputs_):

        # CHECK IF VALUES MATCH REPLACE STRING BEFORE EXECUTION
        if Query_.count('?') != len(Inputs_):
            warnings.warn("NUMBER OF INPUTS != NUMBER OF ENTRIES('?') IN QUERY")

        for value in Inputs_:
                Query_=Query_.replace('?',str(value),1)

        return Query_



# SAMPLE QUERY SKELETON
query = "SELECT COUNT(*) as '?' FROM ? WHERE id = '?'"

# CALL sqlPrepare to create the 
print(sqlPrepare(query,'TotalCount','students_details',73000))
