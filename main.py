import pandas as pd


#the file is directly being used here since this file is running on IDE
#to use this code in lambda function, we'll create a s3 resource,get file object, then get data from the object and use that data
f1=pd.read_csv("schema_yesterday.csv")
f2=pd.read_csv("schema_today.csv")

df1=pd.DataFrame(f1)
df2=pd.DataFrame(f2)

tableNamesY=set(df1.table_name)
tableNamesT=set(df2.table_name)

addedtablenames=list(tableNamesT-tableNamesY)
deletedtablenames=list(tableNamesY-tableNamesT)

#till now we have table names added or removed
# print(addedtablenames)
# print(deletedtablenames)

#now for added or removed columns
df = pd.merge(df1, df2, on='column_name', how='outer')    #combined/outer-joined data-frame

addedCols=dict()
deletedCols=dict()

for currTable in tableNamesT.intersection(tableNamesY):
    
    d1=df[df['table_name_x']==currTable]
    deleted=list(d1[d1['table_name_y'].isna()]['column_name'].unique())
    if len(deleted)!=0:
        deletedCols[currTable]=deleted


    d2=df[df['table_name_y']==currTable]
    added=list(d2[d2['table_name_x'].isna()]['column_name'].unique())
    if len(added)!=0:
        addedCols[currTable]=added

# print(addedCols)
# print(deletedCols)
    
addedColString=""
deletedColString=""

for k,v in addedCols.items():
    st1=k+' | '+' '.join(v)+'\n'
    addedColString=addedColString+st1
    
    
for k,v in deletedCols.items():
    st1=k+' | '+', '.join(v)+'\n'
    deletedColString=deletedColString+st1
    


msgAddTable=f'''
New table detected in source database
Database Name: "source"
Table Name: {", ".join(addedtablenames)}
'''

msgAddCol=f'''
New column(s) detected in source database
Database Name: "source"
Table Name | Column Name
{addedColString}
'''

msgRemCol=f'''
Column(s) does not exist or removed from source database
Database Name: "source"
Table Name | Column Name
{deletedColString}
'''


msgRemTable=f'''
Table(s) does not exist or removed from source database
Database Name: "source"
Table Name: {", ".join(deletedtablenames)}
'''

if len(addedtablenames)==0:
    msgAddTable=""

if len(deletedtablenames)==0:
    msgRemTable=""

if len(addedCols)==0:
    msgAddCol=""    

if len(deletedCols)==0:
    msgRemCol=""
    
if (len(addedtablenames)==0 and len(deletedtablenames)==0 and len(addedCols)==0 and len(deletedCols)==0):
    message=""
else:
    message=f'''

Database schema changes detected!

    {msgAddTable}

    {msgAddCol}

    {msgRemCol}

    {msgRemTable}
    '''

print(message)