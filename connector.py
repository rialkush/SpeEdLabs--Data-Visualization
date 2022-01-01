# for making connection with database
import pyodbc
import pymssql

########### function to make a default connection with database via pyodbc #############

def connect_default_via_pyodbc():
    """This function makes a default connection with database
 
     Parameters:
     No Arguments
 
     Returns:
     pyodbc.Connection: Returns the Connection with database
 
    """
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=20.198.89.10;'
                      'Database=speedlabs-anon;'
                      'UID=Speedlabsread;'
                      'PWD=$tar@Night;')
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM Course')
        print(" ")
        print("Successfully Connected to the Database")
    except:
        print(" ")
        print("Connection-Failed!")
    return conn

########### function to make a manual connection with database via pyodbc#############

def connect_manual_via_pyodbc():
    """This function makes a manual connection with database
 
     Parameters:
     No Arguments
 
     Returns:
     pyodbc.Connection: Returns the Connection with database
 
    """
    server_name = input("Enter Server Name: ")
    database_name = input("Enter Database Name: ")
    user_id = input("Enter User Id: ")
    password = input("Enter Password: ")
    
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server='+server_name+';'
                      'Database='+database_name+';'
                      'UID='+user_id+';'
                      'PWD='+password+';')
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM Course')
        print(" ")
        print("Successfully Connected to the Database")
    except:
        print(" ")
        print("Connection-Failed!")
        print("Please enter correct login details")
    return conn



########### function to make a default connection with database via pymssql #############

def connect_default_via_pymssql():
    """This function makes a default connection with database
 
     Parameters:
     No Arguments
 
     Returns:
     pyodbc.Connection: Returns the Connection with database
 
    """
    server_name = "20.198.89.10"
    database_name = "speedlabs-anon"
    user_id = "Speedlabsread"
    password = "$tar@Night"
 
    conn = pymssql.connect(server_name, user_id, password, database_name)
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM Course')
        print(" ")
        print("Successfully Connected to the Database")
    except:
        print(" ")
        print("Connection-Failed!")
    return conn

########### function to make a manual connection with database via pymssql #############

def connect_manual_via_pymssql():
    """This function makes a manual connection with database
 
     Parameters:
     No Arguments
 
     Returns:
     pyodbc.Connection: Returns the Connection with database
 
    """
    server_name = input("Enter Server Name: ")
    database_name = input("Enter Database Name: ")
    user_id = input("Enter User Id: ")
    password = input("Enter Password: ")
    
    conn = pymssql.connect(server_name, user_id, password, database_name)
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM Course')
        print(" ")
        print("Successfully Connected to the Database")
    except:
        print(" ")
        print("Connection-Failed!")
        print("Please enter correct login details")
    return conn