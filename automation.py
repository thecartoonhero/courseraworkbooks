# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSql
import ibm_db

# Connect to MySQL
def mysql_connect():
    print("attempting mysql connection")
    connection = mysql.connector.connect(user='root', password='ppbguXYSL4vgDeeesaoYeuGe', host='172.21.232.201', port='3306',
                                         database='sales')
    print("Connected to MySQL database")
    return connection

# Connect to DB2 or PostgreSql
def db2_connect():
    dsn_hostname = "6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"
    dsn_uid = "zsw97919"  # e.g. "abc12345"
    dsn_pwd = "Yf6u2i5rRuu2iuaN"  # e.g. "7dBZ3wWt9XN6$o0J"
    dsn_port = "30376"  # e.g. "50000"
    dsn_database = "bludb"  # i.e. "BLUDB"
    dsn_driver = "{IBM DB2 ODBC DRIVER}"  # i.e. "{IBM DB2 ODBC DRIVER}"
    dsn_protocol = "TCPIP"  # i.e. "TCPIP"
    dsn_security = "SSL"  # i.e. "SSL"

    # Create the dsn connection string
    dsn = (
        "DRIVER={0};"
        "DATABASE={1};"
        "HOSTNAME={2};"
        "PORT={3};"
        "PROTOCOL={4};"
        "UID={5};"
        "PWD={6};"
        "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,
                                dsn_security)
    print("attempting db2 connection")
    # create connection
    db2conn = ibm_db.connect(dsn, "", "")

    print("Connected to DB2 database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

    return db2conn

#Establish connections
db2conn = db2_connect()
mysqlconn = mysql_connect()


# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid(dbconnection):

    SQL = "SELECT MAX(rowid) FROM sales_data"
    stmt = ibm_db.exec_immediate(dbconnection, SQL)
    tuple = ibm_db.fetch_tuple(stmt)
    if tuple:
        return tuple[0]  # Assuming the first element is the max rowid
    else:
        return None  # Return None if no rows found
    pass


last_row_id = get_last_rowid(db2conn)
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(connection, rowid):
    cursor = connection.cursor()
    try:
        SQL = "SELECT rowid, product_id, customer_id, quantity FROM sales.sales_data WHERE rowid > %s"
        cursor.execute(SQL, (rowid,))
        return cursor.fetchall()

    finally:
        cursor.close()

new_records = get_latest_records(mysqlconn, last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records, db2connection, mysqlconnection):
    if records is None:
        print("No records to insert")
        return  # exit the function

    for row in records:
        # Prepare the SQL statement with question mark placeholders
        SQL = "INSERT INTO SALES_DATA (ROWID, PRODUCT_ID, CUSTOMER_ID, QUANTITY) VALUES (?, ?, ?, ?)"

        # Prepare the statement (this is crucial for ibm_db)
        stmt = ibm_db.prepare(db2connection, SQL)

        # Execute the prepared statement with the row data
        ibm_db.execute(stmt, row)

insert_records(new_records, db2conn, mysqlconn)
print("New rows inserted into production datawarehouse = ", len(new_records))

#disconnect
ibm_db.close(db2conn)
mysqlconn.close()

# End of program