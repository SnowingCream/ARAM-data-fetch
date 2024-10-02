import psycopg2

# helper for db connection and query string generation
class DBConnector:

    def __init__(self):

        # set up the connection
        with open("db_setting.txt", "r") as f:
            values = f.readlines()
            dbname, user, password = values[0].strip(), values[1].strip(), values[2].strip()
    
        self.connection = psycopg2.connect(host = '127.0.0.1', 
                                  dbname = dbname,
                                  user = user,
                                  password = password,
                                  port=5432)
    
        self.cursor = self.connection.cursor()

    '''
    Query function 1
    Behavior: concatenate the input dictionary into a query string and execute the query
    Paramter: query input (dictionary with key being keyword and value being the following statement for that keyword)
    Return: returned data (a list of tuples)
    '''
    def select_query(self, query_input):
        
        query = ""
        
        for line in query_input:
            query += line + " " + query_input[line] + " "

        query = query.strip()
        # print("given query: " + query)
        self.cursor.execute(query)
        return self.cursor.fetchall()


    '''
    Query function 2
    Behavior: concatenate the input dictionary into a query string and execute the query
        - VALUE (%s, %s, %s...) line doesn't need to be provided; it is auto-generated.
        - if the query contains "DO UPDATE SET", provide optional parameter update_col_input and don't write farther.
    Parameter:
        query input: basic query input (dictionary of keyword: the rest of statement)
            - VALUE line doesn't go in the input.
            - DO UPDATE SET line doesn't contain the rest.
        col input: a list of column names to be inserted
        val_input: a tuple of values to be inserted (and updated).
            - since we get these values from api calls, everything is string. type coversion will be done by DB once inserted.
            - if DO UPDATE SET is used, updating values should also be attached at the end following the order.
        update_col_input: a list of column names to be update in case of update upon conflict (DO UPDATE SET)
    Return: None
    '''

    def insert_query(self, query_input, col_input, val_input, update_col_input = None):

        query = ""

        for line in query_input:
            query += line + " " + query_input[line] + " "

            # if keyword is "INSERT", append "(col1, col2, col3... )" at the end
            if line.upper() == "INSERT":
                
                cols = "("
                for col in col_input:
                    cols += col + ", "
                # trim the end: replace the last comma with ")"
                cols = cols[:-2] + ") "
                query += cols

                cols = "VALUES ("
                for col in col_input:
                    cols += "%s, "
                # trim the end: replace the last comma with ")"
                cols = cols[:-2] + ") "
                query += cols


            if line.upper() == "DO UPDATE SET":
                for col in update_col_input:
                    query += col + " = %s, "

                # trim the end of query: remove the last comma and white space
                query = query[:-2]

        # replace for DO UPDATE SET followed by 2 white spaces, strip just in case.
        query = query.replace("  ", " ").strip()
        
        # debug
        # print("given query: " + query)
        # print("given value: " + val_input)
        
        self.cursor.execute(query, val_input)


    '''
    Helper function 1
    Behavior: commit the changes that have been made so far
    Parameter: None
    Return: None
    '''
    def commit(self):
        self.connection.commit()
    
    
    '''
    Helper function 2
    Behavior: shut down the connection. ALWAYS CALL IT AT THE END
    Parameter: None
    Return: None
    '''
    def end(self):
        self.cursor.close()
        self.connection.close()


                
        

            

        