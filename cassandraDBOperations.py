import cassandra
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import json
from cassandra.cluster import Cluster
class CassaDBManagement:

    def __init__(self, username, password):
        """
        This function sets the required url
        """

        self.username  = username
        self.password = password

    def getCassaDBClientObject(self):
        """
        This function creates the client object for connection purpose
        """
        try:
            cloud_config = {
            'secure_connect_bundle': 'secure-connect-test1.zip'
             }

            auth_provider = PlainTextAuthProvider('DzgSXTSzQgWNpYocAWPpQzAX','27C9coC--crqmF0MiZldjv9Kg8NyhTzMP66SOPbHtiaNOWcidhyBz1FuOIuUp.,p2CajK266pu2QEhLkCNs4Zkt6qQaSce2cS_+10a9clpH6UhkdUkNtuBoTczw8sK_X')


            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        # cluster = 'localhost:27017'
            session = cluster.connect()
            return session
        except Exception as e:
            raise Exception("(getCassaDBClientObject): Something went wrong on creation of client object\n" + str(e))

    def closeCassaDBconnection(self, session):
        """
        This function closes the connection of client
        :return:
        """
        try:
            session.close()
        except Exception as e:
            raise Exception(f"Something went wrong on closing connection\n" + str(e))

    def isDatabasePresent(self, keyspace_nam):
        """
        This function checks if the database is present or not.
        :param db_name:
        :return:
        """

        self.keyspacenam = keyspace_nam


        try:
            session = self.getCassaDBClientObject()

            row1 = session.execute("SELECT table_name FROM system_schema.tables where keyspace_name='ineuron3' and table_name = 'review';").one()
            for i in row1:
                 if i != ' ':
                    #session.shutdown()
                    return True
                 else:
                    session.shutdown()
                    return False
        except Exception as e:
            raise Exception(f"No Keyspace" + str(e))



    def createDatabase(self, db_name):
        """
        This function creates database.
        :param db_name:
        :return:
        """


        try:
            session = self.getCassaDBClientObject()
            database_check_status = self.isDatabasePresent(db_name=db_name)
            if not database_check_status:
                try:
                    session.execute("use ineuron3;").one()
                    try:
                        session.execute(
                            "Create table + '{}' + review(cno INT PRIMARY KEY,comment TEXT,customer_name TEXT,discount_percent TEXT,emi_detail,offer_details TEXT,price TEXT,product_name TEXT,product_searched TEXT,ratings TEXT,review_age TEXT;".format(db_name)).one()
                        try:
                            "Create index on review(product_searched);"
                            "Create index on review(customer_name);"
                        except Exception as e:
                            raise Exception(f"Create table index error" + str(e))
                    except Exception as e:
                        raise Exception(f"Create table error" + str(e))
                except Exception as e:
                    raise Exception(f"No Keyspace" + str(e))

        except Exception as e:
            raise Exception(f"(createDatabase): Failed on creating database\n" + str(e))

    def dropDatabase(self, table_name):
        """
        This function deletes the database from MongoDB
        :param db_name:
        :return:
        """
        try:
            table1 = "DROP TABLE {}".format(keyspace_nam.table_name)
            session = self.getCassaDBClientObject()
            session.execute("DROP TABLE {}  where keyspace_name={}".format(keyspace_nam)).one()
            session.close()
            return True
        except Exception as e:
            raise Exception(f"(dropDatabase): Failed to delete database {db_name}\n" + str(e))

    def isProductPresent(self,firstrow1):
            """
            This function checks if the database is present or not.
            :param db_name:
            :return:
            """


            self.firstrow1 = firstrow1
            session = self.getCassaDBClientObject()
            try:
                self.firstrow1 = session.execute("SELECT count(*) FROM ineuron3.review;").one()


                return  self.firstrow1
            except Exception as e:
                raise Exception(f"Table error1" + str(e))

    def isProductPresent1(self, firstrow2):
                """
                This function checks if the database is present or not.
                :param db_name:
                :return:
                """

                self.firstrow2 = firstrow2
                session = self.getCassaDBClientObject()
                try:
                    self.firstrow2 = session.execute("SELECT * FROM ineuron3.review;").one()

                    return self.firstrow2

                except Exception as e:
                    raise Exception(f"Table Error2" + str(e))

    def iscountDatabase(self,countrec):

                self.countrec = countrec
                session = self.getCassaDBClientObject()
                try:
                    self.countrec = session.execute("SELECT COUNT(*) FROM ineuron3.review;").one()
                    return self.countrec
                except Exception as e:
                    raise Exception(f"iscountDatabase" + str(e))


    def getDetailfromDatabase(self,searchString,response):
                self.searchString = searchString
                self.response = response
                session = self.getCassaDBClientObject()
                try:
                    response = session.execute("select comment,customer_name,discount_percent,emi_detail,offer_details,price,product_name,product_searched,ratings,review_age from ineuron3.review where product_searched = ' + {} +';".format(self.searchString))
                    return response
                except Exception as e:
                    raise Exception(f"iscountDatabase" + str(e))



    def getAllDetailfromDatabase(self,responseAll):

                self.responseAll = responseAll
                session = self.getCassaDBClientObject()
                try:
                    response = session.execute("select comment,customer_name,discount_percent,emi_detail,offer_details,price,product_name,product_searched,ratings,review_age from ineuron3.review;")
                    return response
                except Exception as e:
                    raise Exception(f"iscountDatabase" + str(e))