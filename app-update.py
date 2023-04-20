#!/usr/bin/env python3
##
# Script to continuously inserts new documents into the MongoDB database/
# collection 'AUTO_HA.records'
#
# Prerequisite: Install latest PyMongo driver and other libraries, e.g:
#   $ sudo pip3 install pymongo dnspython
#
# For usage details, run with no params (first ensure script is executable):
#   $ ./continuous-insert.py
##
import sys
from random import randint
import time
import datetime
import pymongo

USER = "sa"
PASSWORD = "admin"
CLUSTER_URL = "ambankdemoonprem.g3aer.mongodb.net"
DB_NAME = 'AMBANK'

####
# Main start function
####


def main():
    peform_inserts()

####
# Perform the continuous database insert workload, sleeping for 10 milliseconds
# between each insert operation
####


def peform_inserts():
    mongodb_url = f'mongodb+srv://{USER}:{PASSWORD}@{CLUSTER_URL}/?retryWrites=true&w=majority'
    print(f'Connecting to:\n {mongodb_url}\n')
    connection = pymongo.MongoClient(
        mongodb_url, retryWrites=True, retryReads=True)
    db = connection[DB_NAME]

    try:
        update_txn = db.transactions.find(
            {"status": "open"}).sort("date_created", -1)

        for i in update_txn:

            print("############ Start Transaction update ##############")
            print("Before Update: " + str(i))
            db.transactions.update_one(
                {
                    "_id": i["_id"]
                },
                {
                    "$set": {"status": "closed", "date_modified": datetime.datetime.utcnow()}
                }
            )
            print("Updated Transaction ID: " + str(i["_id"]))
            print("############ End Transaction update ##############")

    except KeyboardInterrupt:
        print
        sys.exit(0)
    except Exception as e:
        print(f'{datetime.datetime.now()} - DB-CONNECTION-PROBLEM: '
              f'{str(e)}')
        connect_problem = True


# Constants


####
# Main
####
if __name__ == '__main__':
    main()
