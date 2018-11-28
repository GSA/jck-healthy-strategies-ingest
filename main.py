from db.db import dal
from skyspark import SkySparkAPI


def main():
    #get data
    #parse data
    dal.connect()
    session = dal.Session()
    # insert data
    session.commit()