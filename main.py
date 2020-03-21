from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, Numeric 
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker
import pandas as pd
from sqlalchemy.sql import text
from math import sin, cos, acos



db = create_engine('postgres+psycopg2://postgres:1234@localhost:5432/new_db')
base = declarative_base()
statement = text(f"""select * from test_table""")
app = FastAPI()


@app.post("/post_location/")
async def post_location(fk: str=None, pincode: int = None):
    if(pincode==None):
        res = db.execute(text(f"""select city, latitude, longitude, pincode from test_table where location='{fk}';"""))
        lst = []
        for r in res:
            lst.append(r)
    else:
        tmp_res = db.execute(text(f"""SELECT * from test_table where pincode={pincode}"""))
        tmp_lst = []
        for tmp_r in tmp_res:
            tmp_lst.append(tmp_r)
        if(len(tmp_lst)):
            return {"res":"already exist"}
        else:
            res = db.execute(text(f"""select count(*) from test_table;"""))
            for r in res:
                num = r[0]
            db.execute(text(f"""INSERT INTO test_table (index,pincode) VALUES ({num+1},{pincode});"""))
            return {"res":"inserted sucessfully"}

                

    return {'res': lst}


@app.get("/get_location/")
async def get_location(latitude: float, longitude: float):

    res = db.execute(text(f"""select city, location, pincode from test_table where latitude={latitude} and longitude={longitude};"""))
    lst = []
    for r in res:
        lst.append(r)

    return {"res":lst[0]}

@app.get("/get_using_self/")
async def get_using_self(latitude: float, longitude: float):

    def coordDistance(latitude1, longitude1, latitude2, longitude2):
        alpha = acos(sin(latitude1) * sin(latitude2) + cos(latitude1) * cos(latitude2) * cos(longitude2 - longitude1))
        return (2*3.14*6378.1*alpha)/360


    tmp_res = db.execute(text(f"""SELECT  latitude, longitude, pincode FROM test_table;"""))
    tmp_lst = []
    for tmp_r in tmp_res:
        tmp_lst.append(tmp_r)
    lst = []

    for i in range(len(tmp_lst)):
        if(tmp_lst[i][1]==None or (tmp_lst[i][0])==None):
            continue
        else:
            dist = coordDistance(latitude1=latitude,latitude2=float(tmp_lst[i][0]),longitude1=longitude,longitude2=float(tmp_lst[i][1]))
            if(dist<5):
                lst.append(tmp_lst[i])

    return {"res":lst}


@app.get("/detect/")
async def detect(latitude: float, longitude: float):

    res = db.execute(text(f"""select city, state from geojson where latitude={latitude} and longitude={longitude};"""))
    lst = []
    for r in res:
        lst.append(r)
    return({"res":lst})