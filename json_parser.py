import json
from sqlalchemy import create_engine
import pandas as pd

with open("shape.json") as f:
    data = json.load(f)

with open("new.json",'w') as f:
    json.dump(data,f,indent=2)

with open("new.json") as f:
    data = json.load(f)

df = pd.DataFrame(columns=["city","state","latitude","longitude"])
city = []
state = []
latitude = []
longitude = []

for i in range(21):

    for j in range(len(data["features"][i]["geometry"]["coordinates"][0])):

        city.append(data["features"][i]["properties"]["name"])

        state.append(data["features"][i]["properties"]["parent"])

        latitude.append(data["features"][i]["geometry"]["coordinates"][0][j][0])

        longitude.append(data["features"][i]["geometry"]["coordinates"][0][j][1]) 

df["city"] = city
df["state"] = state
df["latitude"] = latitude
df["longitude"] = longitude       

db = create_engine('postgres+psycopg2://postgres:1234@localhost:5432/new_db')

df.to_sql('geojson', db)

df.to_csv("geojson.csv")

latitude = float(input())
longitude = float(input())
res = engine.execute(text(f"""select city, state from geojson where latitude={latitude} and longitude={longitude};"""))
lst = []
for r in res:
    lst.append(r)

lst



