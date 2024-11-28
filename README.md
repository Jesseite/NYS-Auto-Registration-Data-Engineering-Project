# New York Auto Registration End to End DE

## Information

![image]()

## New York Government Data 
Open NY provides public access to digital data for analysis. The New York Transportation Registrations dataset contains over 12 million records. You can refer to the link I provided below for more information.

https://data.ny.gov/
https://data.ny.gov/Transportation/Vehicle-Snowmobile-and-Boat-Registrations/w4pv-hbkt/about_data

## Python
### Steps
`insert_mongo` -> This script fetches vehicle data from the URL, which contains information up to 9 million vehicles with model years from 2000 onwards. It then inserts the fetched JSON data into MongoDB.

`transform_data` -> It transform the JSON data from MongoDB into a well-organized format using Pandas. This process includes removing unnecessary columns, modifying data types, and correcting any misspellings.

`load_postgres` -> This script loads data into PostgreSQL using the pickle file, as the large data size requires serialization for storage.
## Airflow
`DAG_nys_registration` -> The DAG file

## Docker
`dockerfile`

`docker-compose`

## MongoDB
This is a local MongoDB server. I use MongoDBCompass which is a GUI for MongoDB

## PostgreSQL
This is a local PostgreSQL server. I recommend downloading PgAdmin which is a GUI for PostgreSQL.

## Microsoft PowerBI

**** screenshot picture here link don't forget!****!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!dfsdtgwrtewrterfgdfgsdgtw4tw4tw4rtwe4r

`NYS Auto Registration - PowerBI`



