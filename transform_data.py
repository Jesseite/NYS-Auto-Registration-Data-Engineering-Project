import pymongo
import pandas as pd
import os
from dotenv import load_dotenv

#Loading variables from .env file
load_dotenv()

#Connect to MongoDB
client = pymongo.MongoClient(os.getenv('MONGO_CLIENT'))
db = client[os.getenv('MONGO_DATABASE')]
collection = db[os.getenv('MONGO_COLLECTION')]

#Fetch data from MongoDB
data = list(collection.find())

#Convert data to Dataframe
df = pd.DataFrame(data)

#Drop columns
df.drop(['_id', 'record_type', 'registration_class', 'scofflaw_indicator', 'suspension_indicator', 'revocation_indicator', 'maximum_gross_weight'], axis=1, inplace=True)

#Change datatypes
df['reg_valid_date'] = pd.to_datetime(df['reg_valid_date'])
df['reg_expiration_date'] = pd.to_datetime(df['reg_expiration_date'])

#Change the datetime format for reg_valid_date and reg_expiration_date columns
df['reg_valid_date'] = df['reg_valid_date'].dt.strftime('%Y-%m-%d')
df['reg_expiration_date'] = df['reg_expiration_date'].dt.strftime('%Y-%m-%d')

#Convert columns to numeric types
df['zip'] = pd.to_numeric(df['zip'], errors='coerce').fillna(0).astype('int64')
df['model_year'] = pd.to_numeric(df['model_year'], errors='coerce').fillna(0).astype('int64')
df['unladen_weight'] = pd.to_numeric(df['unladen_weight'], errors='coerce').fillna(0).astype('int64')

#Change abbreviations in the color column to full names
color_mapping = {
    "BK": "Black",
    "BL": "Blue",
    "BR": "Brown",
    "GL": "Gold",
    "GY": "Gray",
    "MR": "Maroon",
    "OR": "Orange",
    "PK": "Pink",
    "PR": "Purple",
    "RD": "Red",
    "TN": "Tan",
    "WH": "White",
    "YW": "Yellow"
}

df["color"] = df["color"].map(color_mapping)

#Change abbreviations in the body type column to full names
body_type_mapping = {
    "2DSD": "Sedan",
    "4DSD": "Sedan",
    "SUBN": "SUV",
    "PICK": "Truck"
}

df["body_type"] = df["body_type"].map(body_type_mapping)

#Fix common automakers' names
common_automakers = {
    'HONDA': 'Honda', 'HOND': 'Honda', 'HNODA': 'Honda', 'HONFA': 'Honda', 'HODNA': 'Honda',
    'PONTI': 'Pontiac', 'PONT': 'Pontiac',
    'NISSA': 'Nissan', 'NISS': 'Nissan', 'NSISA': 'Nissan', 'NISAA': 'Nissan',
    'ACURA': 'Acura', 'ACUR': 'Acura',
    'HUMME': 'Hummer', 'HUMMR': 'Hummer', 'HUMM': 'Hummer',
    'HYUND': 'Hyundai', 'HYUN': 'Hyundai', 'HUYND': 'Hyundai', 'HYUDA': 'Hyundai', 'HYNDI': 'Hyundai', 'HYUAN': 'Hyundai',
    'LINCO': 'Lincoln', 'LINC': 'Lincoln', 'LICOL': 'Lincoln',
    'LUCID': 'Lucid',
    'RIVIA': 'Rivian', 'RIVA': 'Rivian', 'RVN': 'Rivian', 'RIVIN': 'Rivian', 'RIV': 'Rivian', 'RIVI': 'Rivian',
    'TOYOT': 'Toyota', 'TOYT': 'Toyota', 'TOYOR': 'Toyota', 'TOYTO': 'Toyota', 'YOYOT': 'Toyota', 'TOYOY': 'Toyota', 'TOTOT': 'Toyota',
    'KIA': 'Kia', 'KI/MO': 'Kia',
    'VOLKS': 'Volkswagen', 'VILKS': 'Volkswagen', 'VW': 'Volkswagen',
    'ME/BE': 'Mercedes-Benz', 'MERZ': 'Mercedes-Benz', 'ME/B': 'Mercedes-Benz',
    'SUBAR': 'Subaru', 'SUBARU': 'Subaru',
    'MERCU': 'Mercury', 'MERCE': 'Mercedes-Benz', 'MERCED': 'Mercedes-Benz',
    'CADIL': 'Cadillac', 'CADILLAC': 'Cadillac',
    'CHEVY': 'Chevrolet', 'CHEVR': 'Chevrolet',
    'MAXDA': 'Mazda', 'MADZA': 'Mazda',
    'CHRYL': 'Chrysler', 'CHRY': 'Chrysler', 'CHR': 'Chrysler',
    'DOGE': 'Dodge', 'DODG': 'Dodge', 'DODHE': 'Dodge', 'DOGDE': 'Dodge',
    'TOYOS': 'Toyota', 'TPYOT': 'Toyota', 'TOYO': 'Toyota',
    'JAGUA': 'Jaguar', 'JAGU': 'Jaguar', 'JAGAU': 'Jaguar', 'JAG': 'Jaguar',
    'LANDR': 'Land Rover', 'LAND': 'Land Rover', 'RA/RO': 'Land Rover',
    'BUIC': 'Buick',
    'VOLV': 'Volvo', 'VOVLO': 'Volvo', 'VOLOV': 'Volvo',
    'AUDI': 'Audi', 'AUD': 'Audi',
    'PORSC': 'Porsche', 'PORSH': 'Porsche', 'PORCH': 'Porsche', 'PORS': 'Porsche',
    'MINI': 'Mini', 'MINIC': 'Mini',
    'MAZD': 'Mazda', 'MAZDA': 'Mazda',
    'LINCL': 'Lincoln', 'LINCO': 'Lincoln',
    'ISU': 'Isuzu', 'IZUZU': 'Isuzu',
    'LEXI': 'Lexus', 'LEXIS': 'Lexus', 'LEXIN': 'Lexus', 'LEXUS': 'Lexus',
    'SUBN': 'Subaru', 'SUBA': 'Subaru', 'SUBAE': 'Subaru', 'SUBAR': 'Subaru',
    'MITS': 'Mitsubishi', 'MITSU': 'Mitsubishi', 'MITSI': 'Mitsubishi', 'MITZU': 'Mitsubishi',
    'HINDA': 'Honda', 'HONA': 'Honda', 'HONDQ': 'Honda', 'HONDS': 'Honda',
    'TESLA': 'Tesla', 'TESL': 'Tesla',
    'CHEVE': 'Chevrolet', 'CHEVR': 'Chevrolet',
}

df["make"] = df["make"].map(common_automakers)

#Save dataframe to a file
df.to_pickle('transformed_data.pkl')