print("Zabusito32")

import pandas as pd
import numpy as np
from sqlalchemy import create_engine 
import requests
import os 
import csv
import logging
import psycopg2
from dotenv import load_dotenv
load_dotenv()

#datos = r"C:\Users\zabu\Desktop\INEGI\data\datospobla.csv"
#limpios = r"C:\Users\zabu\Desktop\INEGI\output\poblaclean.csv"

datos = os.getenv("INPUT_PATH", "/app/data/datospobla.csv")
limpios = os.getenv("OUTPUT_PATH", "app/output/poblacionclean.csv")

#log_dir = r'C:\app\logs'
#os.makedirs(log_dir, exist_ok=True)

log_dir = os.getenv("LOG_DIR", "/app/logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)


logging.basicConfig(
    level=logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'pipeline.log')),
        logging.StreamHandler()
    ]
)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'TMHzabusito88'),
    'database': os.getenv('DB_NAME', 'population_data')
}


def pipeline(datos, limpios):
    try: 
        df = pd.read_csv(datos)
        print(df.head())

        #conversión de tipo: 

        #col_num = ['ENTIDAD', 'MUN', 'LOC', 'ALTITUD', 'POBTOT', 'POBFEM', 'POBMAS',
        #           'P_0A2', 'P_0A2_F', 'P_0A2_M', 'P_3YMAS', 'P_3YMAS_F', 'P_3YMAS_M',
        #            'P_5YMAS', 'P_5YMAS_F', 'P_5YMAS_M', 'P_12YMAS', 'P_12YMAS_F', 'P_12YMAS_M',
        #           'P_15YMAS', 'P_15YMAS_F', 'P_15YMAS_M', 'P_18YMAS', 'P_18YMAS_F', 'P_18YMAS_M', 
        #            'P_3A5', 'P_3A5_F', 'P_3A5_M', 'P_6A11', 'P_6A11_F', 'P_6A11_M', 'P_8A14', 'P_8A14_F', 
        #            'P_8A14_M', 'P_12A14', 'P_12A14_F', 'P_12A14_M', 'P_15A17', 'P_15A17_F', 'P_15A17_M',
        #            'TVIVHAB', 'TOTHOG', 'PROM_OCUP', 'pobtot', 'GRAPROES', 'VPH_PISODT', 'PDESOCUP', 'PEA',
        #            'PDER_IMSS', 
        #            ] #at
        
        #for col in col_num:
        #    df[col] = pd.to_numeric(df[col], errors='coerce')

        miss_pder_imss = df['PDER_IMSS'].isnull().sum()
        if miss_pder_imss > 0: 
            logging.warning(f"pder_imss is not valid: {miss_pder_imss}")

        miss_pea = df['PEA'].isnull().sum()
        if miss_pea > 0: 
            logging.warning(f"pea is not valid: {miss_pea}")

        miss_pdesocup = df['PDESOCUP'].isnull().sum()
        if miss_pdesocup > 0: 
            logging.warning(f"PDESOCUP is not valid: {miss_pdesocup}")

        miss_vph_pisodt = df['VPH_PISODT'].isnull().sum()
        if miss_vph_pisodt > 0: 
            logging.warning(f"vph_pisodt is not valid: {miss_vph_pisodt}")

        miss_graproes = df['GRAPROES'].isnull().sum()
        if miss_graproes > 0: 
            logging.warning(f"graproes is not valid: {miss_graproes}")

        miss_pobtot1 = df['pobtot1'].isnull().sum()
        if miss_pobtot1 > 0: 
            logging.warning(f"pobtot1 is not valid: {miss_pobtot1}")

        miss_tothog = df['TOTHOG'].isnull().sum()
        if miss_tothog > 0: 
            logging.warning(f"tothog is not valid: {miss_tothog}")

        miss_tvivhab = df['TVIVHAB'].isnull().sum()
        if miss_tvivhab > 0: 
            logging.warning(f"tvivhab is not valid: {miss_tvivhab}")
        
        miss_prom_ocup = df['PROM_OCUP'].isnull().sum()
        if miss_prom_ocup > 0: 
            logging.warning(f"prom_ocup is not valid: {miss_prom_ocup}")
        
        miss_mun = df['MUN'].isnull().sum()
        if miss_mun > 0: 
            logging.warning(f"mun is not valid: {miss_mun}")

        miss_altitud = df['ALTITUD'].isnull().sum()
        if miss_altitud > 0: 
            logging.warning(f"altitud is not valid: {miss_altitud}")

        miss_pt = df['POBTOT'].isnull().sum()
        if miss_pt > 0: 
            logging.warning(f"población total is not valid: {miss_pt}")
        
        miss_pfem = df['POBFEM'].isnull().sum()
        if miss_pfem > 0: 
            logging.warning(f"pobfem is not valid: {miss_pfem}")
        
        miss_pmas = df['POBMAS'].isnull().sum()
        if miss_pmas > 0: 
            logging.warning(f"pobms is not valid: {miss_pmas}")
        
        miss_p_0a2 = df['P_0A2'].isnull().sum()
        if miss_p_0a2 > 0: 
            logging.warning(f"p_0a2 is not valid: {miss_p_0a2}")
        
        miss_p_0a2_f = df['P_0A2_F'].isnull().sum()
        if miss_p_0a2_f > 0: 
            logging.warning(f"p_0a2_f is not valid: {miss_p_0a2_f}")
        
        miss_p_0a2_m = df['P_0A2_M'].isnull().sum()
        if miss_p_0a2_m > 0: 
            logging.warning(f"p_0a2_m is not valid: {miss_p_0a2_m}")
        
        miss_p_3ymas = df['P_3YMAS'].isnull().sum()
        if miss_p_3ymas > 0: 
            logging.warning(f"p_3ymas is not valid: {miss_p_3ymas}")

        miss_p_3ymas_f = df['P_3YMAS_F'].isnull().sum()
        if miss_p_3ymas_f > 0: 
            logging.warning(f"p_3ymas_f is not valid: {miss_p_3ymas_f}")
        
        miss_p_3ymas_m = df['P_3YMAS_M'].isnull().sum()
        if miss_p_3ymas_m > 0: 
            logging.warning(f"p_3ymas_m is not valid: {miss_p_3ymas_m}")
        
        miss_p_5ymas = df['P_5YMAS'].isnull().sum()
        if miss_p_5ymas > 0: 
            logging.warning(f"p_5ymas is not valid: {miss_p_5ymas}")

        miss_p_5ymas_f = df['P_5YMAS_F'].isnull().sum()
        if miss_p_5ymas_f > 0: 
            logging.warning(f"p_5ymas_f is not valid: {miss_p_5ymas_f}")
        
        miss_p_5ymas_m = df['P_5YMAS_M'].isnull().sum()
        if miss_p_5ymas_m > 0: 
            logging.warning(f"p_5ymas is not valid: {miss_p_5ymas_m}")
        
        miss_p_18ymas = df['P_18YMAS'].isnull().sum()
        if miss_p_18ymas > 0: 
            logging.warning(f"p_5ymas is not valid: {miss_p_18ymas}")
         
        miss_p_18ymas_f = df['P_18YMAS_F'].isnull().sum()
        if miss_p_18ymas_f > 0: 
            logging.warning(f"p_18ymas_f is not valid: {miss_p_18ymas_f}")
        
        miss_p_18ymas_m = df['P_18YMAS_M'].isnull().sum()
        if miss_p_18ymas_m > 0: 
            logging.warning(f"p_18ymas_m is not valid: {miss_p_18ymas_m}")
         
        miss_p_15ymas = df['P_15YMAS'].isnull().sum() 
        if miss_p_15ymas > 0: 
            logging.warning(f"p_15ymas is not valid: {miss_p_15ymas}")

        miss_p_15ymas_f = df['P_15YMAS_F'].isnull().sum()
        if miss_p_15ymas_f > 0: 
            logging.warning(f"p_15ymas_f is not valid: {miss_p_15ymas_f}")
        
        miss_p_15ymas_m = df['P_15YMAS_M'].isnull().sum()
        if miss_p_15ymas_m > 0: 
            logging.warning(f"p_15ymas is not valid: {miss_p_15ymas_m}")
        
        miss_p_12ymas = df['P_12YMAS'].isnull().sum() 
        if miss_p_12ymas > 0: 
            logging.warning(f"p_15ymas is not valid: {miss_p_12ymas}")

        miss_p_12ymas_f = df['P_12YMAS_F'].isnull().sum()
        if miss_p_12ymas_f > 0: 
            logging.warning(f"p_12ymas_f is not valid: {miss_p_12ymas_f}")
        
        miss_p_12ymas_m = df['P_12YMAS_M'].isnull().sum()
        if miss_p_12ymas_m > 0: 
            logging.warning(f"p_12ymas is not valid: {miss_p_12ymas_m}")
        
        miss_p_3a5 = df['P_3A5'].isnull().sum() 
        if miss_p_3a5 > 0: 
            logging.warning(f"p_3a5 is not valid: {miss_p_3a5}")

        miss_p_3a5_f = df['P_3A5_F'].isnull().sum()
        if miss_p_3a5_f > 0: 
            logging.warning(f"p_3a5_f is not valid: {miss_p_3a5_f}")
        
        miss_p_3a5_m = df['P_3A5_M'].isnull().sum()
        if miss_p_3a5_m > 0: 
            logging.warning(f"p_3a5 is not valid: {miss_p_3a5_m}")
        
        miss_p_6a11 = df['P_6A11'].isnull().sum() 
        if miss_p_6a11 > 0: 
            logging.warning(f"p_6a11 is not valid: {miss_p_6a11}")
        
        miss_p_6a11_f = df['P_6A11_F'].isnull().sum() 
        if miss_p_6a11_f > 0: 
            logging.warning(f"p_6a11_f is not valid: {miss_p_6a11_f}")
        
        miss_p_6a11_m = df['P_6A11_M'].isnull().sum() 
        if miss_p_6a11_m > 0: 
            logging.warning(f"p_6a11_m is not valid: {miss_p_6a11_m}")
        
        miss_p_8a14 = df['P_8A14'].isnull().sum() 
        if miss_p_8a14 > 0: 
            logging.warning(f"p_8a14 is not valid: {miss_p_8a14}")
        
        miss_p_8a14_f = df['P_8A14_F'].isnull().sum() 
        if miss_p_8a14_f > 0: 
            logging.warning(f"p_8a14_f is not valid: {miss_p_8a14_f}")

        miss_p_8a14_m = df['P_8A14_M'].isnull().sum() 
        if miss_p_8a14_m > 0: 
            logging.warning(f"p_8a14_m is not valid: {miss_p_8a14_m}")
        
        miss_p_12a14 = df['P_12A14'].isnull().sum() 
        if miss_p_12a14 > 0: 
            logging.warning(f"p_12a14 is not valid: {miss_p_12a14}")
                
        miss_p_12a14_m = df['P_12A14_M'].isnull().sum() 
        if miss_p_12a14_m > 0: 
            logging.warning(f"p_12a14_m is not valid: {miss_p_12a14_m}")
        
        miss_p_12a14_f = df['P_12A14_F'].isnull().sum() 
        if miss_p_12a14_f > 0: 
            logging.warning(f"p_12a14_f is not valid: {miss_p_12a14_f}")

        miss_p_15a17 = df['P_15A17'].isnull().sum() 
        if miss_p_15a17 > 0: 
            logging.warning(f"p_15a17_f is not valid: {miss_p_15a17}")

        miss_p_15a17_m = df['P_15A17_M'].isnull().sum() 
        if miss_p_15a17_m > 0: 
            logging.warning(f"p_15a17_m is not valid: {miss_p_15a17_m}")      
        
        miss_p_15a17_f = df['P_15A17_F'].isnull().sum() 
        if miss_p_15a17_f > 0: 
            logging.warning(f"p_15a17_f is not valid: {miss_p_15a17_f}")      
        

        #rellenamos los datos faltantes con la media

        df['PDER_IMSS'] = df["PDER_IMSS"].replace('*', np.nan)
        media_pder_imss = df['PDER_IMSS'].mean()
        df['PDER_IMSS'] = df['PDER_IMSS'].fillna(media_pder_imss)

        df['PEA'] = df["PEA"].replace('*', np.nan)
        media_pea = df['PEA'].mean()
        df['PEA'] = df['PEA'].fillna(media_pea)

        df['PDESOCUP'] = df["PDESOCUP"].replace('*', np.nan)
        media_pdesocup = df['PDESOCUP'].mean()
        df['PDESOCUP'] = df['PDESOCUP'].fillna(media_pdesocup)

        df['VPH_PISODT'] = df["VPH_PISODT"].replace('*', np.nan)
        media_vph_pisodt = df['VPH_PISODT'].mean()
        df['VPH_PISODT'] = df['VPH_PISODT'].fillna(media_vph_pisodt)

        df['GRAPROES'] = df["GRAPROES"].replace('*', np.nan)
        media_graproes = df['GRAPROES'].mean()
        df['GRAPROES'] = df['GRAPROES'].fillna(media_graproes)

        df['pobtot'] = df["pobtot"].replace('*', np.nan)
        media_pobtot1 = df['pobtot'].mean()
        df['pobtot'] = df['pobtot'].fillna(media_pobtot1)

        df['PROM_OCUP'] = df["PROM_OCUP"].replace('*', np.nan)
        media_prom_ocup = df['PROM_OCUP'].mean()
        df['PROM_OCUP'] = df['PROM_OCUP'].fillna(media_prom_ocup)

        df['TVIVHAB'] = df["TVIVHAB"].replace('*', np.nan)
        media_tvivhab = df['TVIVHAB'].mean()
        df['TVIVHAB'] = df['TVIVHAB'].fillna(media_tvivhab) 

        df['TOTHOG'] = df["TOTHOG"].replace('*', np.nan)
        media_tothog = df['TOTHOG'].mean()
        df['TOTHOG'] = df['TOTHOG'].fillna(media_tothog)

        df['MUN'] = df["MUN"].replace('*', np.nan)
        media_mun = df['MUN'].mean()
        df['MUN'] = df['MUN'].fillna(media_mun)

        df['ENTIDAD'] = df["ENTIDAD"].replace('*', np.nan)
        media_entidad = df['ENTIDAD'].mean()
        df['ENTIDAD'] = df['ENTIDAD'].fillna(media_entidad)

        df['ALTITUD'] = df["ALTITUD"].replace('*', np.nan)
        media_altitud = df['ALTITUD'].mean()
        df['ALTITUD'] = df['ALTITUD'].fillna(media_altitud)
        
        df['POBTOT'] = df["POBTOT"].replace('*', np.nan)
        media_pobtot = df['POBTOT'].mean()
        df['POBTOT'] = df['POBTOT'].fillna(media_pobtot)
        
        df['POBFEM'] = df["POBFEM"].replace('*', np.nan)
        media_pobfem = df['POBFEM'].mean()
        df['POBFEM'] = df['POBFEM'].fillna(media_pobfem)
        
        df['POBMAS'] = df["POBMAS"].replace('*', np.nan)
        media_pobmas = df['POBMAS'].mean()
        df['POBMAS'] = df['POBMAS'].fillna(media_pobmas)
   
        df['P_0A2'] = df["P_0A2"].replace('*', np.nan)
        media_p_0a2 = df['P_0A2'].mean()
        df['P_0A2'] = df['P_0A2'].fillna(media_p_0a2)

        df['P_0A2_F'] = df["P_0A2_F"].replace('*', np.nan)
        media_p_0a2_f = df['P_0A2_F'].mean()
        df['P_0A2_F'] = df['P_0A2_F'].fillna(media_p_0a2_f)

        df['P_0A2_M'] = df["P_0A2_M"].replace('*', np.nan)
        media_p_0a2_m = df['P_0A2_M'].mean()
        df['P_0A2_M'] = df['P_0A2_M'].fillna(media_p_0a2_m)

        df['P_3YMAS'] = df["P_3YMAS"].replace('*', np.nan)
        media_p_3ymas = df['P_3YMAS'].mean()
        df['P_3YMAS'] = df['P_3YMAS'].fillna(media_p_3ymas)

        df['P_3YMAS_M'] = df["P_3YMAS_M"].replace('*', np.nan)
        media_p_3ymas_m = df['P_3YMAS_M'].mean()
        df['P_3YMAS_M'] = df['P_3YMAS_M'].fillna(media_p_3ymas_m)

        df['P_3YMAS_F'] = df["P_3YMAS_F"].replace('*', np.nan)
        media_p_3ymas_f = df['P_3YMAS_F'].mean()
        df['P_3YMAS_F'] = df['P_3YMAS_F'].fillna(media_p_3ymas_f)

        df['P_5YMAS'] = df["P_5YMAS"].replace('*', np.nan)
        media_p_5ymas = df['P_5YMAS'].mean()
        df['P_5YMAS'] = df['P_5YMAS'].fillna(media_p_5ymas)

        df['P_5YMAS_M'] = df["P_5YMAS_M"].replace('*', np.nan)
        media_p_5ymas_m = df['P_5YMAS_M'].mean()
        df['P_5YMAS_M'] = df['P_5YMAS_M'].fillna(media_p_5ymas_m)

        df['P_5YMAS_F'] = df["P_5YMAS_F"].replace('*', np.nan)
        media_p_5ymas_f = df['P_5YMAS_F'].mean()
        df['P_5YMAS_F'] = df['P_5YMAS_F'].fillna(media_p_5ymas_f)

        df['P_12YMAS'] = df["P_12YMAS"].replace('*', np.nan)
        media_p_12ymas = df['P_12YMAS'].mean()
        df['P_12YMAS'] = df['P_12YMAS'].fillna(media_p_12ymas)

        df['P_12YMAS_M'] = df["P_12YMAS_M"].replace('*', np.nan)
        media_p_12ymas_m = df['P_12YMAS_M'].mean()
        df['P_12YMAS_M'] = df['P_12YMAS_M'].fillna(media_p_12ymas_m)

        df['P_12YMAS_F'] = df["P_12YMAS_F"].replace('*', np.nan)
        media_p_12ymas_f = df['P_12YMAS_F'].mean()
        df['P_12YMAS_F'] = df['P_12YMAS_F'].fillna(media_p_12ymas_f)

        df['P_15YMAS'] = df["P_15YMAS"].replace('*', np.nan)
        media_p_15ymas = df['P_15YMAS'].mean()
        df['P_15YMAS'] = df['P_15YMAS'].fillna(media_p_15ymas)

        df['P_15YMAS_M'] = df["P_15YMAS_M"].replace('*', np.nan)
        media_p_15ymas_m = df['P_15YMAS_M'].mean()
        df['P_15YMAS_M'] = df['P_15YMAS_M'].fillna(media_p_15ymas_m)

        df['P_15YMAS_F'] = df["P_15YMAS_F"].replace('*', np.nan)
        media_p_15ymas_f = df['P_15YMAS_F'].mean()
        df['P_15YMAS_F'] = df['P_15YMAS_F'].fillna(media_p_15ymas_f)
                
        df['P_18YMAS'] = df["P_18YMAS"].replace('*', np.nan)
        media_p_18ymas = df['P_18YMAS'].mean()
        df['P_18YMAS'] = df['P_18YMAS'].fillna(media_p_18ymas)

        df['P_18YMAS_M'] = df["P_18YMAS_M"].replace('*', np.nan)
        media_p_18ymas_m = df['P_18YMAS_M'].mean()
        df['P_18YMAS_M'] = df['P_18YMAS_M'].fillna(media_p_18ymas_m)

        df['P_18YMAS_F'] = df["P_18YMAS_F"].replace('*', np.nan)
        media_p_18ymas_f = df['P_18YMAS_F'].mean()
        df['P_18YMAS_F'] = df['P_18YMAS_F'].fillna(media_p_18ymas_f)

        df['P_3A5'] = df["P_3A5"].replace('*', np.nan)
        media_p_3a5 = df['P_3A5'].mean()
        df['P_3A5'] = df['P_3A5'].fillna(media_p_3a5)

        df['P_3A5_F'] = df["P_3A5_F"].replace('*', np.nan)
        media_p_3a5_f = df['P_3A5_F'].mean()
        df['P_3A5_F'] = df['P_3A5_F'].fillna(media_p_3a5_f)

        df['P_3A5_M'] = df["P_3A5_M"].replace('*', np.nan)
        media_p_3a5_m = df['P_3A5_M'].mean()
        df['P_3A5_M'] = df['P_3A5_M'].fillna(media_p_3a5_m)
 
        df['P_6A11'] = df["P_6A11"].replace('*', np.nan)
        media_p_6a11 = df['P_6A11'].mean()
        df['P_6A11'] = df['P_6A11'].fillna(media_p_6a11)

        df['P_6A11_F'] = df["P_6A11_F"].replace('*', np.nan)
        media_p_6a11_f = df['P_6A11_F'].mean()
        df['P_6A11_F'] = df['P_6A11_F'].fillna(media_p_6a11_f)

        df['P_6A11_M'] = df["P_6A11_M"].replace('*', np.nan)
        media_p_6a11_m = df['P_6A11_M'].mean()
        df['P_6A11_M'] = df['P_6A11_M'].fillna(media_p_6a11_m)

        df['P_8A14'] = df["P_8A14"].replace('*', np.nan)
        media_p_8a14 = df['P_8A14'].mean()
        df['P_8A14'] = df['P_8A14'].fillna(media_p_8a14)

        df['P_8A14_F'] = df["P_8A14_F"].replace('*', np.nan)
        media_p_8a14_f = df['P_8A14_F'].mean()
        df['P_8A14_F'] = df['P_8A14_F'].fillna(media_p_8a14_f)

        df['P_8A14_M'] = df["P_8A14_M"].replace('*', np.nan)
        media_p_8a14_m = df['P_8A14_M'].mean()
        df['P_8A14_M'] = df['P_8A14_M'].fillna(media_p_8a14_m)

        df['P_12A14'] = df["P_12A14"].replace('*', np.nan)
        media_p_12a14 = df['P_12A14'].mean()
        df['P_12A14'] = df['P_12A14'].fillna(media_p_12a14)

        df['P_12A14_F'] = df["P_12A14_F"].replace('*', np.nan)
        media_p_12a14_f = df['P_12A14_F'].mean()
        df['P_12A14_F'] = df['P_12A14_F'].fillna(media_p_12a14_f)

        df['P_12A14_M'] = df["P_12A14_M"].replace('*', np.nan)
        media_p_12a14_m = df['P_12A14_M'].mean()
        df['P_12A14_M'] = df['P_12A14_M'].fillna(media_p_12a14_m)

        df['P_15A17'] = df["P_15A17"].replace('*', np.nan)
        media_p_15a17 = df['P_15A17'].mean()
        df['P_15A17'] = df['P_15A17'].fillna(media_p_15a17)

        df['P_15A17_F'] = df["P_15A17_F"].replace('*', np.nan)
        media_p_15a17_f = df['P_15A17_F'].mean()
        df['P_15A17_F'] = df['P_15A17_F'].fillna(media_p_15a17_f)

        df['P_15A17_M'] = df["P_15A17_M"].replace('*', np.nan)
        media_p_15a17_m = df['P_15A17_M'].mean()
        df['P_15A17_M'] = df['P_15A17_M'].fillna(media_p_15a17_m)

        def densidad_poblacional(mun):
            areas = {
                2: 35.9, 0: 1485.01, 4: 74.58, 3: 54.40, 
                6: 23.30, 7: 117.01, 8: 74.58, 9: 228.41,
                10: 96.17, 11: 85.34, 12: 340.07, 13: 118.01,
                14: 26.63, 15: 32.40, 16: 46.99, 17: 96.17
            }
            return areas.get(mun, None)
        
        df['area_alcaldia'] = df['MUN'].apply(densidad_poblacional)

        df['densidad_por_alcaldia'] = df['POBTOT']/df['area_alcaldia']

        #hogares por vivienda

        df['hogares_por_vivienda'] = df['TOTHOG']/df['TVIVHAB']

        #tasa_anual

        df['tasa_anual'] = abs((df['POBTOT']/(df['pobtot']))**0.1 - 1)

        #proyección a 5 años

        df['factor_5anos_crecimiento'] = (1 + df['tasa_anual'])**5

        #poblacion_2025

        df['poblacion_2025'] = df['POBTOT']*df['factor_5anos_crecimiento']

        #categorizacion de educacion: 

        EDU = df['GRAPROES']

        def clasi_educacion(EDU):
            if EDU > 12: return 'alto'
            elif 9 <= EDU <=12: return 'medio'
            elif  EDU < 9: return 'bajo' 
            else: 
                return 'desconocido'
            
        df['categorizacion_educacion'] = EDU.apply(clasi_educacion)

        #categori_nivel_educativo 

        def clasi_nivel_educativo(EDU):
            if EDU > 12: return 'preparatoria o mas'
            elif 9 <= EDU <=12: return 'secundaria'
            elif  EDU < 9: return 'primaria' 
            else: 
                return 'desconocido'
            
        df['categorizacion_nivel_educativo'] = EDU.apply(clasi_nivel_educativo)

        #piso digno 
        
        df['porcentaje_piso_digno'] = min((df['VPH_PISODT']/df['TVIVHAB']) * (100))

        #tasa de desocupación: 

        df['porcentaje_tasa_desocupacion'] = (df['PDESOCUP']/df['PEA']) * (100)

        #porcentaje con seguro social 

        df['porcentaje_con_seguro_social'] = (df['PDER_IMSS']/df['POBTOT']) * (100)

        ###############################################
        #crea la base de datos
        ###############################################

        def crear_base_dato():
            try: 
                conn = psycopg2.connect(
                        host=DB_CONFIG['host'], 
                        port=DB_CONFIG['port'],
                        user=DB_CONFIG['user'],
                        password = DB_CONFIG['password'],
                        dbname = 'postgres'

                )

                conn.autocommit=True
                cursor = conn.cursor()

                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONFIG['database']}';")
                exists = cursor.fetchone()
                
                if not exists: 
                   cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']};")
                   print(f"Base de datos '{DB_CONFIG['database']}' creada con exito") 
                else: 
                   print(f"la base de datos '{DB_CONFIG['database']}")

                cursor.close()
                conn.close()  
            
            except psycopg2.Error as e: 
                if "already exists" in str(e):
                    print(f"La base de datos {DB_CONFIG['database']} ya existe")
                else: 
                    print(f"error creando la base de datos: {e}")
        #crea el csv limpio

        def crear_columnas():
            try: 
                conn=psycopg2.connect(**DB_CONFIG)
                cursor = conn.cursor()
                create_table_query = """
                CREATE TABLE IF NOT EXISTS population_data(
                 ENTIDAD NUMERIC, 
                 MUN NUMERIC, 
                 LOC NUMERIC, 
                 ALTITUD NUMERIC, 
                 POBTOT2020 NUMERIC, 
                 POBFEM NUMERIC, 
                 POBMAS NUMERIC,
                 P_0A2 NUMERIC, 
                 P_0A2_M NUMERIC, 
                 P_0A2_F NUMERIC, 
                 P_3YMAS NUMERIC, 
                 P_3YMAS_M NUMERIC, 
                 P_3YMAS_F NUMERIC, 
                 P_5YMAS NUMERIC, 
                 P_5YMAS_M NUMERIC, 
                 P_5YMAS_F NUMERIC, 
                 P_15YMAS NUMERIC, 
                 P_15YMAS_M NUMERIC, 
                 P_15YMAS_F NUMERIC, 
                 P_12YMAS NUMERIC, 
                 P_12YMAS_M NUMERIC, 
                 P_12YMAS_F NUMERIC, 
                 P_18YMAS NUMERIC, 
                 P_18YMAS_M NUMERIC, 
                 P_18YMAS_F NUMERIC, 
                 P_3A5 NUMERIC, 
                 P_3A5_M NUMERIC, 
                 P_3A5_F NUMERIC, 
                 P_6A11 NUMERIC, 
                 P_6A11_M NUMERIC, 
                 P_6A11_F NUMERIC, 
                 P_8A14 NUMERIC, 
                 P_8A14_M NUMERIC, 
                 P_8A14_F NUMERIC, 
                 P_12A14_M NUMERIC, 
                 P_12A14_F NUMERIC,
                 P_15A17 NUMERIC, 
                 P_15A17_M NUMERIC, 
                 P_15A17_F NUMERIC,  
                 TVIVHAB NUMERIC, 
                 TOTHOG NUMERIC, 
                 PROM_OCUP NUMERIC, 
                 pobtot2010 NUMERIC, 
                 GRAPROES NUMERIC,
                 VPH_PISODT NUMERIC, 
                 PDESOCUP NUMERIC, 
                 PEA NUMERIC,
                 PDER_IMSS NUMERIC, 
                 area_alcadia NUMERIC,
                 densidad_por_alcadia NUMERIC, 
                 tasa_anual NUMERIC, 
                 factor_5anos_crecimiento NUMERIC,
                 población_2025 NUMERIC,
                 categorizacion_educacion VARCHAR(100),
                 categorizacion_nivel_educativo VARCHAR(100),
                 porcentaje_piso_digno NUMERIC, 
                 porcentaje_tasa_desocupacion NUMERIC, 
                 porcentaje_con_seguro_social NUMERIC
                );
                """
                cursor.execute(create_table_query)
                cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_population_data ON population_data(porcentaje_con_seguro_social);                 
                """)

                conn.commit()
                print("columnas creadas con exito")

            except psycopg2.Error as e: 
                print(f"error al crear las columnas: {e}")

        def cargar_datos_posgres(df):
            try: 
                engine = create_engine(
                    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
                )

                df.to_sql(
                    name = 'population_data',
                    con=engine, 
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                print("datos cargados con existo a postgresql")

            except Exception as e: 
                print(f"Error cargando datos a PosgreSQL: {e}")
                return False
        
        df.to_csv(limpios, index=False, encoding='utf-8')

        crear_base_dato()
        crear_columnas()
        cargar_datos_posgres(df)

        return True 


    except Exception as e: 
        print(f"error: {e}")
        return False


if __name__ == "__main__":
    pipeline(datos, limpios)