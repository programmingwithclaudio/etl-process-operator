# etl-reniec-segmentation.py
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from unidecode import unidecode


def get_reniec_data(db_url, table_name):
    engine = create_engine(db_url)
    query = text(f"""
    SELECT * FROM {table_name} WHERE sexo LIKE 1;
    """)
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)

    return df


def get_ubigeos_data(db_url, table_name):
    engine = create_engine(db_url)
    query = text(f"SELECT * FROM {table_name};")

    with engine.connect() as connection:
        df = pd.read_sql(query, connection)

    return df


def process_reniec_data(df):
    df['fecha_nac'] = pd.to_datetime(df['fecha_nac'], format='%Y-%m-%d', errors='coerce')
    today = pd.to_datetime('today')
    df['edad'] = today.year - df['fecha_nac'].dt.year - (
            (today.month < df['fecha_nac'].dt.month) |
            ((today.month == df['fecha_nac'].dt.month) & (today.day < df['fecha_nac'].dt.day))
    )
    df_filtrado = df[df['edad'].between(23, 63)]
    df_filtrado_ordenado = df_filtrado.sort_values(by='edad', ascending=True)
    df_filtrado_ordenado['ubigeo_nac'] = df_filtrado_ordenado['ubigeo_nac'].astype(str).str.strip()
    return df_filtrado_ordenado


class ReniecDataProcessor:
    def __init__(self, reniec_db_url, dacompany_db_url):
        self.reniec_db_url = reniec_db_url
        self.dacompany_db_url = dacompany_db_url

    def process_data(self):
        # Get RENIEC data
        reniec_df = get_reniec_data(self.reniec_db_url, 'reniec_lima')
        processed_reniec_df = process_reniec_data(reniec_df)

        # Get Ubigeos data
        ubigeos_df = get_ubigeos_data(self.dacompany_db_url, 'dbo.Ubigeos_Peru')
        ubigeos_df['Ubigeo'] = ubigeos_df['Ubigeo'].astype(str).str.strip()

        # Merge data
        result = pd.merge(processed_reniec_df, ubigeos_df, how="left", left_on="ubigeo_nac", right_on="Ubigeo")

        # Process final data
        data = result[["dni", "fecha_nac", "padre", "madre", "Departamento", "Provincia", "Distrito"]].dropna()
        data['new_padre'] = data['padre'].str.split().str[0]
        data['new_madre'] = data['madre'].str.split().str[0]
        data = data.drop(columns=['padre', 'madre'])

        # Reorder columns
        column_order = ["dni", "fecha_nac", "new_padre", "new_madre", "Departamento", "Provincia", "Distrito"]
        data = data[column_order]

        return data

    def get_dnis_to_process(self):
        data = self.process_data()

        # Convert fecha_nac to the required string format
        data['fecha_nac'] = pd.to_datetime(data['fecha_nac']).dt.strftime('%d/%m/%Y')

        # Convert DataFrame to list of tuples
        dnis_to_process = data.itertuples(index=False, name=None)

        return list(dnis_to_process)

# Example usage
if __name__ == "__main__":
    DATABASE_URL_RENIEC = 'mssql+pyodbc://sa:dacompany@192.168.101/Data_Phones?driver=ODBC+Driver+17+for+SQL+Server'
    DATABASE_URL_DACOMPANY = 'mssql+pyodbc://sa:dacompany@192.168.0.101/BaseDatos_Dacompany?driver=ODBC+Driver+17+for+SQL+Server'

    processor = ReniecDataProcessor(DATABASE_URL_RENIEC, DATABASE_URL_DACOMPANY)
    dnis_to_process = processor.get_dnis_to_process()

