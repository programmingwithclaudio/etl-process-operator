import pandas as pd
import re
import glob
import chardet
from datetime import datetime
import numpy as np
import os

def clean_value(value):
    """
    Limpia un valor eliminando el prefijo 'XX:' y espacios en blanco.
    """
    if ':' in value:
        return value.split(':', 1)[1].strip()
    return value.strip()

def normalize_line(line):
    """
    Normaliza los caracteres especiales de una línea.
    """
    replacements = {
        'Ã³': 'ó', 'Ã¡': 'á', 'Ã©': 'é', 'Ãº': 'ú', 'Ã±': 'ñ', 'â': '',
        'ï¿½': 'ú', 'ï': 'í', 'N�': 'Nú', '�': 'ó'
    }
    for old, new in replacements.items():
        line = line.replace(old, new)
    return line.strip()

def process_line(line, data):
    """
    Procesa una línea y la agrega a la lista de datos si es válida.
    """
    line = normalize_line(line)

    # Caso de "No se encontraron resultados"
    if "No se encontraron resultados" in line:
        numero_match = re.search(r'Número: (\d+)', line)
        if numero_match:
            numero = numero_match.group(1)
            data.append({
                'Fecha de procesamiento': 'N/A',
                'Número': numero,
                'Receptor': 'N/A',
                'Cedente': 'N/A',
                'Asignatario Original': 'N/A',
                'Fecha de la ventana': 'N/A',
                'Estado': 'No se encontraron resultados'
            })
        return

    # Normaliza y limpia la línea
    parts = line.split(',')
    parts = [clean_value(part) for part in parts]  # Limpia cada parte

    # Verificar si hay suficientes partes
    if len(parts) >= 6:
        record = {}
        fields = [
            'Fecha de procesamiento',
            'Número',
            'Receptor',
            'Cedente',
            'Asignatario Original',
            'Fecha de la ventana',
            'Estado'
        ]
        
        for i, field in enumerate(fields):
            if i < len(parts):
                record[field] = parts[i]
            else:
                record[field] = 'N/A'

        # Verifica que el número sea válido
        if record['Número'] and re.match(r'^\d+$', record['Número']):
            data.append(record)
    
    # Manejo de líneas con números parciales
    else:
        numero_match = re.search(r'Número: (\d+)', line)
        if numero_match:
            numero = numero_match.group(1)
            data.append({
                'Fecha de procesamiento': 'N/A',
                'Número': numero,
                'Receptor': 'N/A',
                'Cedente': 'N/A',
                'Asignatario Original': 'N/A',
                'Fecha de la ventana': 'N/A',
                'Estado': 'Línea procesada parcialmente'
            })

def process_file(file_path, data):
    """
    Procesa un archivo de texto línea por línea y acumula los registros en 'data'.
    """
    try:
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            encoding = chardet.detect(raw_data)['encoding']
        
        with open(file_path, 'r', encoding=encoding, errors='replace') as file:
            for line in file:
                if line.strip():
                    process_line(line, data)
                    
    except Exception as e:
        print(f"Error procesando archivo {file_path}: {e}")

def process_files_in_directory(directory_pattern):
    """
    Procesa todos los archivos de texto en el directorio especificado por 'directory_pattern'.
    """
    data = []
    for file_name in glob.glob(directory_pattern):
        print(f"Procesando archivo: {file_name}")
        process_file(file_name, data)
    
    return pd.DataFrame(data)

def clean_dataframe(df):
    """
    Limpia y procesa el DataFrame, eliminando duplicados y formateando fechas.
    También elimina las filas donde el campo 'Estado' contiene 'No se encontraron resultados'.
    """
    # Evita advertencias copiando el DataFrame
    df = df.drop_duplicates(subset=['Número']).copy() 
    df.replace(['N/A', '-', ''], np.nan, inplace=True)

    # Asegura que las columnas de fecha existan antes de procesarlas
    if 'Fecha de procesamiento' in df.columns and 'Fecha de la ventana' in df.columns:
        for fecha_col in ['Fecha de procesamiento', 'Fecha de la ventana']:
            # Extrae solo la parte de la fecha si hay datos adicionales en el texto
            df[fecha_col] = df[fecha_col].apply(lambda x: x.split()[0] if isinstance(x, str) and ' ' in x else x)
            # Convierte a fecha; si falla, deja el valor como NaT
            df[fecha_col] = pd.to_datetime(df[fecha_col], format='%d/%m/%Y', errors='coerce')
    
    # Calcula los días de permanencia solo si 'Fecha de la ventana' está en formato fecha
    if 'Fecha de la ventana' in df.columns:
        # Verifica que 'Fecha de la ventana' esté en formato datetime
        df['Días de permanencia'] = (datetime.now() - df['Fecha de la ventana']).dt.days
        df['Días de permanencia'] = df['Días de permanencia'].where(df['Fecha de la ventana'].notna(), np.nan)

    # Elimina filas donde el campo 'Estado' contiene 'No se encontraron resultados'
    df = df[~df['Estado'].str.contains('No se encontraron resultados', na=False)]

    df = df.rename(columns={
    'Fecha de procesamiento': 'fecha_procesamiento',
    'Número': 'numero',
    'Receptor': 'receptor',
    'Cedente': 'cedente',
    'Asignatario Original': 'asignatario_original',
    'Fecha de la ventana': 'fecha_ventana',
    'Estado': 'estado',
    'Días de permanencia': 'dias_permanencia'
    })

    return df


def save_to_csv(df, output_path):
    """
    Guarda el DataFrame en un archivo CSV.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)    
    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Archivo guardado exitosamente en: {output_path}")
    except Exception as e:
        print(f"Error al guardar el archivo: {e}")

# Main Execution
if __name__ == "__main__":
    file_pattern = r'C:\Users\Dacompany\ti\etl-process-operator\datasets\2510\*.txt'
    output_path = r'C:\Users\Dacompany\ti\etl-process-operator\datasets\output\resultadoreniec2610.csv'
    
    df = process_files_in_directory(file_pattern)
    df = clean_dataframe(df)
    
    print("\nEstadísticas del procesamiento:")
    print(f"Total de registros procesados: {len(df)}")
    # print(f"Registros por estado:\n{df['estado'].value_counts()}")
    
    save_to_csv(df, output_path)