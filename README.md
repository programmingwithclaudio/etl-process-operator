Voy a ayudarte a integrar Prefect y organizar las dependencias necesarias.



```python
# etl_reniec_prefect.py
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import List, Tuple
import logging
from dataclasses import dataclass
from contextlib import contextmanager
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    url: str
    table_name: str

@task(retries=3, retry_delay_seconds=30, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def get_database_connection(db_url: str):
    """Task para crear conexión a la base de datos con reintentos."""
    return create_engine(db_url)

@task(retries=2, retry_delay_seconds=60)
def get_data_from_db(engine, table_name: str, where_clause: str = "") -> pd.DataFrame:
    """Task para obtener datos de la base de datos."""
    try:
        with engine.connect() as connection:
            query = f"SELECT * FROM {table_name} {where_clause}"
            return pd.read_sql(text(query), connection)
    except Exception as e:
        logger.error(f"Error al obtener datos de {table_name}: {str(e)}")
        raise

@task
def calculate_age(birth_dates: pd.Series) -> pd.Series:
    """Task para calcular edades de manera vectorizada."""
    today = pd.Timestamp.today()
    return today.year - birth_dates.dt.year - (
        (today.month < birth_dates.dt.month) |
        ((today.month == birth_dates.dt.month) & (today.day < birth_dates.dt.day))
    )

@task
def process_reniec_data(df: pd.DataFrame, age_range: tuple = (23, 63)) -> pd.DataFrame:
    """Task para procesar datos de RENIEC."""
    try:
        df['fecha_nac'] = pd.to_datetime(df['fecha_nac'], format='%Y-%m-%d', errors='coerce')
        df['edad'] = calculate_age(df['fecha_nac'])
        
        mask = df['edad'].between(*age_range)
        df_processed = df[mask].sort_values('edad')
        df_processed['ubigeo_nac'] = df_processed['ubigeo_nac'].astype(str).str.strip()
        
        return df_processed
    except Exception as e:
        logger.error(f"Error en el procesamiento de datos RENIEC: {str(e)}")
        raise

@task
def merge_and_process_data(reniec_df: pd.DataFrame, ubigeos_df: pd.DataFrame) -> pd.DataFrame:
    """Task para combinar y procesar datos finales."""
    try:
        ubigeos_df['Ubigeo'] = ubigeos_df['Ubigeo'].astype(str).str.strip()
        
        result = pd.merge(
            reniec_df,
            ubigeos_df,
            how="inner",
            left_on="ubigeo_nac",
            right_on="Ubigeo"
        )

        result['new_padre'] = result['padre'].str.split().str[0]
        result['new_madre'] = result['madre'].str.split().str[0]

        final_columns = ["dni", "fecha_nac", "new_padre", "new_madre", 
                        "Departamento", "Provincia", "Distrito"]
        
        return result[final_columns]
    except Exception as e:
        logger.error(f"Error en el merge y procesamiento: {str(e)}")
        raise

@task
def format_output_data(data: pd.DataFrame) -> List[Tuple]:
    """Task para formatear datos de salida."""
    data = data.copy()
    data['fecha_nac'] = data['fecha_nac'].dt.strftime('%d/%m/%Y')
    return list(data.itertuples(index=False, name=None))

@flow(name="ETL Reniec Flow", description="Flujo ETL para procesamiento de datos RENIEC")
def etl_reniec_flow(
    reniec_config: DatabaseConfig,
    dacompany_config: DatabaseConfig
) -> List[Tuple]:
    """Flujo principal de ETL para datos RENIEC."""
    try:
        # Crear conexiones
        reniec_engine = get_database_connection(reniec_config.url)
        dacompany_engine = get_database_connection(dacompany_config.url)

        # Obtener datos
        reniec_df = get_data_from_db(
            reniec_engine,
            reniec_config.table_name,
            "WHERE sexo = 1"
        )
        ubigeos_df = get_data_from_db(
            dacompany_engine,
            dacompany_config.table_name
        )

        # Procesar datos
        processed_reniec = process_reniec_data(reniec_df)
        merged_data = merge_and_process_data(processed_reniec, ubigeos_df)
        final_data = format_output_data(merged_data)

        logger.info(f"Procesamiento completado. Total de registros: {len(final_data)}")
        return final_data

    except Exception as e:
        logger.error(f"Error en el flujo ETL: {str(e)}")
        raise

if __name__ == "__main__":
    reniec_config = DatabaseConfig(
        url='mssql+pyodbc://sa:dacompany@192.168.101/Data_Phones?driver=ODBC+Driver+17+for+SQL+Server',
        table_name='reniec_piura'
    )
    
    dacompany_config = DatabaseConfig(
        url='mssql+pyodbc://sa:dacompany@192.168.0.101/BaseDatos_Dacompany?driver=ODBC+Driver+17+for+SQL+Server',
        table_name='dbo.Ubigeos_Peru'
    )

    etl_reniec_flow(reniec_config, dacompany_config)

```

```text
# Base de datos
sqlalchemy==2.0.23
pyodbc==4.0.39

# Procesamiento de datos
pandas==2.1.3
numpy==1.26.2

# Orquestación y monitoreo
prefect==2.14.4

# Utilidades
python-dotenv==1.0.0
typing-extensions==4.8.0

# Logging y monitoreo
structlog==23.2.0

```

Para instalar las dependencias, necesitas ejecutar:

```bash
pip install -r requirements.txt
pip install Unidecode
pip install SQLAlchemy
```

Las principales mejoras con Prefect incluyen:

1. **Orquestación de tareas**:
   - Cada operación importante es ahora una tarea de Prefect (@task)
   - El flujo principal está decorado con @flow
   - Reintentos automáticos para operaciones propensas a fallar

2. **Caché y optimización**:
   - Caché de conexiones a base de datos
   - Reintentos configurados para operaciones de DB
   - Control de tiempo de expiración de caché

3. **Monitoreo y logging**:
   - Mejor trazabilidad de errores
   - Monitoreo de estado de tareas
   - Métricas de ejecución

Para ejecutar el flujo con la UI de Prefect, necesitas:

1. Iniciar el servidor Prefect:

```bash
pip install --upgrade prefect griffe
pip install prefect sqlalchemy pandas pyodbc
```

```bash
prefect server start
```

2. En otra terminal, iniciar un worker:
```bash
prefect worker start -p default-agent-pool
```

3. Ejecutar el script:
```bash
python etl_reniec_prefect.py
```

También puedes programar el flujo para que se ejecute periódicamente:

```python
from prefect.schedules import IntervalSchedule
from datetime import timedelta

schedule = IntervalSchedule(interval=timedelta(days=1))
etl_reniec_flow.serve(schedule=schedule)
```

¿Te gustaría que profundice en algún aspecto específico de la implementación o que agreguemos alguna funcionalidad adicional?
---

```sql
SELECT 
    numero,
    DNI,
    CONCAT(AP_PAT, ' ', AP_MAT, ' ', NOMBRES) AS fullnombres,
	Deuda,
	Estado_Deuda,
	tipo_list,
	Tipo_Gestion,
	Activo,
	Tipo_Claro,
	Tipo_Cliente,
	Full_Claro,
	SBS,
	Cliente_Activo
FROM 
    Base_Clientes_Lima;
```
