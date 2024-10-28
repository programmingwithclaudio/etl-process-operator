import pandas as pd
import concurrent.futures
from sqlalchemy import create_engine
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import List

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    url: str
    table_name: str

class ReniecETLProcessor:
    def __init__(self, reniec_config: DatabaseConfig, dacompany_config: DatabaseConfig):
        self.reniec_config = reniec_config
        self.dacompany_config = dacompany_config
        self.chunk_size = 1000

    def get_dnis_to_process(self) -> List[str]:
        """Obtiene la lista de DNIs a procesar."""
        try:
            engine = create_engine(self.reniec_config.url)
            with engine.connect() as conn:
                query = f"""
                    SELECT dni FROM {self.reniec_config.table_name} 
                    WHERE sexo = 1
                """
                df = pd.read_sql(query, conn)
                return df['dni'].tolist()
        except Exception as e:
            logger.error(f"Error obteniendo DNIs: {e}")
            raise

    def process_chunk(self, dnis: List[str]) -> pd.DataFrame:
        """Procesa un chunk de DNIs."""
        try:
            reniec_engine = create_engine(self.reniec_config.url)
            dacompany_engine = create_engine(self.dacompany_config.url)

            dnis_str = "','".join(dnis)
            query = f"""
                SELECT dni, fecha_nac, padre, madre, ubigeo_nac 
                FROM {self.reniec_config.table_name}
                WHERE dni IN ('{dnis_str}')
            """
            
            with reniec_engine.connect() as conn:
                reniec_df = pd.read_sql(query, conn)

            with dacompany_engine.connect() as conn:
                ubigeos_df = pd.read_sql(f"SELECT * FROM {self.dacompany_config.table_name}", conn)

            reniec_df['fecha_nac'] = pd.to_datetime(reniec_df['fecha_nac'])
            reniec_df['ubigeo_nac'] = reniec_df['ubigeo_nac'].astype(str).str.strip()
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

            return result[["dni", "fecha_nac", "new_padre", "new_madre", 
                          "Departamento", "Provincia", "Distrito"]]

        except Exception as e:
            logger.error(f"Error procesando chunk: {e}")
            raise

def process_reniec_data():
    """Función principal que ejecuta el procesamiento en paralelo localmente."""
    try:
        reniec_config = DatabaseConfig(
            url='mssql+pyodbc://sa:dacompany@192.168.101/Data_Phones?driver=ODBC+Driver+17+for+SQL+Server',
            table_name='reniec_lima'
        )
        
        dacompany_config = DatabaseConfig(
            url='mssql+pyodbc://sa:dacompany@192.168.101/BaseDatos_Dacompany?driver=ODBC+Driver+17+for+SQL+Server',
            table_name='dbo.Ubigeos_Peru'
        )

        processor = ReniecETLProcessor(reniec_config, dacompany_config)
        
        logger.info("Obteniendo lista de DNIs...")
        all_dnis = processor.get_dnis_to_process()
        
        chunks = [all_dnis[i:i + processor.chunk_size] 
                 for i in range(0, len(all_dnis), processor.chunk_size)]
        
        logger.info(f"Procesando {len(chunks)} chunks...")
        
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_chunk = {executor.submit(processor.process_chunk, chunk): chunk 
                             for chunk in chunks}
            
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    data = future.result()
                    results.append(data)
                    logger.info(f"Chunk procesado exitosamente: {len(chunk)} registros") #necesito que escriba directamente el csv no en memoeria please
                except Exception as e:
                    logger.error(f"Error en chunk: {e}")

        if results:
            final_df = pd.concat(results, ignore_index=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"reniec_processed_{timestamp}.csv"
            final_df.to_csv(output_file, index=False)
            logger.info(f"Proceso completado. Resultados guardados en: {output_file}")
            
            return final_df
        
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error en el flujo principal: {e}")
        raise

if __name__ == "__main__":
    process_reniec_data()
