import duckdb
import os

def crear_base_datos():
    try:
        # Verificar y crear el directorio datawarehouse si no existe
        os.makedirs('bot_moviles_opsitel/datawarehouse', exist_ok=True)
        
        # Crear la conexión
        con = duckdb.connect('bot_moviles_opsitel/datawarehouse/bot_moviles_opsitel.db')
        
        # Crear la tabla con tipos de datos específicos
        con.sql("""
        CREATE TABLE IF NOT EXISTS bot_moviles (
            fecha_procesamiento DATE,
            numero VARCHAR,
            receptor VARCHAR,
            cedente VARCHAR,
            asignatario_original VARCHAR,
            fecha_ventana DATE,
            estado VARCHAR,
            dias_permanencia INTEGER
        )
        """)
        
        print("Base de datos y tabla creadas exitosamente")
        
    except Exception as e:
        print(f"Error al crear la base de datos: {str(e)}")
        
    finally:
        # Asegurar que la conexión se cierre incluso si hay un error
        if 'con' in locals():
            con.close()
            print("Conexión cerrada")

if __name__ == "__main__":
    crear_base_datos()