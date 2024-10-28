import pandas as pd
import duckdb

class Database:
    def __init__(self, db_path):
        self.con = duckdb.connect(db_path)

    def fetch_existing_data(self):
        return self.con.execute("SELECT * FROM bot_moviles").fetchdf()

    def close(self):
        self.con.close()


class DataProcessor:
    def __init__(self, existing_data):
        self.existing_data = existing_data

    def filter_new_data(self, df):
        numeros_excluir = self.existing_data['numero'].tolist()
        return df[~df['numero'].isin(numeros_excluir)]


class DataPipeline:
    def __init__(self, db_path, input_csv_path, output_csv_path):
        self.db = Database(db_path)
        self.input_csv_path = input_csv_path
        self.output_csv_path = output_csv_path

    def run(self):
        # Cargar datos existentes
        existing_data = self.db.fetch_existing_data()

        # Cargar el DataFrame desde el archivo CSV
        df2 = pd.read_csv(self.input_csv_path, encoding='utf-8', dtype=str)
        df2.rename(columns={'Número': 'numero'}, inplace=True)

        # Obtener números únicos
        df2ti = df2[df2['numero'].drop_duplicates().index]

        # Filtrar nuevos datos
        processor = DataProcessor(existing_data)
        filtered_data = processor.filter_new_data(df2ti)

        # Guardar resultados si hay datos nuevos
        if not filtered_data.empty:
            filtered_data.to_csv(self.output_csv_path, index=False, encoding='utf-8')
            print(f"Archivo guardado en: {self.output_csv_path}")
        else:
            print("No hay datos nuevos para guardar.")

        # Cerrar la conexión a la base de datos
        self.db.close()


def main():
    db_path = 'bot_moviles_opsitel/datawarehouse/bot_moviles_opsitel.db'
    input_csv_path = r'datasets\output\resultadoreniec.csv'
    output_csv_path = r'datasets\output\sobrantes.csv'

    pipeline = DataPipeline(db_path, input_csv_path, output_csv_path)
    pipeline.run()


if __name__ == "__main__":
    main()
