import pandas as pd
import os
import logging



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LoadData:
    def __init__(self, input_path, output_path, output_format):
        self.input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", input_path))
        self.output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", output_path))
        self.output_format = output_format.lower()

    def load_transformed_data(self):
        logging.info("Membaca data hasil transformasi...")
        df = pd.read_csv(self.input_path)
        return df
    
    def tampilkan_info(self, df):
        logging.info("Menampilkan ringkasan informasi data..")
        print("\n------ Menampilkan info data ------")
        print(df.info())
        print("\n------ Menampilkan statistik data ------")
        print(df.describe())
        print("\n------ Jumlah missing value ------")
        print(df.isnull().sum())

    def simpan_data (self, df):
        output_file = os.path.join(self.output_path, f"final_data.{self.output_format}")

        if self.output_format == "csv":
            df.to_csv(output_file, index=False)
        elif self.output_format == "excel.xlsx":
            df.to_excel(output_file, index=False, engine='openpyxl')
        else:
            logging.error("Format penyimpanan tidak didukung! Gunakan 'csv' atau 'excel'.")
            return
        
        logging.info(f"data berhasil disimpan dalam format{self.output_format} di {output_file}")

    def proses_load(self):
        df = self.load_transformed_data()
        self.tampilkan_info(df)
        self.simpan_data(df)
        logging.info("proses load selesai")
        return df

if __name__ == "__main__":
    loader = LoadData(input_path="staging/data_transformed.csv", output_path="result", output_format="csv")
    loader.proses_load()
  
 


