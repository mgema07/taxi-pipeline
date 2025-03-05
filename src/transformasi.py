import pandas as pd
import os
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Transformasi:
    def __init__(self, input_path, output_path):
        self.input_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", input_path))
        self.output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", output_path))

    def load_data (self):
        logging.info("Membaca data dari staging...")
        df = pd.read_csv(self.input_path)
        return df
    
    def tambah_trip_durasi (self, df):
        logging.info("Menambah kolom trip_durasi")
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        df['trip_durasi'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 60
        return df
    
    def normalisasi_nama_kolom (self, df):
        logging.info("Normalisasi nama kolom")

        def to_snake_case (name):
            name = name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
            name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
            name = re.sub(r'[^a-zA-Z0-9]', '_', name)

            return name.lower()
        
        df.columns = [to_snake_case(col) for col in df.columns]
        return df
    
    def ubah_payment_type (self, df):
        logging.info("Mengubah nilai payment type..")
        mapping_payment = {1: 'Credit', 2: 'Cash'}
        df['payment_type'] = df['payment_type'].map(mapping_payment).fillna('Other')
        return df
    
    def konversi_trip_distance (self, df):
        logging.info("Mengkonversi trip_distance dari mil ke km...")
        df['trio_distance'] = df['trip_distance'] * 1.60934
        return df
    
    def simpan_data (self, df):
        logging.info("Menyimpan data hasil transformasi ke folder staging..")
        df.to_csv(self.output_path, index=False)

    def proses_transfomasi (self):
        df = self.load_data()
        df = self.tambah_trip_durasi(df)
        df = self.normalisasi_nama_kolom(df)
        df = self.ubah_payment_type(df)
        df = self.konversi_trip_distance(df)
        self.simpan_data(df)
        logging.info("Transformasi telah selesai...!")
        return df
if __name__ == "__main__":
    
    transformer = Transformasi(input_path="staging/combined_data.csv", output_path="staging/data_transformed.csv")

    df_transformer = transformer.proses_transfomasi()

    print(df_transformer.head())
    
    



