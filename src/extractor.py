import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Extractor:
    def __init__(self, data_folder, staging_folder):
        self.data_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", data_folder))
        self.staging_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", staging_folder))

        logging.info(f"data folder {self.data_folder}")
        logging.info(f"stagin folder {self.staging_folder}")

    def extract_csv(self, csv_folder):
        csv_path = os.path.abspath(os.path.join(self.data_folder,csv_folder))
        logging.info(f"Mengekstrak dara csv: {csv_path}")
        
        if not os.path.exists(csv_path):
            logging.error(f"folder csv tidak ditemukan : {csv_path}")
            return None
        
        all_data = []
        for file in os.listdir(csv_path):
            file_path = os.path.join(csv_path, file)
            if file.endswith(".csv"):
                logging.info(f"Memproses file csv {file_path}")
                try:
                    df = pd.read_csv(file_path)
                    all_data.append(df)
                except Exception as e :
                    logging.error(f"Error membaca file {file}: {e}")
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logging.info(f"Berhasil menggabungkan {len(all_data)} file csv") 
            return combined_data
        else:
            logging.warning("Tidak ada file csv ditemukan")   
            return None

    def extract_json(self, json_folder):
        json_path = os.path.abspath(os.path.join(self.data_folder, json_folder))
        logging.info(f"Mencarri file json di {json_path}")

        if not os.path.exists(json_path):
            logging.error(f"Folder json tidak ditemukan : {json_path} ")
            return None
        
        all_data = []
        for file in os.listdir(json_path):
            file_path = os.path.join(json_path, file)
            if file.endswith(".json"):
                logging.info(f"Memproses file Json : {file_path}")
                try :
                    try:
                        df = pd.read_json(file_path, lines=True)
                    except ValueError:
                        df = pd.read_json(file_path)
                    df.fillna({
                        "ehail_fee": 0.0,
                        "congestion_surcharge": 0.0,
                        "store_and_fwd_flag" : "",
                        "passenger_count" : df["passenger_count"].median(),
                    }, inplace=True)

                    all_data.append(df)

                except Exception as e:
                    logging.error(f"Error membaca file {file} : {e}")
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            logging.info(f"berhasil menggabungkan {len(all_data)} file Json")
            return combined_data
        else:
            logging.warning("Tidaka ada file json yang ditemukan")
            return None

    def extract_all (self, folder):
        logging.info(f"Memulai ekstrasi dari folder {folder}")
        
        csv_folder = os.path.join(folder, "csv")
        json_folder = os.path.join(folder, "json")
        df_csv = self.extract_csv(csv_folder)
        df_json = self.extract_json(json_folder)
        
        if df_csv is not None and df_json is not None:
            combined_df = pd.concat([df_csv, df_json], ignore_index=True)
        elif df_csv is not None:
            combined_df = df_csv
        elif df_json is not None:
            combined_df = df_json
        else:
            logging.warning("TIdak ada data yang diekstrak")
            return None
        
        if not isinstance(combined_df, pd.DataFrame):
            logging.error("Terjadi kesalahan: combined_df bukan DataFrame")
            return None

        staging_path = os.path.join(self.staging_folder, "combined_data.csv")
        combined_df.to_csv(staging_path, index=False)
        logging.info(f"Data berhasil disimpan di {staging_path}")
        return combined_df


if __name__ == "__main__":
    extrac = Extractor(data_folder="data", staging_folder="staging")
    result = extrac.extract_all(extrac.data_folder)

    print(result)

