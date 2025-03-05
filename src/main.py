from extractor import Extractor
from transformasi import Transformasi
from load import LoadData

if __name__ == "__main__":
    extrac = Extractor(data_folder="data", staging_folder="staging")
    extrac_csv = extrac.extract_csv("csv")
    extrac_json = extrac.extract_json("json")

    result = extrac.extract_all(extrac.data_folder)
    print(result.head())

    transformer = Transformasi(input_path="staging/combined_data.csv", output_path="staging/data_transformed.csv")
    df_transformer = transformer.proses_transfomasi()
    print(df_transformer.head())

    loader = LoadData(input_path="staging/data_transformed.csv", output_path="result", output_format="csv")
    df_load = loader.proses_load()
    print(df_load)
    
        
