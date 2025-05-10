import os
import zipfile
from abc import ABC, abstractmethod
import pandas as pd


# why did we do all this??
# to ensure correct file extension for scalabilty and readability
# lets say we want to do json file extension then just make a data ingestion for json


#  defining the abstract class for data ingestion
#  using factory design.

class DataIngestor(ABC):
    @abstractmethod
    def data_ingestion(self, file_path:str)->pd.DataFrame:
        pass


# Implmenting the data ingestion class
class ZipDataIngestor(DataIngestor):
    def data_ingestion(self, file_path:str)->pd.DataFrame:
        if not file_path.endswith(".zip"):
            raise ValueError("The provided path is not a .zip file")
        
        with zipfile.ZipFile(file_path,"r") as zip_ref:
            zip_ref.extractall("extracted_data")

        extracted_files = os.listdir("extracted_data")
        csv_files = [f for f in extracted_files in f.endswith(".csv")]  # type: ignore

        if len(csv_files) == 0:
            raise FileNotFoundError("No csv file in zip folder")
        if len(csv_files)>1:
            raise ValueError("multiple csv files.. please specify which one has data") 

        csv_file_path = os.path.join("extracted_data", csv_files[0])
        df = pd.read_csv(csv_file_path)
        return df
    

class DataIngetstorFactory:
    @staticmethod
    def get_data_ingestor(file_extension:str)-> DataIngestor:
        if file_extension == ".zip":
            return ZipDataIngestor()
        else :
            raise ValueError(f"No Ingestor available for file_extension{file_extension}")
        


if __name__ == "__main__":
    file_path = ""
    file_extension = os.path.splitext(file_path)[1]

    data_ingestor = DataIngetstorFactory.get_data_ingestor(file_extension)

    df = data_ingestor.data_ingestion(file_path)
    print(df.head())
    pass
