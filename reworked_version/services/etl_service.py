import pandas as pd

class ETLService:
    def load_csv_into_dataframe(self, input_path):
        return pd.read_csv(input_path)
    
    def save_dataframe_to_csv(self, df: pd.DataFrame, output_path):
        df.to_csv(output_path)
