from schedules.stage_transform.KKSTransformer import KKSTransformer
import pandas as pd


class StageTransformer:
    def __init__(self):
        self.kks_transformer = KKSTransformer()

    def kks(self, df: pd.DataFrame) -> pd.DataFrame:
        df["kks"] = self.kks_transformer(df["kks"].to_list())
        return df
    
    def row_number(self, df: pd.DataFrame) -> pd.DataFrame:
        df["_row_number"] = [i+1 for i in range(df.shape[0])]
        return df
    
    def created_at(self, df: pd.DataFrame, dt) -> pd.DataFrame:
        df["_created_at"] = dt
        return df
    