from .KKSTransformer import KKSTransformer
import pandas as pd


class StageTransformer:
    def __init__(self):
        self.kks_transformer = KKSTransformer()

    def kks(self, df: pd.DataFrame) -> pd.DataFrame:
        df["KKS"] = self.kks_transformer(df["KKS"].to_list())
        
        return df
    