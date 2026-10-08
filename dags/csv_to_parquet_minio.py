from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

CSV_PATH = "/opt/airflow/dags/extraction/sample.csv"
CONTRACT_PATH = "/opt/airflow/config/schema_contract.yaml"
STAGE_DIR = "/tmp/dhap42"


def _snake(name: str) -> str:
    import re
    return re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()


@dag(
    dag_id="csv_to_parquet_minio",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dhap-42"],
)
def csv_to_parquet_minio():

    @task
    def read_csv() -> str:
        import os
        import pandas as pd

        os.makedirs(STAGE_DIR, exist_ok=True)
        # dtype=str: types contract , pandas not match here.
        df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8", encoding_errors="replace")
        raw_path = f"{STAGE_DIR}/raw.pkl"
        df.to_pickle(raw_path)
        print(f"Read {len(df)} rows, {len(df.columns)} columns")
        return raw_path

    @task
    def validate_contract(raw_path: str) -> str:
        import pandas as pd
        import yaml

        with open(CONTRACT_PATH, encoding="utf-8") as f:
            contract = yaml.safe_load(f)
        df = pd.read_pickle(raw_path)
        errors = []

        expected = [c["name"] for c in contract["columns"]]
        actual = list(df.columns)
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        if missing:
            errors.append(f"Missing columns: {missing}")
        if extra and contract.get("strict_columns", True):
            errors.append(f"Unexpected columns: {extra}")

        for col in contract["columns"]:
            name = col["name"]
            if name not in df.columns:
                continue
            s = df[name]

            if not col.get("nullable", True) and s.isna().any():
                errors.append(f"'{name}': {int(s.isna().sum())} null value(s), nullable=false")

            nn = s.dropna().str.strip()
            ctype = col.get("type", "string")
            bad = pd.Series(False, index=nn.index)
            if ctype == "integer":
                bad = ~nn.str.fullmatch(r"[+-]?\d+")
            elif ctype == "float":
                bad = pd.to_numeric(nn, errors="coerce").isna()
            elif ctype == "date":
                bad = pd.to_datetime(nn, errors="coerce").isna()
            if bad.any():
                rows = [int(i) + 2 for i in nn[bad].index[:5]]  # +2 = CSV line number
                errors.append(f"'{name}': {int(bad.sum())} value(s) not {ctype} (CSV lines {rows})")

            allowed = col.get("allowed_values")
            if allowed:
                bad = ~nn.isin(allowed)
                if bad.any():
                    rows = [int(i) + 2 for i in nn[bad].index[:5]]
                    errors.append(f"'{name}': {int(bad.sum())} value(s) not in {allowed} (CSV lines {rows})")

            if col.get("unique") and nn.duplicated().any():
                errors.append(f"'{name}': duplicate values found")

        if errors:
            raise AirflowFailException("Schema contract violated:\n- " + "\n- ".join(errors))

        print("Schema contract OK")
        return raw_path

    @task
    def transform(raw_path: str) -> str:
        import pandas as pd

        df = pd.read_pickle(raw_path)
        df.columns = [_snake(c) for c in df.columns]

        # strip whitespace; treat empty strings as null
        df = df.apply(lambda s: s.str.strip()).replace("", pd.NA)

        # drop rows where every column except dataset_code is empty (blank rows at the end of the CSV)
        other_cols = [c for c in df.columns if c != "dataset_code"]
        before = len(df)
        df = df.dropna(subset=other_cols, how="all").copy()
        print(f"Dropped {before - len(df)} empty rows")

        df["dataset_code"] = df["dataset_code"].astype("int64")
        df["author"] = df["author"].fillna("unknown")  # partition column must not be null

        clean_path = f"{STAGE_DIR}/clean.pkl"
        df.to_pickle(clean_path)
        print(f"Transformed: {len(df)} rows")
        return clean_path

    @task
    def write_parquet_to_minio(clean_path: str) -> None:
        import os
        from urllib.parse import urlparse

        import pandas as pd
        import pyarrow as pa
        import pyarrow.dataset as ds
        import yaml
        from pyarrow import fs

        with open(CONTRACT_PATH, encoding="utf-8") as f:
            contract = yaml.safe_load(f)
        partition_col = _snake(contract["partition_column"])
        dataset_name = contract["dataset"]

        df = pd.read_pickle(clean_path)
        table = pa.Table.from_pandas(df, preserve_index=False)

        endpoint = urlparse(os.environ["MINIO_ENDPOINT"])
        s3 = fs.S3FileSystem(
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            endpoint_override=endpoint.netloc,
            scheme=endpoint.scheme,
        )
        base_dir = f"{os.environ['MINIO_BUCKET']}/{dataset_name}"

        ds.write_dataset(
            table,
            base_dir=base_dir,
            filesystem=s3,
            format="parquet",
            partitioning=ds.partitioning(pa.schema([table.schema.field(partition_col)]), flavor="hive"),
            basename_template="part-{i}.parquet",
            existing_data_behavior="delete_matching",  # re-runs overwrite the same partitions, no duplicates
        )
        print(f"Wrote {len(df)} rows to s3://{base_dir}/{partition_col}=*/")

    write_parquet_to_minio(transform(validate_contract(read_csv())))


csv_to_parquet_minio()