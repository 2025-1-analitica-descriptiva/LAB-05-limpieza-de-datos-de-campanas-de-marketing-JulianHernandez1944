"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
import pandas as pd  # type: ignore
import zipfile  # type: ignore
from pathlib import Path  # type: ignore
def load_data(zip_file):
    """Lea los archivos comprimidos y devuelva un DataFrame"""
    dfs = []
    with zipfile.ZipFile(zip_file, 'r') as z:
        for csv_file in z.namelist():
            with z.open(csv_file) as f:
                df = pd.read_csv(f)
                dfs.append(df)
    return pd.concat(dfs, ignore_index=True)
def process_client_data(df):
    """Procesa y limpia los datos de cliente"""
    client_df = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    client_df["job"] = client_df["job"].str.replace(".", "").str.replace("-", "_")
    client_df["education"] = client_df["education"].str.replace(".", "_").replace("unknown", pd.NA)
    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    return client_df
def process_campaign_data(df):
    """Procesa y limpia los datos de campaña"""
    campaign_df = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]].copy()
    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    campaign_df["last_contact_date"] = pd.to_datetime(campaign_df["day"].astype(str) + "-" + campaign_df["month"] + "-2022", format="%d-%b-%Y")
    campaign_df.drop(columns=["day", "month"], inplace=True)
    return campaign_df
def process_economics_data(df):
    """Procesa y limpia los datos económicos"""
    economics_df = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
    return economics_df
def save_data(df, output_file):
    """Guarda el DataFrame en un archivo"""
    df.to_csv(output_file, index=False)
def clean_campaign_data():
    """Ejecuta la limpieza de datos"""
    input_folder = Path("files/input/")
    output_folder = Path("files/output/")
    output_folder.mkdir(parents=True, exist_ok=True)
    client_df = pd.DataFrame()
    campaign_df = pd.DataFrame()
    economics_df = pd.DataFrame()
    for zip_file in input_folder.glob("*.zip"):
        df = load_data(zip_file)
        client_df = pd.concat([client_df, process_client_data(df)], ignore_index=True)
        campaign_df = pd.concat([campaign_df, process_campaign_data(df)], ignore_index=True)
        economics_df = pd.concat([economics_df, process_economics_data(df)], ignore_index=True)
    save_data(client_df, output_folder / "client.csv")
    save_data(campaign_df, output_folder / "campaign.csv")
    save_data(economics_df, output_folder / "economics.csv")
    



if __name__ == "__main__":
    clean_campaign_data()
