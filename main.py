from src.extract.download_dataset import download_dataset
from src.transform.clean_tables import load_raw_tables, clean_all_tables
from src.transform.powerbi_tables import prepare_powerbi_tables
from src.load.save_outputs import save_interim_table, save_processed_table

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

def run_pipeline():
    log.info("1. Baixando dataset...")
    download_dataset()

    log.info("2. Carregando tabelas raw...")
    raw_tables = load_raw_tables()

    log.info("3. Limpando tabelas...")
    cleaned_tables = clean_all_tables(raw_tables)

    for table_name, df in cleaned_tables.items():
        save_interim_table(df, f"{table_name}_clean.csv")

    log.info("4. Preparando tabelas para Power BI...")
    powerbi_tables = prepare_powerbi_tables(cleaned_tables)

    log.info("5. Salvando tabelas processadas...")
    for table_name, df in powerbi_tables.items():
        save_processed_table(df, f"{table_name}.csv")

    log.info("Pipeline finalizado com sucesso.")

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        log.info(f"Pipeline falhou: {e}")
        raise