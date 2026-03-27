from src.config import INTERIM_DIR, PROCESSED_DIR

import logging
log = logging.getLogger(__name__)

def save_interim_table(df, file_name: str) -> None:
    output_path = INTERIM_DIR / file_name
    df.to_csv(output_path, index=False)
    log.info(f"Arquivo interim salvo em: {output_path}")


def save_processed_table(df, file_name: str) -> None:
    output_path = PROCESSED_DIR / file_name
    df.to_csv(output_path, index=False)
    log.info(f"Arquivo processed salvo em: {output_path}")