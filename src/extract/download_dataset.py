import shutil
from pathlib import Path

import kagglehub
from src.config import DATASET_NAME, RAW_DIR

import logging
log = logging.getLogger(__name__)

def download_dataset() -> Path:
    """
    Baixa o dataset via kagglehub e copia os arquivos CSV para data/raw.
    Retorna o caminho da pasta raw.
    """
    source_path = Path(kagglehub.dataset_download(DATASET_NAME))
    log.info(f"Dataset baixado em: {source_path}")

    for file in source_path.glob("*.csv"):
        destination = RAW_DIR / file.name
        shutil.copy(file, destination)
        log.info(f"Copiado: {file.name} -> {destination}")

    return RAW_DIR