# /seu_projeto/config.py

import os

class Config:
    """
    Configurações centralizadas para a aplicação.
    """
    # URL da planilha pública do Google Sheets
    GOOGLE_SHEET_URL = os.environ.get(
        'GOOGLE_SHEET_URL',
        "https://docs.google.com/spreadsheets/d/1c7VSRDYM82BGTnoOoZQ_yN_FOnbJaqfiiDrfBvSYYdI/edit?usp=sharing"
    )

    # Duração do cache em memória (em segundos)
    # A planilha será lida novamente apenas após este tempo
    CACHE_DURATION_SECONDS = 600  # 10 minutos