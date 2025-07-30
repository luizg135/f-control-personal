# /seu_projeto/services/finance_service.py

import pandas as pd
import requests
import re
import json
from io import StringIO
from datetime import datetime, timedelta
from config import Config

# --- Variáveis de Cache em Memória ---
_cache = {
    "data": None,
    "last_fetched": None
}

def _is_cache_valid():
    """Verifica se o cache em memória ainda é válido."""
    if not _cache["data"] or not _cache["last_fetched"]:
        return False
    
    elapsed_time = datetime.now() - _cache["last_fetched"]
    return elapsed_time < timedelta(seconds=Config.CACHE_DURATION_SECONDS)

def _clean_currency_value(value):
    """Limpa e converte valores monetários no formato brasileiro."""
    if pd.isna(value) or not value:
        return 0.0
    
    value_str = str(value)
    value_str = re.sub(r'[R$\s"]', '', value_str)
    
    # Lida com o padrão 1.000,00
    if re.match(r'^\d{1,3}(\.\d{3})*,\d{2}$', value_str):
        value_str = value_str.replace('.', '').replace(',', '.')
    else:
        # Lida com o padrão 1000,00
        value_str = value_str.replace(',', '.')
        
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return 0.0

def _fetch_and_process_data():
    """Busca os dados do Google Sheets e os processa."""
    print(f"Buscando dados da planilha... ({datetime.now()})")
    try:
        sheet_id = Config.GOOGLE_SHEET_URL.split('/d/')[1].split('/')[0]
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        
        response = requests.get(csv_url, timeout=15)
        response.raise_for_status()

        # --- INÍCIO DA CORREÇÃO ---
        # Força a codificação da resposta para UTF-8 ANTES de ler
        response.encoding = 'utf-8'

        # Lê o CSV especificando a mesma codificação
        df = pd.read_csv(StringIO(response.text), header=1, encoding='utf-8')
        # --- FIM DA CORREÇÃO ---
        
        # --- Limpeza e Processamento do DataFrame ---
        df = df.drop(columns=['Unnamed: 0', 'â\x86\x91â\x86\x93'], errors='ignore')
        df.columns = ['Data', 'Tipo', 'Grupo', 'Categoria', 'Item', 'Conta', 'Pagamento', 'Valor']
        
        df = df.dropna(how='all', subset=df.columns[1:]) # Manter se tiver algo além da data
        df = df.dropna(subset=['Data', 'Tipo', 'Valor'])

        df['Valor'] = df['Valor'].apply(_clean_currency_value)
        df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
        
        df = df.dropna(subset=['Data'])
        df = df[df['Valor'] > 0]
        
        df['Ano'] = df['Data'].dt.year
        df['Mes'] = df['Data'].dt.month
        df['MesAno'] = df['Data'].dt.strftime('%Y-%m')
        
        # --- Cálculos e Agregações ---
        total_entradas = df[df['Tipo'] == 'Receita']['Valor'].sum()
        total_saidas = df[df['Tipo'] == 'Despesa']['Valor'].sum()
        
        df_despesas = df[df['Tipo'] == 'Despesa']
        
        # Cards de Resumo
        df_sem_reserva = df[~df['Categoria'].str.contains('Reserva', na=False)]
        valor_conta = df_sem_reserva[df_sem_reserva['Tipo'] == 'Receita']['Valor'].sum() - df_sem_reserva[df_sem_reserva['Tipo'] == 'Despesa']['Valor'].sum()
        
        df_alimentacao = df[df['Conta'].str.contains('Alimentação', na=False)]
        valor_alimentacao = df_alimentacao[df_alimentacao['Tipo'] == 'Receita']['Valor'].sum() - df_alimentacao[df_alimentacao['Tipo'] == 'Despesa']['Valor'].sum()
        
        df_reserva = df[df['Categoria'].str.contains('Reserva', na=False)]
        valor_reserva = df_reserva[df_reserva['Tipo'] == 'Receita']['Valor'].sum() - df_reserva[df_reserva['Tipo'] == 'Despesa']['Valor'].sum()

        # Preparando dados para o frontend (JSON serializável)
        # Converte o DataFrame para um formato que não causa problemas com JSON
        transacoes_dict = json.loads(df.to_json(orient='records', date_format='iso'))

        dados_finais = {
            'resumo': {
                'total_entradas': total_entradas,
                'total_saidas': total_saidas,
                'saldo': total_entradas - total_saidas,
                'valor_conta': valor_conta,
                'valor_alimentacao': valor_alimentacao,
                'valor_reserva': valor_reserva
            },
            'por_tipo': df.groupby('Tipo')['Valor'].sum().to_dict(),
            'despesas_por_categoria': df_despesas.groupby('Categoria')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'despesas_por_grupo': df_despesas.groupby('Grupo')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'saldo_mensal': df.groupby('MesAno').apply(lambda x: x[x['Tipo']=='Receita']['Valor'].sum() - x[x['Tipo']=='Despesa']['Valor'].sum()).to_dict(),
            'transacoes': transacoes_dict,
            'meses_disponiveis': sorted(df['MesAno'].unique().tolist())
        }

        # Atualiza o cache
        _cache["data"] = dados_finais
        _cache["last_fetched"] = datetime.now()
        
        print("Dados processados e cache atualizado com sucesso.")
        return dados_finais

    except Exception as e:
        print(f"ERRO CRÍTICO ao buscar ou processar dados: {e}")
        # Em caso de erro, invalida o cache para tentar de novo na próxima vez
        _cache["data"] = None
        _cache["last_fetched"] = None
        raise # Propaga o erro para a camada da API tratar

def get_financial_data():
    """
    Ponto de entrada principal. Retorna dados do cache se válidos,
    senão, busca e processa novos dados.
    """
    if _is_cache_valid():
        print("Retornando dados do cache.")
        return _cache["data"]
    
    return _fetch_and_process_data()