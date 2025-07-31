# /seu_projeto_backend/services/finance_service.py

import pandas as pd
import requests
import re
import json
from io import BytesIO
from datetime import datetime, timedelta
from config import Config

_cache = { "data": None, "last_fetched": None }

def _is_cache_valid():
    if not _cache["data"] or not _cache["last_fetched"]: return False
    return (datetime.now() - _cache["last_fetched"]) < timedelta(seconds=Config.CACHE_DURATION_SECONDS)

def _clean_currency_value(value):
    if pd.isna(value) or not value: return 0.0
    value_str = str(value)
    value_str = re.sub(r'[R$\s"]', '', value_str)
    if re.match(r'^\d{1,3}(\.\d{3})*,\d{2}$', value_str):
        value_str = value_str.replace('.', '').replace(',', '.')
    else:
        value_str = value_str.replace(',', '.')
    try: return float(value_str)
    except (ValueError, TypeError): return 0.0

def _fetch_and_process_data():
    """Busca os dados do Google Sheets e os processa de forma robusta."""
    print(f"Buscando dados da planilha... ({datetime.now()})")
    try:
        # ... (código de leitura do CSV e limpeza inicial continua o mesmo) ...
        sheet_id = Config.GOOGLE_SHEET_URL.split('/d/')[1].split('/')[0]
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        response = requests.get(csv_url, timeout=15)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content), header=1, encoding='utf-8')
        df = df.iloc[:, 2:10]
        df.columns = ['Data', 'Tipo', 'Grupo', 'Categoria', 'Item', 'Conta', 'Pagamento', 'Valor']
        df['Valor'] = df['Valor'].apply(_clean_currency_value)
        df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')
        df.dropna(subset=['Data'], inplace=True)
        df = df[df['Valor'] > 0]
        df.dropna(subset=['Tipo'], inplace=True)
        
        # No lugar do bloco 'if df.empty' antigo
        if df.empty:
            print("AVISO: Nenhum dado válido encontrado na planilha após a limpeza.")
            # --- INÍCIO DA CORREÇÃO ---
            # Retorna a estrutura completa e zerada, sem erros de sintaxe
            return {
                'resumo': {'saldo': 0, 'total_entradas': 0, 'total_saidas': 0, 'valor_alimentacao': 0, 'valor_conta': 0, 'valor_reserva': 0},
                'despesas_por_categoria': {},
                'despesas_por_grupo': {},
                'meses_disponiveis': [],
                'por_tipo': {},
                'saldo_mensal': {},
                'transacoes': []
            }
            # --- FIM DA CORREÇÃO ---

        df['MesAno'] = df['Data'].dt.strftime('%Y-%m')
        
        # --- LÓGICA DE CÁLCULO DOS CARDS (permanece a mesma) ---
        is_reserva = df['Categoria'].str.contains('Reserva', na=False)
        is_ajuste = df['Grupo'].str.contains('Ajuste', na=False)
        is_receita = df['Tipo'] == 'Receita'
        is_despesa = df['Tipo'] == 'Despesa'
        total_entradas = df[is_receita & ~is_ajuste & ~is_reserva]['Valor'].sum()
        total_saidas = df[is_despesa & ~is_ajuste & ~is_reserva]['Valor'].sum()
        # ... (cálculos de valor_conta, etc. continuam os mesmos) ...
        is_alimentacao = df['Conta'].str.contains('Alimentação', na=False)
        receitas_normais = df[is_receita & ~is_reserva & ~is_alimentacao]['Valor'].sum()
        despesas_normais = df[is_despesa & ~is_reserva & ~is_alimentacao]['Valor'].sum()
        receitas_reserva_valor = df[is_receita & is_reserva]['Valor'].sum()
        despesas_reserva_valor = df[is_despesa & is_reserva]['Valor'].sum()
        valor_conta = (receitas_normais - despesas_normais) - receitas_reserva_valor + despesas_reserva_valor
        valor_alimentacao = df[is_alimentacao & is_receita]['Valor'].sum() - df[is_alimentacao & is_despesa]['Valor'].sum()
        valor_reserva = receitas_reserva_valor - despesas_reserva_valor

        transacoes_dict = df.to_json(orient='records', date_format='iso')
        
        # --- INÍCIO DA CORREÇÃO DA LÓGICA DO GRÁFICO ---
        
        # 1. Filtra para remover 2024 e transações de ajuste/reserva
        df_grafico = df[
            (df['Data'].dt.year >= 2025) &
            (~is_ajuste) &
            (~is_reserva)
        ].copy()
        
        # 2. Calcula o fluxo de caixa líquido (entradas - saídas) para cada mês
        fluxo_mensal = df_grafico.groupby('MesAno').apply(
            lambda x: x[x['Tipo'] == 'Receita']['Valor'].sum() - x[x['Tipo'] == 'Despesa']['Valor'].sum()
        )
        
        # --- FIM DA CORREÇÃO ---

        dados_finais = {
            'resumo': { 'total_entradas': total_entradas, 'total_saidas': total_saidas, 'saldo': total_entradas - total_saidas, 'valor_conta': valor_conta, 'valor_alimentacao': valor_alimentacao, 'valor_reserva': valor_reserva },
            'despesas_por_categoria': df[is_despesa].groupby('Categoria')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'despesas_por_grupo': df[is_despesa].groupby('Grupo')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'saldo_mensal': fluxo_mensal.to_dict(), # <--- Usa o novo cálculo aqui
            'transacoes': json.loads(transacoes_dict),
            'meses_disponiveis': sorted(df['MesAno'].unique().tolist())
        }

        _cache["data"] = dados_finais
        _cache["last_fetched"] = datetime.now()
        
        print("Dados processados e cache atualizado com sucesso.")
        return dados_finais

    except Exception as e:
        # ... (bloco de erro continua o mesmo) ...