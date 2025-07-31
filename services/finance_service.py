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
    print(f"Buscando dados da planilha... ({datetime.now()})")
    try:
        sheet_id = Config.GOOGLE_SHEET_URL.split('/d/')[1].split('/')[0]
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        
        response = requests.get(csv_url, timeout=15)
        response.raise_for_status()

        df = pd.read_csv(BytesIO(response.content), header=1, encoding='utf-8')
        
        df = df.iloc[:, 1:9]
        df.columns = ['Data', 'Tipo', 'Grupo', 'Categoria', 'Item', 'Conta', 'Pagamento', 'Valor']
        
        df = df.dropna(how='all', subset=df.columns[1:])
        df = df.dropna(subset=['Data', 'Tipo', 'Valor'])

        df['Valor'] = df['Valor'].apply(_clean_currency_value)
        df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')
        
        df = df.dropna(subset=['Data'])
        df = df[df['Valor'] > 0]
        
        df['Ano'] = df['Data'].dt.year
        df['Mes'] = df['Data'].dt.month
        df['MesAno'] = df['Data'].dt.strftime('%Y-%m')
        
        total_entradas = df[df['Tipo'] == 'Receita']['Valor'].sum()
        total_saidas = df[df['Tipo'] == 'Despesa']['Valor'].sum()
        df_despesas = df[df['Tipo'] == 'Despesa']

        # --- INÍCIO DA NOVA LÓGICA DE CÁLCULO ---
        is_reserva = df['Categoria'].str.contains('Reserva', na=False)
        is_alimentacao = df['Conta'].str.contains('Alimentação', na=False)
        is_receita = df['Tipo'] == 'Receita'
        is_despesa = df['Tipo'] == 'Despesa'

        # Saldo em Conta: (Receitas Normais - Despesas Normais) - Receitas de Reserva (transferência p/ fora) + Despesas de Reserva (transferência p/ dentro)
        receitas_normais = df[is_receita & ~is_reserva & ~is_alimentacao]['Valor'].sum()
        despesas_normais = df[is_despesa & ~is_reserva & ~is_alimentacao]['Valor'].sum()
        receitas_reserva_valor = df[is_receita & is_reserva]['Valor'].sum()
        despesas_reserva_valor = df[is_despesa & is_reserva]['Valor'].sum()
        valor_conta = (receitas_normais - despesas_normais) - receitas_reserva_valor + despesas_reserva_valor

        # Saldo Alimentação: Apenas transações da conta Alimentação
        valor_alimentacao = df[is_alimentacao & is_receita]['Valor'].sum() - df[is_alimentacao & is_despesa]['Valor'].sum()

        # Saldo Reserva: Apenas transações da categoria Reserva
        valor_reserva = receitas_reserva_valor - despesas_reserva_valor
        # --- FIM DA NOVA LÓGICA DE CÁLCULO ---

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

        _cache["data"] = dados_finais
        _cache["last_fetched"] = datetime.now()
        
        print("Dados processados e cache atualizado com sucesso.")
        return dados_finais

    except Exception as e:
        print(f"ERRO CRÍTICO ao buscar ou processar dados: {e}")
        import traceback
        traceback.print_exc()
        _cache["data"] = None
        _cache["last_fetched"] = None
        raise