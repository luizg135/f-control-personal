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

# NO ARQUIVO services/finance_service.py

# NO ARQUIVO services/finance_service.py

def _fetch_and_process_data():
    """Busca os dados do Google Sheets e os processa com LOGS DE DEPURAÇÃO."""
    print("--- INICIANDO PROCESSO DE BUSCA E DEPURAÇÃO ---")
    try:
        sheet_id = Config.GOOGLE_SHEET_URL.split('/d/')[1].split('/')[0]
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        
        print(f"1. Acessando URL: {csv_url}")
        response = requests.get(csv_url, timeout=15)
        response.raise_for_status()

        df = pd.read_csv(BytesIO(response.content), header=1, encoding='utf-8')
        
        print("2. Dados lidos do CSV. Shape inicial:", df.shape)
        print("Colunas originais:", list(df.columns))
        print("Primeiras 3 linhas (RAW):\n", df.head(3).to_string())

        df = df.iloc[:, 1:9]
        print("\n3. Após fatiar para 8 colunas. Shape:", df.shape)
        
        df.columns = ['Data', 'Tipo', 'Grupo', 'Categoria', 'Item', 'Conta', 'Pagamento', 'Valor']
        print("4. Colunas renomeadas.")
        print("Primeiras 3 linhas (Após renomear):\n", df.head(3).to_string())

        df['Valor'] = df['Valor'].apply(_clean_currency_value)
        print("\n5. Coluna 'Valor' convertida para número.")
        
        df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')
        print("6. Coluna 'Data' convertida para datetime.")
        print("Contagem de datas nulas (NaT) após conversão:", df['Data'].isnull().sum())

        df.dropna(subset=['Data'], inplace=True)
        print("\n7. Após remover linhas com datas inválidas. Shape:", df.shape)
        
        df = df[df['Valor'] > 0]
        print("8. Após remover linhas com valor <= 0. Shape:", df.shape)

        df.dropna(subset=['Tipo'], inplace=True)
        print("9. Após remover linhas com 'Tipo' vazio. Shape final:", df.shape)
        
        if df.empty:
            print("\n!!! AVISO: DataFrame ficou vazio após a limpeza. !!!")
            # O resto do código continua a partir daqui...
        
        # ... (O restante da função continua exatamente como antes) ...
        df['Ano'] = df['Data'].dt.year
        df['Mes'] = df['Data'].dt.month
        df['MesAno'] = df['Data'].dt.strftime('%Y-%m')
        
        total_entradas = df[df['Tipo'] == 'Receita']['Valor'].sum()
        total_saidas = df[df['Tipo'] == 'Despesa']['Valor'].sum()
        df_despesas = df[df['Tipo'] == 'Despesa']

        is_reserva = df['Categoria'].str.contains('Reserva', na=False)
        is_alimentacao = df['Conta'].str.contains('Alimentação', na=False)
        is_receita = df['Tipo'] == 'Receita'
        is_despesa = df['Tipo'] == 'Despesa'
        
        receitas_normais = df[is_receita & ~is_reserva & ~is_alimentacao]['Valor'].sum()
        despesas_normais = df[is_despesa & ~is_reserva & ~is_alimentacao]['Valor'].sum()
        receitas_reserva_valor = df[is_receita & is_reserva]['Valor'].sum()
        despesas_reserva_valor = df[is_despesa & is_reserva]['Valor'].sum()
        valor_conta = (receitas_normais - despesas_normais) - receitas_reserva_valor + despesas_reserva_valor

        valor_alimentacao = df[is_alimentacao & is_receita]['Valor'].sum() - df[is_alimentacao & is_despesa]['Valor'].sum()
        valor_reserva = receitas_reserva_valor - despesas_reserva_valor

        transacoes_dict = json.loads(df.to_json(orient='records', date_format='iso'))

        dados_finais = {
            'resumo': { 'total_entradas': total_entradas, 'total_saidas': total_saidas, 'saldo': total_entradas - total_saidas, 'valor_conta': valor_conta, 'valor_alimentacao': valor_alimentacao, 'valor_reserva': valor_reserva },
            'por_tipo': df.groupby('Tipo')['Valor'].sum().to_dict(),
            'despesas_por_categoria': df_despesas.groupby('Categoria')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'despesas_por_grupo': df_despesas.groupby('Grupo')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'saldo_mensal': df.groupby('MesAno').apply(lambda x: x[x['Tipo']=='Receita']['Valor'].sum() - x[x['Tipo']=='Despesa']['Valor'].sum()).to_dict(),
            'transacoes': transacoes_dict,
            'meses_disponiveis': sorted(df['MesAno'].unique().tolist())
        }

        _cache["data"] = dados_finais
        _cache["last_fetched"] = datetime.now()
        
        print("\n--- PROCESSO CONCLUÍDO COM SUCESSO ---")
        return dados_finais

    except Exception as e:
        print(f"--- ERRO CRÍTICO NO PROCESSAMENTO: {e} ---")
        import traceback
        traceback.print_exc()
        _cache["data"] = None
        _cache["last_fetched"] = None
        raise