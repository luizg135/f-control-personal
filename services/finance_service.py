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

def _fetch_and_process_data():
    """Busca os dados do Google Sheets e os processa de forma mais robusta."""
    print(f"Buscando dados da planilha... ({datetime.now()})")
    try:
        sheet_id = Config.GOOGLE_SHEET_URL.split('/d/')[1].split('/')[0]
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        
        response = requests.get(csv_url, timeout=15)
        response.raise_for_status()

        # Tenta ler o CSV com a codificação correta
        df = pd.read_csv(BytesIO(response.content), header=1, encoding='utf-8')
        
        # Seleciona as colunas de dados (B até I) e as renomeia
        df = df.iloc[:, 1:9]
        df.columns = ['Data', 'Tipo', 'Grupo', 'Categoria', 'Item', 'Conta', 'Pagamento', 'Valor']
        
        # --- INÍCIO DA LÓGICA DE LIMPEZA APRIMORADA ---

        # 1. Converte a coluna 'Valor' para número primeiro.
        #    Isso garante que tenhamos um valor numérico antes de qualquer filtro.
        df['Valor'] = df['Valor'].apply(_clean_currency_value)

        # 2. Converte a coluna 'Data' para datetime de forma mais flexível.
        #    'dayfirst=True' ajuda a interpretar 'dd/mm/yyyy' corretamente. 'coerce' transforma erros em NaT (Not a Time).
        df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')

        # 3. Agora, remove as linhas que não são válidas.
        #    Remove qualquer linha onde a conversão da data falhou.
        df.dropna(subset=['Data'], inplace=True)
        # Remove qualquer linha onde o valor é 0 ou menor, ou onde o 'Tipo' está em branco.
        df = df[df['Valor'] > 0]
        df.dropna(subset=['Tipo'], inplace=True)

        # --- FIM DA LÓGICA DE LIMPEZA APRIMORADA ---
        
        # Se após a limpeza o dataframe estiver vazio, retorna uma estrutura vazia controlada
        if df.empty:
            print("AVISO: Nenhum dado válido encontrado na planilha após a limpeza.")
            return {
                'resumo': {'saldo': 0, 'total_entradas': 0, 'total_saidas': 0, 'valor_alimentacao': 0, 'valor_conta': 0, 'valor_reserva': 0},
                'despesas_por_categoria': {}, 'despesas_por_grupo': {}, 'meses_disponiveis': [], 'por_tipo': {}, 'saldo_mensal': {}, 'transacoes': []
            }

        # Continua com o processamento normal se houver dados
        df['Ano'] = df['Data'].dt.year
        df['Mes'] = df['Data'].dt.month
        df['MesAno'] = df['Data'].dt.strftime('%Y-%m')
        
        total_entradas = df[df['Tipo'] == 'Receita']['Valor'].sum()
        total_saidas = df[df['Tipo'] == 'Despesa']['Valor'].sum()
        df_despesas = df[df['Tipo'] == 'Despesa']

        # Lógica de cálculo dos cards
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