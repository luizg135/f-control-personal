# /seu_projeto/routes/financial_routes.py

from flask import Blueprint, jsonify
from services import finance_service

financial_bp = Blueprint('financial', __name__)

# No arquivo routes/financial_routes.py

@financial_bp.route('/data')
def get_all_data():
    """Endpoint para obter todos os dados financeiros processados."""
    try:
        data = finance_service.get_financial_data()
        return jsonify(data)
    except Exception as e:
        # Mensagem de erro amigável para o usuário final
        print(f"Erro na rota /data: {e}")
        return jsonify({'error': 'Não foi possível obter os dados financeiros.'}), 500

# NO ARQUIVO /routes/financial_routes.py

import traceback # Verifique se este import está no topo do arquivo

@financial_bp.route('/data')
def get_all_data():
    """Endpoint para obter todos os dados financeiros processados."""
    try:
        data = finance_service.get_financial_data()
        return jsonify(data)
    except Exception as e:
        # --- MUDANÇA PARA DEPURAÇÃO ---
        # Captura e retorna o erro detalhado para descobrirmos a causa raiz
        error_details = traceback.format_exc()
        print(f"ERRO DETALHADO NA API: \n{error_details}")
        
        return jsonify({
            "error": f"Ocorreu um erro interno no servidor.",
            "detalhes_tecnicos": str(e),
            "traceback": error_details
        }), 500
        # --- FIM DA MUDANÇA ---
        
        # Filtra as transações para o mês solicitado
        transacoes_mes = [
            t for t in all_data['transacoes'] 
            if t['MesAno'] == mes_ano
        ]
        
        if not transacoes_mes:
            return jsonify({'error': 'Nenhum dado encontrado para este mês.', 'transacoes': []}), 404

        # Recalcula o resumo para o mês específico
        total_entradas = sum(t['Valor'] for t in transacoes_mes if t['Tipo'] == 'Receita')
        total_saidas = sum(t['Valor'] for t in transacoes_mes if t['Tipo'] == 'Despesa')
        
        # Filtra os grupos/categorias para o mês
        df_mes = pd.DataFrame(transacoes_mes)
        df_despesas_mes = df_mes[df_mes['Tipo'] == 'Despesa']

        dados_filtrados = {
            'resumo': {
                'total_entradas': total_entradas,
                'total_saidas': total_saidas,
                'saldo': total_entradas - total_saidas
            },
            'despesas_por_categoria': df_despesas_mes.groupby('Categoria')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'despesas_por_grupo': df_despesas_mes.groupby('Grupo')['Valor'].sum().sort_values(ascending=False).to_dict(),
            'transacoes': transacoes_mes
        }

        return jsonify(dados_filtrados)
    except Exception as e:
        print(f"Erro na rota /data/{mes_ano}: {e}")
        return jsonify({'error': 'Não foi possível obter os dados para o mês especificado.'}), 500