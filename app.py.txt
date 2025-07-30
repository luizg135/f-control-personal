# /seu_projeto/app.py

from flask import Flask
from flask_cors import CORS
from routes.financial_routes import financial_bp
import os

def create_app():
    """Cria e configura a instância da aplicação Flask."""
    app = Flask(__name__)
    
    # Habilita CORS para permitir requisições de outros domínios (frontend)
    CORS(app)

    # Registra o blueprint com as rotas financeiras
    app.register_blueprint(financial_bp, url_prefix='/api/financial')

    @app.route('/')
    def index():
        return "API Financeira está no ar! Acesse /api/financial/data"
        
    return app

if __name__ == '__main__':
    app = create_app()
    # Usa a porta 5000 por padrão, mas pode ser configurada por variável de ambiente
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)