import sys
import os
import importlib.util
from pathlib import Path


def configure_app_paths():
    """Configura os caminhos padrão para a aplicação"""
    app_dir = Path(__file__).parent
    os.chdir(app_dir)  # Muda o diretório de trabalho para a pasta da aplicação
    sys.path.insert(0, str(app_dir))  # Adiciona ao PYTHONPATH


def load_app(script_path: str, app_var: str = "app") -> object:
    """Carrega a aplicação Flask isolando-a dos argumentos do Gunicorn"""
    script_path = Path(script_path).resolve()
    module_name = script_path.stem

    # Backup do ambiente original
    original_argv = sys.argv
    original_path = sys.path.copy()
    original_cwd = os.getcwd()

    try:
        # Configuração de ambiente
        configure_app_paths()

        # Isolamento de contexto
        sys.argv = [sys.argv[0]]  # Mantém apenas o nome do script

        # Carregamento dinâmico
        spec = importlib.util.spec_from_file_location(module_name, script_path)
        module = importlib.util.module_from_spec(spec)

        # Garante que o rede.ini será encontrado
        os.environ["CONFIG_PATH"] = str(Path(__file__).parent / "rede.ini")

        spec.loader.exec_module(module)
        flask_app = getattr(module, app_var)

        # Configuração adicional para o Flask
        flask_app.config.update({"INSTANCE_PATH": str(Path(__file__).parent)})

        # Registra extensões do fork (blueprints etc.) ainda dentro do try,
        # enquanto sys.path/cwd/argv isolados por configure_app_paths() seguem em vigor.
        registrar_extensoes(flask_app, module)

        return flask_app
    finally:
        # Restauração do ambiente
        sys.argv = original_argv
        sys.path = original_path
        os.chdir(original_cwd)


def registrar_extensoes(flask_app, rede_mod):
    """Registra blueprints adicionais deste fork, sem tocar em rede/rede.py."""
    from modulos.api_ext.rede_api_ext import criar_blueprint

    bp = criar_blueprint(rede_mod)
    flask_app.register_blueprint(bp, url_prefix=rede_mod.base + 'api/ext')


# Carrega a aplicação
application = load_app("/rede-cnpj/rede/rede.py")
