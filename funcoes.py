# ==============================================================
# Importações e Configurações Iniciais

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

# Inicializar SparkSession
spark = SparkSession.builder.appName("CapturaDeDados").getOrCreate()

# Carregar variáveis de ambiente do .env
load_dotenv()

# ==============================================================
# Função para Captura de Dados

def capturar_dados(escolha, caminho_ou_view):
    """
    Captura dados de acordo com a fonte escolhida pelo usuário.
    
    :param escolha: Fonte de dados ('csv' ou 'db').
    :param caminho_ou_view: Caminho para o CSV ou nome da view.
    :return: DataFrame do PySpark com os dados capturados.
    """
    if escolha == "csv":
        try:
            return spark.read.csv(caminho_ou_view, header=True, inferSchema=True)
        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return None

    elif escolha == "db":
        try:
            # Configuração da conexão com o banco de dados
            DB_HOST = os.getenv("DB_HOST")
            DB_PORT = os.getenv("DB_PORT")
            DB_USER = os.getenv("DB_USER")
            DB_PASS = os.getenv("DB_PASS")
            DB_NAME = os.getenv("DB_NAME")
            jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

            return spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", caminho_ou_view) \
                .option("user", DB_USER) \
                .option("password", DB_PASS) \
                .load()
        except Exception as e:
            print(f"Erro ao acessar o banco de dados: {e}")
            return None
    else:
        print("Fonte de dados inválida. Escolha 'csv' ou 'db'.")
        return None
        
# ==============================================================
