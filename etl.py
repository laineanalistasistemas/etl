import os
import pandas as pd
import requests
from dotenv import load_dotenv
import openai

# Carregar variáveis de ambiente
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SANTANDER_API_KEY = os.getenv("SANTANDER_API_KEY")

openai.api_key = OPENAI_API_KEY

# -------------------------------
# 🔍 EXTRAÇÃO
# -------------------------------
def extract_user_ids(file_path: str):
    """Extrai IDs de usuários de uma planilha Excel/CSV."""
    df = pd.read_csv(file_path)  # ou pd.read_excel(file_path)
    return df["user_id"].tolist()

def get_user_data(user_id: str):
    """Consulta API Santander Dev Week 2023 para obter dados detalhados."""
    url = f"https://api.santanderdevweek.com/users/{user_id}"
    headers = {"Authorization": f"Bearer {SANTANDER_API_KEY}"}
    response = requests.get(url, headers=headers)
    return response.json()

# -------------------------------
# ⚙️ TRANSFORMAÇÃO
# -------------------------------
def generate_marketing_message(user_data: dict):
    """Usa GPT-4 para criar mensagem personalizada de marketing."""
    prompt = f"""
    Crie uma mensagem de marketing personalizada para o usuário:
    Nome: {user_data.get('name')}
    Produto de interesse: {user_data.get('product')}
    """
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=150
    )
    return response.choices[0].message["content"]

# -------------------------------
# 📤 CARREGAMENTO
# -------------------------------
def send_message_to_api(user_id: str, message: str):
    """Envia mensagem personalizada de volta para API Santander Dev Week 2023."""
    url = f"https://api.santanderdevweek.com/messages/{user_id}"
    headers = {"Authorization": f"Bearer {SANTANDER_API_KEY}"}
    payload = {"message": message}
    response = requests.post(url, json=payload, headers=headers)
    return response.status_code

# -------------------------------
# 🚀 PIPELINE PRINCIPAL
# -------------------------------
def run_pipeline():
    user_ids = extract_user_ids("data/users.csv")
    for user_id in user_ids:
        user_data = get_user_data(user_id)
        message = generate_marketing_message(user_data)
        status = send_message_to_api(user_id, message)
        print(f"Mensagem enviada para {user_id} - Status: {status}")

if __name__ == "__main__":
    run_pipeline()
