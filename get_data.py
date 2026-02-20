import requests
import pandas as pd

def get_data(cep):
    endpoint = f"https://viacep.com.br/ws/{cep}/json/"
    
    response = requests.get(endpoint)
    
    if response.status_code == 200: # 200 indica sucesso
        cep_info = response.json()
        return cep_info
    else:
        print(f"Erro ao obter dados para o CEP {cep}: {response.status_code}")
        return None

users_csv = "01-bronze-raw/usuarios.csv"
users_df = pd.read_csv(users_csv)

ceps = users_df['cep'].tolist()

cep_info_list = []

for cep in ceps:
    cep_info = get_data(cep)
    # se contém a chave 'erro', significa que o CEP é inválido
    if 'erro' in cep_info:
        print(f"CEP inválido: {cep}")
        cep_info_list.append({
            'cep': cep,
            'logradouro': None,
            'bairro': None,
            'localidade': None,
            'uf': None
        })
    cep_info_list.append(cep_info)

cep_info_df = pd.DataFrame(cep_info_list)
final_df = pd.concat([users_df, cep_info_df], axis=1)
final_df.to_csv("01-bronze-raw/usuarios_com_cep_info.csv", index=False)