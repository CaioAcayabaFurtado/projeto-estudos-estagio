import funcoes

# ==============================================================
# Captura de Dados com Base na Escolha do Usuário

def main():

    while True:
        escolha = input("Digite a fonte de dados (csv ou db): ").strip().lower()
        if escolha not in ["csv", "db"]:
            print("Opção inválida. Tente novamente.")
        else:
            break

    caminho_ou_view = input("Digite o caminho do arquivo CSV ou nome da view: ").strip()
    base_dados = capturar_dados(escolha, caminho_ou_view)
    base_dados_filtrada = limpar_base_dados(base_dados)

    if base_dados is not None:
        base_dados.show()
        base_dados_filtrada.show()
    else:
        print("Não foi possível capturar os dados.")

main()

# ==============================================================
