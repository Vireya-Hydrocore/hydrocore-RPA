import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRESQL_URI_primeiro = os.getenv("POSTGRESQL_URI_primeiro")
POSTGRESQL_URI_segundo = os.getenv("POSTGRESQL_URI_segundo")

conn1 = psycopg2.connect(POSTGRESQL_URI_primeiro)
conn2 = psycopg2.connect(POSTGRESQL_URI_segundo)

cursor2 = conn2.cursor()

lista_queries = [
    "SELECT * FROM eta;",
    "SELECT * FROM funcionario;",
]

def extrair_dados_e_criar_dfs(conn, lista_queries):
    """Extrai dados de múltiplas tabelas e os armazena em DataFrames."""
    dataframes_dict = {}
    try:
        for query in lista_queries:
            tabela_nome = query.split('FROM ')[1].replace(';', '')
            print(f"\nExtraindo dados da tabela '{tabela_nome}'...")
            df = pd.read_sql_query(query, conn)
            dataframes_dict[tabela_nome] = df
        return dataframes_dict
    except Exception as e:
        print(f"Erro ao extrair dados do banco de dados: {e}")
        return {}

try:
    dfs = extrair_dados_e_criar_dfs(conn1, lista_queries)
    
    # -----------------------------
    # Processando a tabela ETA
    # -----------------------------
    print("\nIniciando o upsert para a tabela 'eta'...")
    df_eta = dfs.get("eta")
    if df_eta is not None:
        for index, row in df_eta.iterrows():
            cursor2.execute("SELECT id_eta FROM eta WHERE id_eta = %s;", (row['id'],))
            if cursor2.fetchone():
                update_query = """
                    UPDATE eta 
                    SET nome = %s, capacidade_tratamento = %s, telefone = %s, id_endereco = %s 
                    WHERE id_eta = %s;
                """
                cursor2.execute(update_query, (row['nome'], row['capacidade'], row['telefone'], row['id_endereco'], row['id']))
            else:
                insert_query = """
                    INSERT INTO eta (id_eta, nome, capacidade_tratamento, telefone, id_endereco) 
                    VALUES (%s, %s, %s, %s, %s);
                """
                cursor2.execute(insert_query, (row['id'], row['nome'], row['capacidade'], row['telefone'], row['id_endereco']))
        print("Upsert da tabela 'eta' concluído.")
    
    # -----------------------------
    # Processando a tabela FUNCIONARIO
    # -----------------------------
    print("\nIniciando o upsert para a tabela 'funcionario'...")
    df_funcionario = dfs.get("funcionario")
    if df_funcionario is not None:
        for index, row in df_funcionario.iterrows():
            cursor2.execute("SELECT email FROM funcionario WHERE email = %s;", (row['email'],))
            if cursor2.fetchone():
                update_query = """
                    UPDATE funcionario SET id_funcionario = %s, nome = %s, email = %s, data_admissao = %s, data_nascimento = %s ,id_eta = %s , id_cargo = %s WHERE email = %s;
                """
                cursor2.execute(update_query, (row['id'], row['nome'], row['email'], row['data_admissao'], row['data_nascimento'],row['id_eta'], row['id_cargo'], row['email']))
            else:
                insert_query = """
                    INSERT INTO funcionario (id_funcionario, nome, email, data_admissao, data_nascimento, id_eta, id_cargo) VALUES (%s, %s, %s, %s, %s, %s, %s);
                """
                cursor2.execute(insert_query, (row['id'], row['nome'], row['email'], row['data_admissao'], row['data_nascimento'],row['id_eta'], row['id_cargo']))

        print("Upsert da tabela 'funcionario' concluído.")
    
    conn2.commit()
    print("\nProcesso concluído com sucesso!")

except Exception as e:
    print(f"\nOcorreu um erro no processo: {e}")
    conn2.rollback()

finally:
    cursor2.close()
    conn1.close()
    conn2.close()
    print("Conexões com os bancos de dados fechadas.")