# 🗄️ Sincronização de Bancos PostgreSQL (Upsert Automático)

## 📘 Descrição
Este projeto realiza a **sincronização automática de dados** entre dois bancos de dados **PostgreSQL** distintos.  
Ele extrai informações de tabelas específicas do banco **origem**, compara com o banco **destino** e executa um processo de **upsert** (atualização ou inserção conforme necessário).

Atualmente, o script está configurado para trabalhar com as tabelas:
- `eta`
- `funcionario`

---

## ⚙️ Funcionalidades
✅ Conecta a dois bancos PostgreSQL usando variáveis de ambiente.  
✅ Extrai dados de múltiplas tabelas do banco origem.  
✅ Cria *DataFrames* com os dados extraídos (via `pandas`).  
✅ Realiza **upsert**:  
  - **UPDATE** se o registro já existir no destino;  
  - **INSERT** se for um novo registro.  
✅ Garante consistência via *commit* e *rollback* automáticos.  
✅ Fecha todas as conexões ao final do processo.

---

## 🧩 Tecnologias Utilizadas
- Python 3.x  
- [psycopg2](https://pypi.org/project/psycopg2/) — Conexão com PostgreSQL  
- [pandas](https://pandas.pydata.org/) — Manipulação de dados  
- [python-dotenv](https://pypi.org/project/python-dotenv/) — Leitura de variáveis de ambiente  

---

## 🗂️ Estrutura Básica do Código
📁 projeto/
├── .env
├── main.py
├── requirements.txt
└── README.md


---

## 🔑 Configuração do `.env`
Antes de rodar o script, crie um arquivo `.env` na raiz do projeto com o seguinte conteúdo:


---

## ▶️ Como Executar

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt

python RPA.py

Extraindo dados da tabela 'eta'...
Extraindo dados da tabela 'funcionario'...
Upsert da tabela 'eta' concluído.
Upsert da tabela 'funcionario' concluído.
Processo concluído com sucesso!

🧠 Como o Script Funciona:

Leitura das variáveis de ambiente com as URIs dos bancos.

Conexão simultânea com os dois bancos (conn1 = origem, conn2 = destino).

Extração dos dados usando pandas.read_sql_query().

Iteração linha a linha nos DataFrames:

Verifica se o registro já existe no destino (SELECT ... WHERE id = ...).

Executa UPDATE ou INSERT conforme o caso.

Confirma as alterações com commit().

Fecha os cursores e conexões no final.


🧾 Exemplo de Saída Esperada
Extraindo dados da tabela 'eta'...
Extraindo dados da tabela 'funcionario'...

Iniciando o upsert para a tabela 'eta'...
Upsert da tabela 'eta' concluído.

Iniciando o upsert para a tabela 'funcionario'...
Upsert da tabela 'funcionario' concluído.

Processo concluído com sucesso!
Conexões com os bancos de dados fechadas.


## ✍️ Autor

**Guilherme Costa**  
Estudante do Instituto Germinare Tech  
💼 Interesse em dados, IA e aplicações sustentáveis  
📅 2025
