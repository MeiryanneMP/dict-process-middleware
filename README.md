Projeto:
-------------------------------------------------------------------------------------------------------------
Middleware que processa dados e formata de acordo com padrão configurado no dicionário.
Após o processamento, é possível enviar para API a lista de produtores solicitada.

Requisitos:
-------------------------------------------------------------------------------------------------------------
Python: 3.x (versão recomendada)

pip: O gerenciador de pacotes do Python

API rodando: https://github.com/MeiryanneMP/golden-raspberry-awards-api

Passos para rodar o projeto:
-------------------------------------------------------------------------------------------------------------
git clone https://github.com/MeiryanneMP/golden-raspberry-awards-middleware.git

Para garantir que você está isolado de outras dependências no seu sistema, crie um ambiente virtual Python.
O comando varia de acordo com a versão do Python que você tem instalada.

No Linux/MacOS:

python3 -m venv venv
source venv/bin/activate

No Windows:

python -m venv venv
.\venv\Scripts\activate

Instale as dependências:

pip install -r requirements.txt

Rodando o projeto:
-------------------------------------------------------------------------------------------------------------
python3 middleware.py

Considerações
--------------------------------------------------------------------------------------------------------------
Caso queira rodar outro conjunto de dados CSV, basta alterar ou editar o arquivo na pasta DATA do projeto.
Certifique-se de que a API esteja rodando antes de enviar os dados.
