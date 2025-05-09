# VerificaRegras

Um projeto desenvolvido para validar regras de negócio em arquivos armazenados no Azure Blob, garantindo a integridade e a precisão dos dados.

## 📋 Funcionalidades

- Validação automatizada de regras em arquivos CSV e Excel
- Logs detalhados para rastrear e analisar verificações
- Estrutura modular para fácil manutenção e expansão

## 📌 Requisitos

- Python 3.10 ou superior
- Git instalado
- Conta configurada no Azure Blob (se necessário)

## 🚀 Instalação

1. Clone o repositório:

```bash
git clone https://github.com/RodrigoBolgheroni/VerificaRegras.git
```

2. Acesse o diretório do projeto:

```bash
cd VerificaRegras
```

3. Crie e ative um ambiente virtual (opcional, mas recomendado):

```bash
python -m venv venv
# No Windows
venv\Scripts\activate
# No Linux/Mac
source venv/bin/activate
```

4. Instale as dependências:

```bash
pip install -r requirements.txt
```

## ▶️ Uso

1. Certifique-se de que os arquivos a serem validados (CSV ou Excel) estejam no diretório correto.

2. Execute o script principal para iniciar a validação:

```bash
python main.py
```

3. Personalize as regras de validação nos arquivos de configuração localizados no diretório `config`.

## 📊 Estrutura do Projeto

```
VerificaRegras/
├── .vscode/            # Configurações do Visual Studio Code
├── config/             # Arquivos de configuração
├── utils/              # Funções utilitárias e módulos auxiliares
├── main.py            # Arquivo principal para execução
├── requirements.txt   # Dependências do projeto
└── README.md          # Documentação
```

