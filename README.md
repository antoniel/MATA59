# UFBA-MATA59: Sistema de Armazenamento Distribuído com Replicação

## Descrição do Projeto

Este projeto é um sistema de armazenamento distribuído com capacidade de replicação de arquivos. Ele foi desenvolvido como um trabalho acadêmico e tem como objetivo principal demonstrar os conceitos de redes de computadores, sistemas distribuídos e programação em rede.

## Integrantes

- Antoniel Magalhães de Sousa
- Felipe Angelo
- João Victor Leahy de Melo
- Luis Felipe
- Luis Felipe Cordeiro Sena

Professor orientador: Gustavo Bittencourt Figueiredo

## Ideia Principal

O sistema permite que arquivos sejam armazenados em um ambiente distribuído com várias réplicas. Isso aumenta a confiabilidade e a disponibilidade dos dados, pois os arquivos são replicados em diferentes locais (buckets). O sistema segue o modelo cliente-servidor, com um servidor principal (Gateway) que gerencia as requisições dos clientes e a comunicação com os buckets.

## Arquitetura do Sistema

A arquitetura é composta por três componentes principais:

1. **Gateway**: O servidor central que gerencia as requisições dos clientes. Ele é responsável por redirecionar as solicitações de armazenamento e recuperação para os buckets adequados, bem como gerenciar a replicação dos arquivos.

2. **Bucket**: Unidades de armazenamento que mantêm as cópias dos arquivos. Cada bucket pode receber solicitações do Gateway para armazenar ou fornecer arquivos, e também para ajustar o número de réplicas.

3. **Cliente**: Usuários que interagem com o sistema enviando arquivos para armazenamento ou solicitando arquivos armazenados. Eles se comunicam apenas com o Gateway.

## Como Rodar o Projeto

Para executar o sistema, siga as etapas abaixo:

1. **Configurar o Ambiente**:
   Certifique-se de que Python está instalado em sua máquina. Este projeto foi desenvolvido usando Python 3.8.

2. **Clonar o Repositório**:
   Faça o clone do repositório do projeto para sua máquina local.

   ```
   git clone https://github.com/antoniel/MATA59
   ```

3. **Iniciar o Servidor Gateway**:
   Navegue até o diretório do projeto e execute o script do Gateway.

   ```
   cd MATA59
   python gateway.py
   ```

4. **Iniciar os Buckets**:
   Em terminais separados, inicie cada Bucket.

   ```
   python bucket.py [ID do Bucket]
   ```

5. **Executar o Cliente**:
   Use o script do cliente para interagir com o sistema.

   ```
   python client.py  [ID do usuário]
   ```

   As ações podem incluir enviar um arquivo, solicitar um arquivo, ajustar réplicas, etc.
