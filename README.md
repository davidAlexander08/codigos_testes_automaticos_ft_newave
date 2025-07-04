# codigos_testes_automaticos_ft_newave
Transferência de Conhecimento dos Testes Automáticos da FT-NW

Atividade interrompida na Etapa 01.

OBS: Testes realizados com o deck exemplo na pasta do código com a versão 30.0.1, então pode não estar funcionando para outras versões de Newave, verificar antes.
OBS2: Testado apenas na nuvem da PEM por enquanto por motivos de desenvolvimento, pode não funcionar em outras nuvens.
OBS3: Há um pull request aberto na inewave que corrige um ponto da biblioteca, permitindo uso dos testes de pmo.dat. (Título do pull request: Atualização leitura c_adic e sistema.dat)


Etapa 01
-  Automatização dos testes do pmo.dat e alguns demais testes em arquivos separados.
-  Trocar prints por logs.

Etapa 02
- Modularização dos testes unindo funções em comum para transformar em um código coeso e com padrões de Python para código aberto.

Etapa 03
- Disponibilização para uso por agentes do setor em seus clusters pessoais na forma de código aberto por github ou alguma aplicação com base nos códigos em python. 


Como executar:

- Ter o python na máquina e baixar o resitório do Github
- Com o repositório executar o comando

(Windows)
python main.py ftnewave deck.txt

(Linux)
python3 main.py ftnewave deck.txt


Arquivo "deck.txt" é o arquivo de leitura do programa para realizar os testes automáticos.
Comentários com &

caminho="/home/david/FTNEWAVE/deck_teste"
Indica o caminho do deck a ser rodado


Palavras chave dos testes:

&Evaporacao
&Balanco_Hidrico_Usina
&Balanco_Demanda
&Restricao_Eletrica_Especial
&Altura
&FPHA
ECO
