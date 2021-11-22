# IDaS-RL (Core)

Esta é uma ferramenta de record linkage desenvolvida por uma equipe do projeto
Recovida a partir do código
do [Cidacs-RL](https://github.com/gcgbarbosa/cidacs-rl-v1).

Uma interface gráfica está disponível no repositório recovida/idas-rl-gui>.


## Execução

Inicialmente, [instale o Java](https://www.java.com/pt-BR/download/manual.jsp)
na versão 8 ou qualquer versão mais recente.

Faça o download da última versão do IDaS-RL (Core) na página
[Releases](https://github.com/Recovida/idas-rl-core/releases).

Dentro do diretório (pasta) em que o arquivo `.jar` estiver salvo, crie
um subdiretório chamado `assets` contendo um arquivo de configurações chamado
`config.properties`, seguindo o formato descrito pela [documentação](doc/).

Abra uma janela de um terminal / prompt de comando / Powershell, a depender
do sistema operacional, e entre no diretório do arquivo `.jar`.
Para executar o programa, digite
```
java -jar idas-rl-core-packaged.jar
```
(substituindo `idas-rl-core-packaged.jar` pelo nome exato do arquivo salvo,
  que varia conforme a versão).
O programa será executado lendo o arquivo de configurações
`assets/config.properties`.

Caso queira utilizar um arquivo de configurações com outro nome ou em outra
localização, passe o nome desse arquivo como o único argumento do programa,
de acordo com o formato a seguir:
```
java -jar idas-rl-core-packaged.jar arquivo-de-configurações.properties
```
(substituindo `idas-rl-core-packaged.jar`
  e `arquivo-de-configurações.properties`
  pelos nomes exatos dos arquivos).


## Compilação

Este repositório utiliza o
[Maven](https://maven.apache.org/) para gerenciar o programa, o processo de
compilação e suas dependências.

Estando na raiz do repositório, execute ```mvn compile``` para compilar
o programa, ```mvn exec:java``` para executar o programa e ```mvn package```
para gerar o arquivo `.jar` no diretório `target`.

## Tradução

Para adicionar suporte a um novo idioma na exibição de mensagens para o
usuário, acrescente ao arquivo
[languages.txt](src/main/resources/lang/languages.txt)
uma nova linha com a
[*language tag*](https://docs.oracle.com/javase/tutorial/i18n/locale/matching.html)
correspondente ao idioma, e crie um arquivo com extensão `.properties` no
diretório [`lang`](src/main/resources/lang/) com as mensagens traduzidas.
Utilize um dos idiomas existentes como base e mantenha o
[formato](https://docs.oracle.com/javase/8/docs/api/java/text/MessageFormat.html)
das mensagens com argumentos. Recomenda-se utilizar uma ferramenta como o
[ResourceBundle Editor](https://marketplace.eclipse.org/content/resourcebundle-editor)
para facilitar o processo.


## Licença

Os conteúdos deste repositório estão publicados sob a licença [MIT](LICENSE).



## Projeto Recovida

Este repositório e os demais repositórios deste grupo fazem parte do projeto
**Recovida**
(*Reavaliação da Mortalidade por Causas Naturais no Município de São Paulo
durante a Pandemia da COVID-19*),
da
[Faculdade de Medicina da Universidade de São Paulo](https://www.fm.usp.br/),
sob responsabilidade do
[Prof. Dr. Paulo Andrade Lotufo](https://uspdigital.usp.br/especialistas/especialistaObter?codpub=F7A214F0B89F),
e com a atuação da [Dra. Ana Carolina de Moraes Fontes Varella](https://bv.fapesp.br/en/pesquisador/690479/ana-carolina-de-moraes-fontes-varella/) como supervisora de dados.

Sob a orientação de Paulo Lotufo e a supervisão de Ana Varella,
o desenvolvimento está sendo feito por:

- Débora Lina Nascimento Ciriaco Pereira (bolsista de dez/2020 a set/2021);
- Vinícius Bitencourt Matos (bolsista de dez/2020 a set/2021).


## Apoio

Agradecemos à iniciativa [Todos pela Saúde](https://www.todospelasaude.org/),
da [Fundação Itaú para Educação e Cultura](https://fundacaoitau.org.br/),
pelo financiamento deste projeto. 

Agradecemos também à
[Secretaria Municipal da Saúde da Prefeitura da Cidade de São Paulo](https://www.prefeitura.sp.gov.br/cidade/secretarias/saude/)
pela parceria durante a execução do projeto. 

Esta ferramenta de *record linkage* foi desenvolvida a partir do código do programa
[Cidacs-RL](https://github.com/gcgbarbosa/cidacs-rl-v1), criado pelo
*[Centro de Integração de Dados e Conhecimentos para Saúde](https://cidacs.bahia.fiocruz.br/)* \(Cidacs\),
da [Fiocruz Bahia](https://www.bahia.fiocruz.br/).
O programa foi adaptado para adequar-se às necessidades
de nosso projeto, com mudanças em algumas de suas partes e acréscimo de funcionalidades.
<br/>
**DISCLAIMER:** O Cidacs não possui vínculo com o projeto Recovida e não tem
responsabilidade sobre esta ferramenta, suas novas funções e possíveis erros.
Agradecemos ao Cidacs por disponibilizar livremente o código-fonte do Cidacs-RL.
