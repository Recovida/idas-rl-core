# IDaS-RL (Core)

Esta é uma ferramenta de record linkage desenvolvida por uma equipe do projeto
Recovida a partir do código
do [Cidacs-RL](https://github.com/gcgbarbosa/cidacs-rl-v1).

Uma interface gráfica está disponível no repositório recovida/idas-rl-gui>.


## Execução

Inicialmente, [instale o Java](https://www.java.com/pt-BR/download/manual.jsp)
na versão 8 ou qualquer versão mais recente.

Faça o download da última versão do IDaS-RL (Core) na página
[Releases](https://gitlab.com/recovida/idas-rl-gui/-/releases) deste
repositório.

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
