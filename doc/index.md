# IDaS-RL - Documentação

O formato do arquivo `config.properties` utilizado pelo IDaS-RL é baseado
no formato do Cidacs-RL.

## Comentários

Em um arquivo de configurações, linhas que começam com uma cerquilha ("#")
são consideradas comentários e são ignoradas. Note que a edição de um
arquivo de configurações por meio da
[interface gráfica (LinkaSUS)](https://gitlab.com/recovida/idas-rl-gui)
**remove** todos os
comentários do arquivo, bem como todos os campos não reconhecidos.

## Campos sobre os datasets

- `db_a` e `db_b`: Indicam os nomes dos arquivos que contêm os datasets A e B,
  respectivamente, no formato CSV ou DBF. Ambos são obrigatórios.
  É possível utilizar caminhos completos ou relativos
  **ao arquivo de configurações**
  (ou seja, se o dataset estiver na mesma pasta do arquivo de
  configurações, não é preciso especificar o caminho).
- `encoding_a` e `encoding_b`: Indicam a codificação dos arquivos dos datasets
  A e B, respectivamente. Podem ser omitidos se a codificação for *UTF-8*.
- `lenient_a` e `lenient_b`: Quando verdadeiros ("yes", "1" ou "true"),
  fazem com que a
  decodificação ocorra de forma leniente na leitura dos datasets,
  ignorando sequências de bytes que não correspondem a caracteres na
  codificação escolhida. Caso contrário (valores falsos ou campos omitidos),
  a decodificação será feita no modo estrito, exibindo erro caso existam
  bytes não decodificáveis usando a codificação especificada.
- `suffix_a` e `suffix_b`: Determinam os sufixos a serem adicionados aos nomes
  das colunas dos datasets A e B, respectivamente. Se omitidos, os
  valores-padrão _dsa_ e _dsb_ serão utilizados. Colunas que forem renomeadas
  não ganham sufixos.
- `row_num_col_a`e `row_num_col_a`: Nomes das colunas do arquivo de saída que
  guardarão os números das linhas das tabelas originais. Se omitidos, os
  valores-padrão _#A_ e _#B_ serão utilizados.

Exemplo do formato:

```ini
db_a = aaaaaa.dbf
db_b = bbbbbb.csv

encoding_a = ANSI
encoding_b = UTF-8

suffix_a = AAAA
suffix_b = BBBB

row_num_col_a = NUMCOL_A
row_num_col_b = NUMCOL_B
```

## Campos sobre opções

- `db_index`: Diretório (pasta) em que será gerado o índice do dataset B.
  Campo obrigatório.
- `linkage_folder`: Diretório (pasta) em que será salvo o resultado do
  linkage. Campo obrigatório.
- `min_score`: Pontuação mínima. Se definido, pares de linhas que obtiverem
  pontuação menor que o valor fixado não aparecerão no arquivo de saída,
  mesmo quando não for possível encontrar uma correspondência melhor.
  Note que a pontuação varia de 0 a 100. Independentemente da presença
  deste campo, a pontuação será calculada durante o linkage com base nos
  pesos e armazenada na coluna _score_  do arquivo de saída.
- `num_threads`: Número de threads que serão criadas para o linkage.
  Deve ser um número inteiro positivo, normalmente não ultrapassando
  a quantidade de núcleos (cores)
  do processador (que é o valor utilizado caso o campo seja omitido).
  Em geral, quanto maior o número de threads, menor deve ser o tempo de
  processamento, porém isso pode inviabilizar a utilização da máquina antes
  do fim do processo. Valores menores podem ser escolhidos quando for
  preciso utilizar o
  computador para realizar
  outras tarefas durante a execução do programa.
- `output_dec_sep`: Separador decimal utilizado no arquivo de saída
  (atualmente, apenas para a pontuação e a similaridade).
  Os valores aceitos são `comma`
  (vírgula) e `dot` (ponto). Se omitido ou inválido, é utilizado o separador
  padrão do idioma em que o programa está sendo executado (ponto em inglês,
  vírgula em português e espanhol).
- `output_col_sep`: Separador entre colunas utilizado no arquivo de saída.
  Os valores aceitos são `comma`
  (vírgula), `semicolon` (ponto e vírgula), `colon` (dois-pontos),
  `tab` (tabulação) e `pipe` (barra vertical).
  Se omitido ou inválido, é utilizada a vírgula se o idioma em que
  o programa está sendo executado tiver o ponto como separador decimal
  (como inglês), ou ponto e vírgula se o idioma tiver a vírgula como separador
  decimal (como espanhol ou português).
- `max_rows`: Utilizado para interromper o linkage após um determinado número
  de colunas do dataset A. Deve ser utilizado apenas para fim de
  teste, visto que esse limite é a quantidade de linhas do dataset A que
  o programa vai considerar (por exemplo, se esse valor for 50, a 51ª linha
  e todas as linhas subsequentes serão ignoradas,
  independentemente do sucesso do linkage
  das primeiras 50 linhas).
- `cleaning_regex`: Expressão regular utilizada para limpar valores do tipo
  `name`. Convém notar que a limpeza é feita **após** a retirada de acentos
  e a transformação para letras maiúsculas. Além disso, no arquivo de
  configurações, é preciso fazer "escape" de caracteres como a barra
  invertida. A expressão regular do exemplo do fim desta seção corresponde
  a um padrão que remove certas palavras do fim do nome, seguidas ou não
  por números, que é utilizada por membros da SMS-SP.

Exemplo do formato:

```ini
db_index = assets/index_dir
linkage_folder = assets/linkage
min_score = 90.0
num_threads = 3
max_rows = 50
cleaning_regex = (\\s+(LAUDO|BO|FF|NATIMORTO|DESCONHECIDO|NUA|CHAMADA)\\s*[0-9]*\\s*)+$
```


## Campos sobre as colunas

**Para cada variável relevante para a integração**
(correspondente a um par de colunas), deve ser associado um
número inteiro entre 0 e 999 **exclusivo** para ela,
sem zeros à esquerda. **Para cada coluna que deve ser apenas copiada**,
deve ser associado um número exclusivo da mesma maneira, mas não é
preciso preencher o par (ou seja, é suficiente uma coluna de apenas um dos
datasets).

Os seguintes campos podem ser preenchidos para cada variável, iniciando
sempre com o número seguido de um underscore ("\_"), conforme exemplo
no fim da seção. Variáveis com campos obrigatórios faltantes são ignoradas.

- `type`: Tipo do dado. Campo obrigatório. Os valores possíveis são:
  `name`, `date`, `ibge`, `gender`, `numerical_id`, `categorical` e `copy`.
- `index_a` e `index_b`: Nomes das colunas nas tabelas originais dos datasets
  A e B, respectivamente. Ambos são obrigatórios, exceto para o tipo `copy`,
  em que é suficiente preencher apenas um desses campos.
- `rename_a` e `rename_b`: Nomes das colunas dos dataset A e B a serem
  escritos no arquivo de saída. Se omitidos, são utilizados os nomes originais
  seguidos dos sufixos correspondentes.
- `weight`: Peso. Se for zero, a variável não será considerada.
  Campo obrigatório, exceto para o tipo `copy`, em que não há peso.
- `phon_weight`: Peso das colunas geradas a partir da transformação
  fonética. É opcional, e só pode ser
  utilizado se o tipo for `name`.
- `col_similarity`: Campo opcional, utilizado apenas em variáveis do tipo `name`,
  que contém o nome da nova coluna em que
  será salva a medida de similaridade entre os valores. Ao contrário da
  pontuação (*score*), este cálculo considera **apenas** os valores desta
  variável (presentes nas colunas indicadas por `index_a` e `index_b` nos
  datasets A e B, respectivamente), sendo 100 quando os valores são iguais
  e 0 quando são totalmente diferentes. Quando a variável está em branco
  em ao menos um dos
  datasets, a similaridade não é calculada. 
  <br/>
  <small>
  A fórmula utilizada para o cálculo de similaridade é a
  *[longest common subsequence distance](https://commons.apache.org/proper/commons-text/apidocs/org/apache/commons/text/similarity/LongestCommonSubsequenceDistance.html)*
  após ser normalizada e transformada no complemento. Se `lcsd(s1, s2)` for
  essa distância, a similaridade é definida como
  `1 - 100 * lcsd(s1, s2) / [length(s1) + length(s2)]`.
  </small>
- `min_similarity`: Campo opcional, utilizado apenas quando `col_similarity` está
  presente. Se definido, será a similaridade mínima: o programa descartará
  pares de linhas nas quais os valores desta variável não sejam vazios e
  possuam similaridade menor que o mínimo estabelecido,
  mesmo quando não for possível encontrar uma correspondência melhor.
  Note que a similaridade varia de 0 a 100.

**Exemplo de par de colunas para o linkage:**

Considere que os datasets A e B possuem colunas
`NOME_PESSOA` e `NOME_INDIVIDUO`, respectivamente, contendo nomes, e
queremos que o linkage
utilize essas colunas com peso `1`. Além disso, no arquivo de saída,
queremos que os nomes dessas colunas sejam `NOME_NA_TABELA_A` e
`NOME_NA_TABELA_B`,
respectivamente. A fim de lidar com erros de digitação causados por
maneiras distintas de representar uma mesma pronúncia, queremos que
uma transformação fonética seja utilizada, de modo que colunas adicionais
sejam geradas internamente e utilizadas no linkage com peso 0,9.
Por ser uma variável importante, queremos que a similaridade
entre os valores seja calculada e salva numa coluna nova chamada
`SIMILARIDADE_NOME`, e que correspondências nas quais a similaridade
seja menor que 700 sejam descartadas.
Neste
caso, o arquivo de configurações deverá ter as seguintes linhas (em que
`42` é o número que associamos à variável nome):

```ini
42_type = name
42_index_a = NOME_PESSOA
42_index_b = NOME_INDIVIDUO
42_weight = 1.0
42_phon_weight = 0.9
42_rename_a = NOME_NA_TABELA_A
42_rename_b = NOME_NA_TABELA_B
42_col_similarity = SIMILARIDADE_NOME
42_min_similarity = 700
```

**Exemplo de coluna a ser copiada:**

Considere que o dataset A contém uma coluna `NUMTEL`, que queremos no
arquivo de saída com o nome `TELEFONE`. Se `201` for o número que associamos
a essa coluna, o arquivo de configurações deverá ter as seguintes linhas:

```ini
201_type = copy
201_index_a = NUMTEL
201_rename_a = TELEFONE
```
