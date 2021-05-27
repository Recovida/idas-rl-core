# IDaS-RL - Documentação

O formato do arquivo `config.properties` utilizado pelo IDaS-RL é baseado
no formato do Cidacs-RL.

## Comentários

Em um arquivo de configurações, linhas que começam com uma cerquilha ("#")
são consideradas comentários e são ignoradas. Note que a edição de um
arquivo de configurações por meio do
[editor gráfico](https://gitlab.com/recovida/idas-rl-gui)
**remove** todos os
comentários do arquivo.

## Campos sobre os datasets

- `db_a` e `db_b`: indicam os nomes dos arquivos que contêm os datasets A e B,
  respectivamente, no formato CSV ou DBF. Ambos são obrigatórios.
- `encoding_a` e `encoding_b`: indicam a codificação dos arquivos dos datasets
  A e B, respectivamente. Podem ser omitidos se a codificação for *UTF-8*.
- `suffix_a` e `suffix_b`: determinam os sufixos a serem adicionados aos nomes
  das colunas dos datasets A e B, respectivamente. Se omitidos, os
  valores-padrão _dsa_ e _dsb_ serão utilizados. Colunas que forem renomeadas
  não ganham sufixos.
- `row_num_col_a`e `row_num_col_a`: nomes das colunas do arquivo de saída que
  guardarão os números das linhas das tabelas originais. Se omitidos, os
  valores-padrão _#A_ e _#B_ serão utilizados.

Exemplo do formato:

```ini
db_a = assets/aaaaaa.dbf
db_b = assets/bbbbbb.csv

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
  Note que a pontuação varia de 0 a 100.
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
- `max_rows`: Utilizado para interromper o linkage após um determinado número
de colunas do dataset A. Deve ser utilizado apenas para fim de
teste, visto que esse limite é a quantidade de linhas do dataset A que
o programa vai considerar (por exemplo, se esse valor for 50, a 51ª linha
  e todas as linhas subsequentes serão ignoradas,
  independentemente do sucesso do linkage
  das primeiras 50 linhas).

Exemplo do formato:

```ini
db_index = assets/index_dir
linkage_folder = assets/linkage
min_score = 90.0
num_threads = 3
max_rows = 50
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
sejam geradas internamente e utilizadas no linkage com peso `0.9`. Neste
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
