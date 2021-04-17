-- crea la carpeta input in el HDFS
fs -mkdir paracontar

-- copia los archivos del sistema local al HDFS
fs -put paracontar/ .

-- carga de datos
lines = LOAD 'paracontar/text*.txt' AS (line:CHARARRAY);

-- genera una tabla llamada words con una palabra por registro
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- agrupa los registros que tienen la misma palabra
grouped = GROUP words BY word;

-- genera una variable que cuenta las ocurrencias por cada grupo
wordcount = FOREACH grouped GENERATE group, COUNT(words);

-- selecciona las primeras 15 palabras
s = LIMIT wordcount 15;

-- escribe el archivo de salida
STORE s INTO 'outConteo';

-- copia los archivos del HDFS al sistema local
fs -get outConteo/ .

-- pig -execute 'run conteo.pig'
-- hadoop fs -cat outConteo/*
-- cat outConteo/*