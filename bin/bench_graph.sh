budget=10000
lr=1e-6
thread=70

java -Xmx320G -Xms320G -classpath .:../lib/* benchmark.BenchAndVerify PATH_TO_GRAPH_FOLDER PATH_TO_GRAPH_SQL ${budget} ${thread} ${lr} > result.txt
