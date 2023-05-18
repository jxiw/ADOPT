# ADOPT: Adaptively Optimizing Attribute Orders for Worst-Case Optimal Join Algorithms via Reinforcement Learning

ADOPT is a query engine that combines adaptive query processing with worst-case optimal join algorithms. See technical report is [here](https://github.com/jxiw/ADOPT/blob/main/report/ADOPT.pdf)

**If you want to run our demo, please check this code https://github.com/jxiw/ADOPT/releases/tag/ADOPT**

# Running the Graph Benchmark

Our graph dataset is from [Stanford Large Network Dataset Collection](https://snap.stanford.edu/data/)

1. Compile source code
```commandline
bash compile.sh
```
2. Create a database. Please replace the GRAPH_NAME to graph to be created and FOLDER_SAVE_GRAPH to the save path in your local machine. 
```commandline
cd bin
java tools.CreateDB GRAPH_NAME FOLDER_SAVE_GRAPH
```

3. Import graph data. Please replace the FOLDER_SAVE_GRAPH to the graph save path in your local machine and PATH_TO_GRAPH_CSV_FILE to the graph csv file path.
```commandline
java -classpath .:../lib/* -Xmx16G -XX:+UseConcMarkSweepGC console.SkinnerCmd FOLDER_SAVE_GRAPH
exec create.sql
load edge PATH_TO_GRAPH_CSV_FILE , NULL;
```

4. Run queries.
```commandline
export budget=10000
export lr=1e-6
export thread=70
java -Xmx320G -Xms320G -classpath .:../lib/* benchmark.BenchAndVerify PATH_TO_GRAPH_FOLDER PATH_TO_GRAPH_SQL ${budget} ${thread} ${lr} > result.txt
```

