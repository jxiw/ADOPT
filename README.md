# ADOPT: Adaptively Optimizing Attribute Orders for Worst-Case Optimal Join Algorithms via Reinforcement Learning

ADOPT is a query engine that combines adaptive query processing with worst-case optimal join algorithms. See technical report is [here](https://github.com/jxiw/ADOPT/blob/main/report/ADOPT.pdf)

**If you want to run our demo, please check this code https://github.com/jxiw/ADOPT/releases/tag/ADOPT**

**If you want to run TPC-H or JCC-H, please checkout tpch branch**

**If you want to run imdb, please checkout imdb branch**

# Running the Graph Benchmark

Our graph dataset is from [Stanford Large Network Dataset Collection](https://snap.stanford.edu/data/)

Download the [jar libs](https://drive.google.com/file/d/1EKkJyVHj-JZeTHpNb8amCyfrqg7MBc99/view?usp=sharing), and put it in the ADOPT folder.

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

# Citation

If you use our repository or you are inspired by our work, please cite the following papers.

```
@article{wang2023adopt,
  title={ADOPT: Adaptively Optimizing Attribute Orders for Worst-Case Optimal Join Algorithms via Reinforcement Learning},
  author={Wang, Junxiong and Trummer, Immanuel and Kara, Ahmet and Olteanu, Dan},
  journal={Proceedings of the VLDB Endowment},
  volume={16},
  number={11},
  pages={2805--2817},
  year={2023},
  publisher={VLDB Endowment}
}

@article{10.14778/3611540.3611629,
author = {Wang, Junxiong and Gray, Mitchell and Trummer, Immanuel and Kara, Ahmet and Olteanu, Dan},
title = {Demonstrating ADOPT: Adaptively Optimizing Attribute Orders for Worst-Case Optimal Joins via Reinforcement Learning},
year = {2023},
issue_date = {August 2023},
publisher = {VLDB Endowment},
volume = {16},
number = {12},
issn = {2150-8097},
url = {https://doi.org/10.14778/3611540.3611629},
doi = {10.14778/3611540.3611629},
abstract = {Performance of worst-case optimal join algorithms depends on the order in which the join attributes are processed. It is challenging to identify suitable orders prior to query execution due to the huge search space of possible orders and unreliable execution cost estimates in case of data skew or data correlation.We demonstrate ADOPT, a novel query engine that integrates adaptive query processing with a worst-case optimal join algorithm. ADOPT divides query execution into episodes, during which different attribute orders are invoked. With runtime feedback on performance of different attribute orders, ADOPT rapidly approaches near-optimal orders. Moreover, ADOPT uses a unique data structure which keeps track of the processed input data to prevent redundant work across different episodes. It selects attribute orders to try via reinforcement learning, balancing the need for exploring new orders with the desire to exploit promising orders. In experiments, ADOPT outperforms baselines, including commercial and open-source systems utilizing worst-case optimal join algorithms, particularly for complex queries that are difficult to optimize.},
journal = {Proc. VLDB Endow.},
month = {aug},
pages = {4094–4097},
numpages = {4}
}
```
