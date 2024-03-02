Repository for paper "CLIMBER:Pivot-Based Approximiate Similarity Search over Big Data Series".

* [Enviornment](#cluster-environment)
* [Source Code](#source-code)
* [Experimental](#experimental)

## Cluster Environment
### Cluster Software
Install Apache Spark and Hadoop for your cluster afer checking [here](https://spark.apache.org/downloads.html) for the compatibility.\
Update the spark binary path and shell path in `./etc/startenv.sh` and `./etc/stopenv.sh`.

### Cluster Configuration

Update application configure in the `spark-defaults.conf`.  
The configure file of cluster should be stored under `hadoop/etc/hadoop/` and `spark/conf` directories.


## Source Code

### Structure
The functional features include: `Index creation` and `Query process` whiile the utilization features include: `Randomwalk creater`, `Evaluation` and `Ground truth generation`.

- Main Entry: `src/.../climber/RunMain.scala`
- Index data structure: `src/.../climber/idx/*`
  - pivot: `src/.../climber/idx/pivots.scala`
  - group: `src/.../climber/idx/group.scala`
  - pivot_to_group: `src/.../climber/idx/Pivot_2_Group.scala`
  - group_to_partition: `src/.../climber/idx/Group_2_Partition.scala`
  - trie: `src/.../climber/idx/Trie.scala`
  - compress_structure: `src/.../climber/idx/Compress_Structure.scala`
- Index build: `src/.../climber/Index.scala`
- Query process: `src/.../climber/Query.scala`
- RandomWalk dataset generation: `src/.../climber/RandWalkTsCreater.scala`
- Ground truth generation: `src/.../climber/RandWalkTsCreater.scala`
- Configure:  `src/.../climber/cfg/*`
  - index config: `src/.../climber/cfg/IdxCfg.scala`
  - ground truth config: `src/.../climber/cfg/GtCfg.scala`
  - randomwalk config: `src/.../climber/cfg/RwCfg.scala` 

### Critical Functions

```scala
/** generate pivots, cache pivots in-memory and persist one copy to disk;
 *  since dataset is load, convert all data series to paa (cache);
 *  input: sc: sparkContext for running RDD
 *         idxcfg: config for index construction
 *  output: RDD[Array[Float]]: dataseries_paa;
 *           Broadcast[Array[(Short, Array[Float])]]: pivot_id and pivots; */          
def Index.sample_pivots(sc: SparkContext, idxcfg: IdxCfg): 
                  (RDD[Array[Float]], 
                   Broadcast[Array[(Short, Array[Float])]])
```

```scala
/** convert paa to ranking-sensitive representation;
 *  input: paa: paa rdd of data series
 *         pivots: pivot id to pivots
 *         prefix_length: prefix length
 *  output: Array[Short]: representation; */  
def Ops.cvtPaaTo4Ps_os(paa: Array[Float],
                   pivots: Array[(Short, Array[Float])],
                   prefix_length: Int): Array[Short]
```

```scala
/** retrieve centers for groups;
 *  input: p4s_os_weight_sample: p4s_os with weight 
 *  output: Array[Short]: centers; */  
def Group.retrieve(sc: SparkContext,
            p4s_os_weight_sample: RDD[(String, Long)],
            idxcfg: IdxCfg): Array[(Int, Array[Short])]
```

```scala
/** split group into partitions;
 *  input:  p4s_os_weight:  p4s_os with weight
 *          centers: centers of groups
 *  output: Int: partition number;
 *          Array[(Int, Trie)]: center_to_partition; */ 
def Group.split_groups(sc: SparkContext,
                 p4s_os_weight: RDD[(String, Long)],
                 centers: Array[(Int, Array[Short])],
                 idxcfg: IdxCfg): (Int, Array[(Int, Trie)])
```
```scala
/** trie construction;
 *  input:  p4s_os_weight: p4s_os with weight
 *          block_cap: capacity of block */ 
def Trie.construct(p4s_os_weight: Array[(String, Long)],
            block_cap: Int): Unit
```
```scala
/** shuffle all data series;
 *  input:  centers: centor id to center
 *          partition_num: total partition number
 *          center_2_partition: map from cent to partition
*/ 
def Index.shuffle_data_partition_mode(sc: SparkContext,
                                centers: Array[(Int, Array[Short])],
                                partition_num: Int,
                                center_2_partition: Array[(Int, Trie)],
                                idxcfg: IdxCfg): Unit
```

```scala
/** query one data series;
 *  input: query: query data series;
 *         pivots: pivots id to pivot
 *         comp_struct: compress structure of index
 *  output: Array[Float]: distance to query;
 *          int: candidate size;
 *          int: partition size; */
def Query.process_one_query_partition_mode(sc: SparkContext,
                                    query: Array[Float],
                                    pivots: Array[(Short, Array[Float])],
                                    comp_struct: Compress_Structure,
                                    idxcfg: IdxCfg
                                    ): (Array[Float], Int, Int)
```

```scala
/** index traversal;
 * input: p4s_os: p4s_os representation of query data series
 *  output: Array[Int]: partition ids; */ 
def Query.search_partition_mode(p4s_os: Array[Short], 
                          idxcfg: IdxCfg): Array[Int]
```

## Experimental
### Compilation
The code has been writen in Scala and compiled in `SBT`.\
Please update `scalaVersion` and `libraryDependencies` about `spark package` in build.sbt file of source code before compiling it.\
Make sure the compatibility with `spark packages`.\
Since I use Jetbrain IDEA to compile code, please refer to following linke to learn how to compile sbt project using IDEA.\
https://www.scala-sbt.org/1.x/docs/Running.html\
https://www.jetbrains.com/help/idea/sbt-support.html#sbt_task

### Configure 

The configure file is `./etc/config.conf`.\
The log is under `./etc/log/`, and log name can be set by `logFileName`.\
Key parameters:\
**Index:**
* mode: paratition or group;
* ts_length: data series dimension;
* paa_length: paa length;
* pivots_num: pivots number;
* permutation_prefix_length: length of permutation prefix;
* rwDirPath: raw data path;

**Query:**
* dataset_path: data path for clustered data;
* query_num: query number;
* knn_k: k value for similarity query;
* dataset_query_path: queries path;
* label_truth: ground truth path;

**Other**
* logFileName: log path;
* blockSize: default HDFS block size, like 64 (MB) or 128 (MB)
* rw**  : parameters generate randomwalk data series;
* gt**  : parameters create ground truth;

### Run code:
Compile code and get the jar file: `climer.jar`

```sh
~/spark/bin/spark-submit --class org.apache.spark.edu.wpi.dsrg.climber --properties-file ./spark-defaults.conf  climer.jar -h
```
* -h : display help information;
* -g : generate raw data series;
* -c : generate groud truth;
* -b : build index;
* -q : run data series similarly query;

#  Fairness of Usage 
Please ensure that the code is used correctly.\
If our research is beneficial for publication, kindly cite our paper.
