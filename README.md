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
 *  output: RDD[Array[Float]]: dataseries_paa;
 *           Broadcast[Array[(Short, Array[Float])]]: pivot_id and pivots; */          
def Index.sample_pivots(sc: SparkContext, idxcfg: IdxCfg): 
                  (RDD[Array[Float]], 
                   Broadcast[Array[(Short, Array[Float])]])
```

```scala
/** convert paa to ranking-sensitive representation;
 *  output: Array[Short]: representation; */  
def Ops.cvtPaaTo4Ps_os(paa: Array[Float],
                   pivots: Array[(Short, Array[Float])],
                   prefix_length: Int): Array[Short]
```

```scala
/** retrieve centers for groups;
 *  output: Array[Short]: centers; */  
def Group.retrieve(sc: SparkContext,
            p4s_os_weight_sample: RDD[(String, Long)],
            idxcfg: IdxCfg): Array[(Int, Array[Short])]
```

```scala
/** split group into partitions;
 *  output: Int: partition number;
 *          Array[(Int, Trie)]: center_to_partition; */ 
def Group.split_groups(sc: SparkContext,
                 p4s_os_weight: RDD[(String, Long)],
                 centers: Array[(Int, Array[Short])],
                 idxcfg: IdxCfg): (Int, Array[(Int, Trie)])
```
```scala
/** trie construction; */ 
def Trie.construct(p4s_os_weight: Array[(String, Long)],
            block_cap: Int): Unit
```
```scala
/** shuffle all data series; */ 
def Index.shuffle_data_partition_mode(sc: SparkContext,
                                centers: Array[(Int, Array[Short])],
                                partition_num: Int,
                                center_2_partition: Array[(Int, Trie)],
                                idxcfg: IdxCfg): Unit
```

```scala
/** query one data series; 
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
 *  output: Array[Int]: partition ids; */ 
def Query.search_partition_mode(p4s_os: Array[Short], 
                          idxcfg: IdxCfg): Array[Int]
```

## Experimental
### Compilation
The code has been writen in Scala and compiled in `SBT`.\
Please update `scalaVersion` and `libraryDependencies` about `spark package` in build.sbt file of source code before compiling it.\
Make sure the compatibility of `spark packages`.

### Configure 

The configure file is `./etc/config.conf`.\
The log is under `./etc/log/`, and log name can be set by `logFileName`.\
Key parameters:
* mode: paratition or group;
* ts_length: data series dimension;
* paa_length: paa length;
* pivots_num: pivots number;
* permutation_prefix_length: length of permutation prefix;
* knn_k: k value for similarity query;


Other parameters for data series generator and ground truth generator
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
Please make sure that the code is used properly.\
If our research is useful for publication, please reference our paper.
