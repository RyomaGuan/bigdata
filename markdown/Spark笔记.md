# Spark 运行模式
## 集群角色

<img src="Spark笔记.assets/image-20210801232553978.png" alt="image-20210801232553978" style="zoom:50%;" />

### Master 和 Worker
#### Master
Spark 特有资源调度系统的 Leader。掌管着整个集群的资源信息，类似于 Yarn 框架中的 ResourceManager，主要功能：
1. 监听 Worker，看 Worker 是否正常工作
2. Master 对 Worker、Application 等的管理(接收 Worker 的注册并管理所有的Worker，接收 Client 提交的 Application，调度等待的 Application 并向Worker 提交)

#### Worker
Spark 特有资源调度系统的 Slave，有多个。每个 Slave 掌管着所在节点的资源信息，类似于 Yarn 框架中的 NodeManager，主要功能：
1.	通过 RegisterWorker 注册到 Master；
2.	定时发送心跳给 Master；
3.	根据 Master 发送的 Application 配置进程环境，并启动 ExecutorBackend(执行 Task 所需的临时进程)

### Driver和Executo
#### Driver（驱动器）
Spark 的驱动器是执行开发程序中的 main 方法的线程。它负责开发人员编写的用来创建SparkContext、创建RDD，以及进行RDD的转化操作和行动操作代码的执行。如果你是用Spark Shell，那么当你启动Spark shell的时候，系统后台自启了一个Spark驱动器程序，就是在Spark shell中预加载的一个叫作 sc 的SparkContext对象。如果驱动器程序终止，那么Spark应用也就结束了。主要负责：
1. 将用户程序转化为作业（Job）
2. 在Executor之间调度任务（Task）
3. 跟踪Executor的执行情况
4. 通过UI展示查询运行情况。
#### Executor（执行器）
Spark Executor是一个工作节点，负责在 Spark 作业中运行任务，任务间相互独立。Spark 应用启动时，Executor 节点被同时启动，并且始终伴随着整个 Spark 应用的生命周期而存在。如果有Executor节点发生了故障或崩溃，Spark 应用也可以继续执行，会将出错节点上的任务调度到其他Executor节点上继续运行。主要负责：
1.	负责运行组成 Spark 应用的任务，并将状态信息返回给驱动器程序
2.	通过自身的块管理器（Block Manager）为用户程序中要求缓存的RDD提供内存式存储。RDD是直接缓存在Executor内的，因此任务可以在运行时充分利用缓存数据加速运算

总结：Master 和 Worker 是 Spark 的守护进程，即 Spark 在特定模式下正常运行所必须的进程。Driver 和 Executor 是临时程序，当有具体任务提交到 Spark 集群才会开启的程序。

## 案例实操

<img src="Spark笔记.assets/image-20210801232616590.png" alt="image-20210801232616590" style="zoom:50%;" />

Spark Shell 仅在测试和验证我们的程序时使用的较多，在生产环境中，通常会在 IDE 中编制程序，然后打成 jar 包，然后提交到集群，最常用的是创建一个 Maven 项目，利用 Maven 来管理 jar 包的依赖。

# RDD
RDD（Resilient Distributed Dataset）叫做弹性分布式数据集，是Spark中最基本的数据（计算）抽象。适合并行计算的最小计算单元 ，可以理解为逻辑的封装。在代码中是一个抽象类，它代表一个弹性的、不可变、可分区、里面的元素可并行计算的集合。

<img src="Spark笔记.assets/image-20210801232931286.png" alt="image-20210801232931286" style="zoom:50%;" />

## RDD 的 5 个主要属性(property)
1. 多个分区，分区可以看成是数据集的基本组成单位
	对于 RDD 来说, 每个分区都会被一个计算任务处理, 并决定了并行计算的粒度
	用户可以在创建 RDD 时指定 RDD 的分区数, 如果没有指定, 那么就会采用默认值。默认值就是程序所分配到的 CPU Coure 的数目
	每个分配的存储是由BlockManager 实现的。每个分区都会被逻辑映射成 BlockManager 的一个 Block, 而这个 Block 会被一个 Task 负责计算

2. 计算每个切片(分区)的函数
	Spark 中 RDD 的计算是以分片为单位的, 每个 RDD 都会实现 compute 函数以达到这个目的
3. 与其他 RDD 之间的依赖关系
	RDD 的每次转换都会生成一个新的 RDD, 所以 RDD 之间会形成类似于流水线一样的前后依赖关系。在部分分区数据丢失时, Spark 可以通过这个依赖关系重新计算丢失的分区数据, 而不是对 RDD 的所有分区进行重新计算
4. 对存储键值对的 RDD, 还有一个可选的分区器
	只有对于 key-value的 RDD, 才会有 Partitioner, 非key-value的 RDD 的 Partitioner 的值是 None。Partitiner 不但决定了 RDD 的本区数量, 也决定了 parent RDD Shuffle 输出时的分区数量
5. 存储每个切片优先(preferred location)位置的列表
	比如对于一个 HDFS 文件来说, 这个列表保存的就是每个 Partition 所在文件块的位置。按照“移动数据不如移动计算”的理念, Spark 在进行任务调度的时候, 会尽可能地将计算任务分配到其所要处理数据块的存储位置

## 理解 RDD
一个 RDD 可以简单的理解为一个分布式的元素集合。RDD 表示只读的分区的数据集，对 RDD 进行改动，只能通过 RDD 的转换操作，然后得到新的 RDD并不会对原 RDD 有任何的影响

在 Spark 中，所有的工作要么是创建 RDD，要么是转换已经存在 RDD 成为新的 RDD，要么在 RDD 上去执行一些操作来得到一些计算结果。每个 RDD 被切分成多个分区(partition)，每个分区可能会在集群中不同的节点上进行计算。

### RDD 特点
#### 弹性
1. 存储的弹性：内存与磁盘的自动切换；
2. 容错的弹性：数据丢失可以自动恢复；
3. 计算的弹性：计算出错重试机制；
4. 分片的弹性：可根据需要重新分片。
#### 分区
RDD 逻辑上是分区的，每个分区的数据是抽象存在的，计算的时候会通过一个compute函数得到每个分区的数据。

如果 RDD 是通过已有的文件系统构建，则compute函数是读取指定文件系统中的数据，如果 RDD 是通过其他 RDD 转换而来，则 compute函数是执行转换逻辑将其他 RDD 的数据进行转换。

#### 只读
RDD 是只读的，要想改变 RDD 中的数据，只能在现有 RDD 基础上创建新的 RDD。由一个 RDD 转换到另一个 RDD，可以通过丰富的转换算子实现，不再像 MapReduce 那样只能写map和reduce了。RDD 的操作算子包括两类：
1. 一类叫做transformation，它是用来将 RDD 进行转化，构建 RDD 的血缘关系；
2. 另一类叫做action，它是用来触发 RDD 进行计算，得到 RDD 的相关计算结果或者 保存 RDD 数据到文件系统中。

#### 依赖(血缘)
RDDs 通过操作算子进行转换，转换得到的新 RDD 包含了从其他 RDDs 衍生所必需的信息，RDDs 之间维护着这种血缘关系，也称之为依赖。依赖包括两种：
1. 一种是窄依赖，RDDs 之间分区是一一对应的
2. 另一种是宽依赖，下游 RDD 的每个分区与上游 RDD(也称之为父RDD)的每个分区都有关，是多对多的关系

#### 缓存
如果在应用程序中多次使用同一个 RDD，可以将该 RDD 缓存起来，该 RDD 只有在第一次计算的时候会根据血缘关系得到分区的数据，在后续其他地方用到该 RDD 的时候，会直接从缓存处取而不用再根据血缘关系计算，这样就加速后期的重用。

#### checkpoint
虽然 RDD 的血缘关系天然地可以实现容错，当 RDD 的某个分区数据计算失败或丢失，可以通过血缘关系重建。但是对于长时间迭代型应用来说，随着迭代的进行，RDDs 之间的血缘关系会越来越长，一旦在后续迭代过程中出错，则需要通过非常长的血缘关系去重建，势必影响性能。

为此，RDD 支持checkpoint 将数据保存到持久化的存储中，这样就可以切断之前的血缘关系，因为checkpoint 后的 RDD 不需要知道它的父 RDDs 了，它可以从 checkpoint 处拿到数据。

## RDD 编程
在 Spark 中，RDD 被表示为对象，通过对象上的方法调用来对 RDD 进行转换。经过一系列的transformations定义 RDD 之后，就可以调用 actions 触发 RDD 的计算。action可以是向应用程序返回结果(count, collect等)，或者是向存储系统保存数据(saveAsTextFile等)。

在Spark中，只有遇到action，才会执行 RDD 的计算(即延迟计算)，这样在运行时可以通过管道的方式传输多个转换。要使用 Spark，开发者需要编写一个 Driver 程序，它被提交到集群以调度运行 Worker。Driver 中定义了一个或多个 RDD，并调用 RDD 上的 action，Worker 则执行 RDD 分区计算任务。

算子外部的代码在Driver端执行，算子内部的代码在Executor端执行。在 RDD 上支持 2 种操作:
1.	transformation
	从一个已知的 RDD 中创建出来一个新的 RDD 例如: map就是一个transformation
2.	action
	在数据集上计算结束之后, 给驱动程序返回一个值. 例如: reduce就是一个action

在 Spark 中几乎所有的transformation操作都是懒执行的(lazy), 也就是说transformation操作并不会立即计算他们的结果, 而是记住了这个操作。只有当通过一个action来获取结果返回给驱动程序的时候这些转换操作才开始计算。这种设计可以使 Spark 运行起来更加的高效。默认情况下, 你每次在一个 RDD 上运行一个action的时候, 前面的每个transformed RDD 都会被重新计算。但是我们可以通过persist (or cache)方法来持久化一个 RDD 在内存中, 也可以持久化到磁盘上, 来加快访问速度。

## RDD 的创建
### 从集合中创建 RDD
```scala
val rdd1 = sc.parallelize(Array(10,20,30,40,50,60))
val rdd2 = sc.makeRDD(Array(10,20,30,40,50,60))
```
makeRDD 底层代码调用 parallelize。说明:
1. 一旦 RDD 创建成功, 就可以通过并行的方式去操作这个分布式的数据集了
2. parallelize 和 makeRDD  还有一个重要的参数就是把数据集切分成的分区数。在创建 RDD 的同时，可以指定分区的数量，其实就是设定第二个参数。保存文件时，会按照默认的分区数量进行保存
3. Spark 会为每个分区运行一个任务(task)。正常情况下, Spark 会自动的根据你的集群来设置分区数

### 从外部存储创建 RDD
Spark 也可以从任意 Hadoop 支持的存储数据源来创建分布式数据集，可以是本地文件系统、HDFS、Cassandra、HVase, Amazon S3 等等。Spark 支持 文本文件、SequenceFiles、和其他所有的 Hadoop InputFormat。

1. 读取文件的分区规则是以文件为单位
2. 读取文件的规则是以行为单位
```scala
var rdd = sc.textFile("words.txt")
```
说明：
1. url可以是本地文件系统文件，hdfs://...、s3n://...等等
2. 如果是使用的本地文件系统的路径，则必须每个节点都要存在这个路径
3. 所有基于文件的方法，都支持目录，压缩文件，和通配符(*)。例如: textFile("/my/directory")，textFile("/my/directory/*.txt")，and textFile("/my/directory/*.gz")
4. textFile还可以有第二个参数，表示分区数。默认情况下，每个块对应一个分区.(对 HDFS 来说，块大小默认是 128M)。可以传递一个大于块数的分区数，但是不能传递一个比块数小的分区数

## RDD 的转换(transformation)
根据 RDD 中数据类型的不同, 整体分为 2 种 RDD：
1. Value类型
2. Key-Value类型(其实就是存一个二维的元组)

### Value 类型
####  map
返回一个新的 RDD，该 RDD 是由原 RDD 的每个元素经过函数转换后的值而组成
```scala
val rdd1 = sc.parallelize(1 to 10)
val rdd2 = rdd1.map(_ * 2)
```

<img src="Spark笔记.assets/image-20210802015903527.png" alt="image-20210802015903527" style="zoom:50%;" />

#### mapPartitions
类似于 map(func)，但是是独立在每个分区上运行。func 的类型是```Iterator<T> => Iterator<U>```。假设有N个元素，有M个分区，那么 map 的函数的将被调用N次，而 mapPartitions 被调用M次，一个函数一次处理所有分区。
```scala
rdd.mapPartitions(it => it.map(_ * 2))
```
以分区单位进行逻辑运算，但是运算过程中，数据不会被释放掉，所以容易产生内存溢出。在这种场合下，一般采用map算子，保证程序执行通过。

<img src="Spark笔记.assets/image-20210802022036952.png" alt="image-20210802022036952" style="zoom:50%;" />

#### mapPartitionsWithIndex
和 mapPartitions(func) 类似，但是会给 func 多提供一个 Int 值来表示分区的索引。func 的类型是```(Int, Iterator<T>) => Iterator<U>```
```scala
val rdd1 = sc.parallelize(Array(10,20,30,40,50,60), 2)
// Array((0,10), (0,20), (0,30), (1,40), (1,50), (1,60))
rdd1.mapPartitionsWithIndex((idx, items) => items.map((idx, _))).collect
```

#### flatMap
类似于map，但是每一个输入元素可以被映射为 0 或多个输出元素。所以func应该返回一个序列，而不是单一元素```（T => TraversableOnce[U]）```
```scala
// 创建一个元素为 1-5 的RDD，运用 flatMap创建一个新的 RDD，新的 RDD 为原 RDD 每个元素的 平方和三次方 来组成 1,1,4,8,9,27..
val rdd1 = sc.parallelize(Array(1,2,3,4,5))
// Array(1, 1, 4, 8, 9, 27, 16, 64, 25, 125)
rdd1.flatMap(x => Array(x * x, x * x * x)).collect
```

#### glom
将每一个分区的元素合并成一个数组，形成新的 RDD 类型是 RDD[Array[T]]
```scala
// 创建一个 4 个分区的 RDD，并将每个分区的数据放到一个数组
var rdd1 = sc.parallelize(Array(10,20,30,40,50,60), 4)
// Array(Array(10), Array(20, 30), Array(40), Array(50, 60))
rdd1.glom.collect
```

#### groupBy
按照func的返回值进行分组。func返回值作为 key，对应的值放入一个迭代器中。返回```RDD: RDD[(K, Iterable[T])```。每组内元素的顺序不能保证，并且甚至每次调用得到的顺序也有可能不同。

#### filter
过滤。返回一个新的 RDD 是由 func 的返回值为 true 的那些元素组成。

#### sample(withReplacement, fraction, seed)
1.	以指定的随机种子随机抽样出比例为fraction的数据，(抽取到的数量是: size * fraction)。需要注意的是得到的结果并不能保证准确的比例
2. withReplacement 表示是抽出的数据是否放回，true 为有放回的抽样，false 为无放回的抽样。放回表示数据有可能会被重复抽取到, false 则不可能重复抽取到，如果是 false, 则 fraction 必须是:[0,1；是 true 则大于等于0就可以了
3.	seed 用于指定随机数生成器种子，一般用默认或者传入当前的时间戳

#### distinct([numPartitions]))
对 RDD 中元素执行去重操作。参数表示任务的数量，默认值和分区数保持一致

<img src="Spark笔记.assets/image-20210802034104902.png" alt="image-20210802034104902" style="zoom:50%;" />

#### coalesce(numPartitions)
缩减分区数到指定的数量，用于大数据集过滤后，提高小数据集的执行效率。第二个参数表示是否 shuffle，如果不传或者传入的为 false, 则表示不进行 shuffer, 此时分区数减少有效, 增加分区数无效
```scala
val rdd1 = sc.parallelize(0 to 100, 5)
// 5
rdd1.partitions.length
val rdd2 = rdd1.coalesce(2)
// 2
rdd2.partitions.length
```

#### repartition(numPartitions)
根据新的分区数，重新 shuffle 所有的数据，这个操作总会通过网络。新的分区数相比以前可以多，也可以少。
1. coalesce重新分区，可以选择是否进行 shuffle 过程。由参数 shuffle: Boolean = false/true 决定
2. repartition实际上是调用的 coalesce，进行 shuffle。源码如下
```scala
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
  coalesce(numPartitions, shuffle = true)
}
```
3. 如果是减少分区, 尽量避免 shuffle

#### sortBy(func,[ascending], [numTasks])
使用 func  先对数据进行处理，按照处理后结果排序，默认为正序。
```scala
val rdd = sc.parallelize(Array(1,3,4,10,4,6,9,20,30,16))
// Array(1, 3, 4, 4, 6, 9, 10, 16, 20, 30)
rdd.sortBy(x => x).collect
```

#### pipe(command, [envVars])
管道针对每个分区，把 RDD 中的每个数据通过管道传递给shell命令或脚本，返回输出的RDD。一个分区执行一次这个命令，每个元素算是标准输入中的一行。如果只有一个分区, 则执行一次命令。脚本要放在 worker 节点可以访问到的位置。


### 双 Value 类型交互
这里的“双 Value 类型交互”是指的两个  RDD[V] 进行交互。
#### union(otherDataset)
求并集，对源 RDD 和参数 RDD 求并集后返回一个新的 RDD。union 和 ++ 是等价的

####  subtract (otherDataset)
计算差集，从原 RDD 中减去 原 RDD 和 otherDataset 中的共同的部分。


#### intersection(otherDataset)
计算交集，对源 RDD 和参数 RDD 求交集后返回一个新的 RDD。

#### zip(otherDataset)
拉链操作。在 Spark 中, 两个 RDD 的元素的数量和分区数都必须相同否则会抛出异常（ scala 中，两个集合的长度可以不同），其实本质就是要求的每个分区的元素的数量相同。

####  cartesian(otherDataset)
计算 2 个 RDD 的笛卡尔积，尽量避免使用。


### Key-Value 类型
大多数的 Spark 操作可以用在任意类型的 RDD 上，但是有一些比较特殊的操作只能用在 key-value 类型的 RDD 上。这些特殊操作大多都涉及到 shuffle 操作。比如: 按照 key 分组(group)、聚集(aggregate)等。

在 Spark 中，这些操作在包含对偶类型(Tuple2)的 RDD 上自动可用(通过隐式转换)。
```scala
object RDD {
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }
```
键值对的操作是定义在 PairRDDFunctions 类上，这个类是对 RDD[(K, V)] 的装饰。
#### partitionBy
对 pairRDD 进行分区操作，如果原有的 partionRDD 的分区器和传入的分区器相同, 则返回原 pairRDD，否则会生成 ShuffleRDD，即会产生 shuffle 过程。

####  reduceByKey(func, [numTasks])
在一个 (K,V) 的 RDD 上调用，返回一个 (K,V) 的 RDD，使用指定的 reduce 函数，将相同 key 的 value 聚合到一起，reduce 任务的个数可以通过第二个可选的参数来设置。
```scala
val rdd = sc.makeRDD(List(("a", 1), ("a", 1), ("b", 2), ("b", 2), ("b", 2)))
// Array(("a",2), ("b",6))
rdd1.reduceByKey(_ + _).collect
```
<img src="Spark笔记.assets/image-20210802052942558.png" alt="image-20210802052942558" style="zoom:50%;" />

#### groupByKey
按照key进行分组。groupByKey 必须在内存中持有所有的键值对。如果一个key有太多的value，则会导致内存溢出(OutOfMemoryError)。所以这操作非常耗资源，如果分组的目的是为了在每个key上执行聚合操作(比如: sum 和 average)，则应该使用 PairRDDFunctions.aggregateByKey 或者 PairRDDFunctions.reduceByKey，因为他们有更好的性能(会先在分区进行预聚合)。
1. reduceByKey：按照key进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是 RDD[(K, V)]
2. groupByKey：按照key进行分组，直接进行 shuffle

#### aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])
使用给定的 combine 函数和一个初始化的 zero value，对每个 key 的 value 进行聚合。
```scala
// zeroValue：给每一个分区中的每一个key一个初始值
// seqOp：函数用于在每一个分区中用初始值逐步迭代 value
// combOp：函数用于合并每个分区中的结果
def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                              combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
}
```
```scala
val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
// Array((b,3), (a,3), (c,12))
rdd.aggregateByKey(Int.MinValue)(math.max(_, _), _ +_).collect
```
<img src="Spark笔记.assets/image-20210802055156724.png" alt="image-20210802055156724" style="zoom:75%;" />

#### foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
aggregateByKey 的简化操作，seqop 和 combop 相同
```scala
val rdd = sc.parallelize(Array(("a",3), ("a",2), ("c",4), ("b",3), ("c",6), ("c",8)))
// Array((b,3), (a,5), (c,18))
rdd.foldByKey(0)(_ + _).collect
```

####  combineByKey[C]
针对每个 K, 将 V 进行合并成C, 得到 RDD[(K,C)]
```scala
//	createCombiner: combineByKey 会遍历分区中的每个 key-value 对。如果第一次碰到这个 key 则调用 createCombiner 函数，传入value，得到一个 C 类型的值(如果不是第一次碰到这个 key，则不会调用这个方法)
//	mergeValue: 如果不是第一个遇到这个key, 则调用这个函数进行合并操作。分区内合并
//	mergeCombiners 跨分区合并相同的key的值(C)。跨分区合并
def combineByKey[C](
                       createCombiner: V => C,
                       mergeValue: (C, V) => C,
                       mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
        partitioner, mapSideCombine, serializer)(null)
}
```

```scala
// 创建一个 pairRDD，根据 key 计算每种 key 的value的平均值
  val rdd = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
  // acc 累加器, 用来记录分区内的值的和这个 key 出现的次数
  // acc1, acc2 跨分区的累加器
  val combineByKeyRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
    (_, 1), // 88 => (88, 1)
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // (88, 1) + 91 => (179, 2)
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )
  // Array((b,95), (a,91))
  combineByKeyRDD.map {
    case (key, (sum, size)) => (key, sum / size)
  }.collect
}
```
<img src="Spark笔记.assets/image-20210802154436188.png" alt="image-20210802154436188" style="zoom:75%;" />

#### sortByKey
在一个 (K,V) 的 RDD 上调用, K必须实现 Ordered[K] 接口(或者有一个隐式值: Ordering[K]), 返回一个按照 key 进行排序的 (K,V)  的 RDD。

#### mapValues
针对 (K,V) 形式的类型只对 V 进行操作。


#### join(otherDataset, [numTasks])
内连接。在类型为 (K,V) 和 (K,W) 的 RDD 上调用，返回一个相同 key 对应的所有元素对在一起的```(K,(V,W))```的RDD。
1. 如果某一个 RDD 有重复的 Key，则会分别与另外一个 RDD 的相同的 Key进行组合
2. 也支持外连接：leftOuterJoin、rightOuterJoin、fullOuterJoin

####  cogroup(otherDataset, [numTasks])
在类型为 (K,V)和 (K,W) 的 RDD 上调用，返回一个```(K,(Iterable<V>,Iterable<W>))```类型的 RDD
```scala
val rdd1 = sc.parallelize(Array((1, 10), (2, 20), (1, 100), (3, 30)))
val rdd2 = sc.parallelize(Array((1, "a"),(2, "b"),(1, "aa"),(3, "c")))
Array((1,(CompactBuffer(10, 100),CompactBuffer(a, aa))), (3,(CompactBuffer(30),CompactBuffer(c))), (2,(CompactBuffer(20),CompactBuffer(b))))
rdd1.cogroup(rdd2).collect
```

#### 案例实操
数据结构：时间戳、省份、城市、用户、广告字段使用空格分割

需求: 统计出每一个省份广告被点击次数的 TOP3
```scala
val lineRDD = sc.textFile("input/agent.log")
lineRDD
  .map(line => {
    val splits = line.split(" ")
    ((splits(1), splits(4)), 1)
  })
  .reduceByKey(_ + _)
  .map {
    case ((city, id), cnt) => (city, (id, cnt))
  }
  .groupByKey
  .mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
  .sortBy(_._1) // 按照省份的升序
  .collect
  .foreach(println)
```

## RDD的 Action 操作
所谓 的行动算子 ，就是用于提交Job（ActiveJob对象）。
### reduce
通过 func 函数聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据。
```scala
val rdd = sc.parallelize(1 to 100)
val sum: Int = rdd.reduce(_ + _)
```

### collect
以数组的形式返回 RDD 中的所有元素，所有的数据都会被拉到 driver 端, 所以要慎用。

### count
返回 RDD 中元素的个数。

###  take(n)
返回 RDD 中前 n 个元素组成的数组，take 的数据也会拉到 driver 端, 应该只对小数据集使用。

### first
返回 RDD 中的第一个元素，类似于take(1)。

### takeOrdered(n, [ordering])
返回排序后的前 n 个元素, 默认是升序排列，数据也会拉到 driver 端。

### aggregate
aggregate 函数将每个分区里面的元素通过 seqOp 和初始值 (zeroValue) 进行聚合，然后用 combine 函数将每个分区的结果和初始值 (zeroValue) 进行 combine 操作。这个函数最终返回的类型不需要和RDD中元素类型一致。初始值 (zeroValue) 分区内聚合和分区间聚合的时候各会使用一次。
```scala
def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
```

```scala
val rdd = sc.parallelize(1 to 4, 2)
// 40
rdd.aggregate(10)(_ + _, _ + _)
```

### fold
折叠操作，aggregate 的简化操作，seqop 和 combop 一样的时候,可以使用 fold。

### saveAsTextFile(path)
将数据集的元素以 textfile 的形式保存到 HDFS 文件系统或者其他支持的文件系统，对于每个元素，Spark 将会调用 toString 方法，将它转换为文件中的文本。

### saveAsSequenceFile(path)
针对 (K,V) 类型的 RDD，将数据集中的元素以 Hadoop sequencefile 的格式保存到指定的目录下，可以使 HDFS 或者其他 Hadoop 支持的文件系统。

### saveAsObjectFile(path)
用于将 RDD 中的元素序列化成对象，存储到文件中。

### countByKey
针对 (K,V) 类型的 RDD，返回一个 (K,Int) 的map，表示每一个 key 对应的元素个数。注意于 countByValue 区分。可以用来查看数据是否倾斜。
```scala
val rdd1 = sc.parallelize(Array(("a", 10), ("a", 20), ("b", 100), ("c", 200)))
// Map(b -> 1, a -> 2, c -> 1)
rdd1.countByKey
val rdd2 = sc.parallelize(Array("a", "a", "b"))
// Map(b -> 1, a -> 2)
rdd1.countByValue
```

### foreach
针对 RDD 中的每个元素都执行一次func，每个函数是在 Executor 上执行的, 不是在 driver 端执行的。


##  RDD 中函数的传递
我们进行 Spark 进行编程的时候，初始化工作是在 driver 端完成的，而实际的运行程序是在executor端进行的。所以就涉及到了进程间的通讯, 数据是需要序列化的。


## RDD 的依赖关系


## Spark 中的 Job 调度


## Spark Job 的划分



# Spark SQL
## Saprk SQL 简介
在老的版本中，SparkSQL 提供两种 SQL 查询起始点：一个叫 SQLContext，用于 Spark 自己提供的 SQL 查询；一个叫 HiveContext，用于连接 Hive 的查询。从2。0开始，SparkSession是 Spark 最新的 SQL 查询起始点，实质上是SQLContext和HiveContext的组合，所以在 SQLContext 和 HiveContext 上可用的 API 在 SparkSession 上同样是可以使用的。SparkSession 内部封装了SparkContext，所以计算实际上是由 SparkContext 完成的。

Spark SQL 是 Spark 用于结构化数据(structured data)处理的 Spark 模块。与基本的 Spark RDD API 不同，Spark SQL 的抽象数据类型为 Spark 提供了关于数据结构和正在执行的计算的更多信息。在内部，Spark SQL 使用这些额外的信息去做一些额外的优化。

有多种方式与 Spark SQL 进行交互，比如: SQL 和 Dataset API。 当计算结果的时候，使用的是相同的执行引擎，不依赖你正在使用哪种 API 或者语言。这种统一也就意味着开发者可以很容易在不同的 API 之间进行切换，这些 API 提供了最自然的方式来表达给定的转换。

Hive 是将 Hive SQL 转换成 MapReduce 然后提交到集群上执行，大大简化了编写 MapReduc 的程序的复杂性，由于 MapReduce 这种计算模型执行效率比较慢。所以 Spark SQL 的应运而生，它是将 Spark SQL 转换成 RDD，然后提交到集群执行，执行效率非常快！

Spark SQL 它提供了2个编程抽象，类似 Spark Core 中的 RDD
1. DataFrame
2. DataSet

## DataFrame
与 RDD 类似，DataFrame 也是一个分布式数据容器。然而 DataFrame 更像传统数据库的二维表格，除了数据以外，还记录数据的结构信息，即schema。同时，与Hive类似，DataFrame也支持嵌套数据类型（struct、array和map）。从 API 易用性的角度上看，DataFrame API提供的是一套高层的关系操作，比函数式的 RDD API 要更加友好，门槛更低。

DataFrame 是为数据提供了 Schema 的视图。可以把它当做数据库中的一张表来对待，DataFrame也是懒执行的。性能上比 RDD要高，主要原因： 优化的执行计划：查询计划通过Spark catalyst optimiser进行优化。简而言之，逻辑查询计划优化就是一个利用基于关系代数的等价变换，将高成本的操作替换为低成本操作的过程。

### 使用 DataFrame 进行编程
Spark SQL 的 DataFrame API 允许我们使用 DataFrame 而不用必须去注册临时表或者生成 SQL 表达式。DataFrame API 既有 transformation操作也有action操作。 DataFrame的转换从本质上来说更具有关系，而 DataSet API 提供了更加函数式的 API。
#### SQL 语法风格(主要)
SQL 语法风格是指我们查询数据的时候使用 SQL 语句来查询。这种风格的查询必须要有临时视图或者全局视图来辅助。
1. 临时视图只能在当前 Session 有效, 在新的 Session 中无效
2. 可以创建全局视图。访问全局视图需要全路径：如global_temp.xxx

#### DSL 语法风格(了解)
DataFrame 提供一个特定领域语言(domain-specific language，DSL)去管理结构化的数据。可以在 Scala、Java、Python 和 R 中使用 DSL 使用 DSL 语法风格不必去创建临时视图了。设计到运算的时候, 每列都必须使用```$```。

## DataSet
1. 是DataFrame API的一个扩展，是 SparkSQL 最新的数据抽象(1.6新增)。
2. 用户友好的API风格，既具有类型安全检查也具有 DataFrame 的查询优化特性。
3. Dataset 支持编解码器，当需要访问非堆上的数据时可以避免反序列化整个对象，提高了效率。
4. 样例类被用来在 DataSet 中定义数据的结构信息，样例类中每个属性的名称直接映射到DataSet中的字段名称。
5. DataFrame 是 DataSet 的特列```DataFrame=DataSet[Row]``` ，所以可以通过as方法将DataFrame转换为 DataSet。Row 是一个类型，跟 Car、Person这些的类型一样，所有的表结构信息都用 Row 来表示。
6. DataSet 是强类型的。比如可以有 DataSet[Car]，DataSet[Person]。
7. DataFrame 只是知道字段，但是不知道字段的类型，所以在执行这些操作的时候是没办法在编译的时候检查是否类型失败的，比如你可以对一个 String 进行减法操作，在执行的时候才报错，而 DataSet 不仅仅知道字段，而且知道字段类型，所以有更严格的错误检查。就跟 JSON 对象和类对象之间的类比。

##  RDD、DataFrame 和 DataSet 之间的关系
涉及到RDD，DataFrame，DataSet之间的操作时，需要导入```import spark.implicits._``` 这里的 spark 不是包名，而是表示SparkSession 的那个对象。所以必须先创建 SparkSession 对象再导入。implicits是一个内部 object。
### 三者的共性
1.	RDD、DataFrame、Dataset全都是 Spark 平台下的分布式弹性数据集，为处理超大型数据提供便利
2.	三者都有惰性机制，在进行创建、转换，如map方法时，不会立即执行，只有在遇到Action如foreach时，三者才会开始遍历运算。
3.	三者都会根据 Spark 的内存情况自动缓存运算，这样即使数据量很大，也不用担心会内存溢出
4.	三者都有 partition 的概念
5.	三者有许多共同的函数，如map, filter，排序等
6.	DataFrame和Dataset均可使用模式匹配获取各个字段的值和类型

### 三者的区别
1. RDD 一般和 spark mlib 同时使用
2. RDD 不支持 sparksql 操作
3. DataFrame 与 DataSet 均支持 SparkSQL 的操作，比如 select，groupby 之类，还能注册临时表/视窗，进行 sql 语句操作
4. Dataset 和 DataFrame 拥有完全相同的成员函数，区别只是每一行的数据类型不同。DataFrame 其实就是 DataSet 的一个特例。DataFrame 也可以叫```Dataset[Row]```,每一行的类型是 Row，不解析，每一行究竟有哪些字段，各个字段又是什么类型都无从得知。而 Dataset 中，每一行是什么类型是不一定的，在自定义了case class之后可以很自由的获得每一行的信息。
5. DataFrame 与 DataSet 支持一些特别方便的保存方式，比如保存成csv，可以带上表头，这样每一列的字段名一目了然
<img src="Spark笔记.assets/image-20210804004020701.png" alt="image-20210804004020701" style="zoom:75%;" />

## 自定义 SparkSQL 函数
### 自定义 UDF 函数
可以通过spark.udf功能用户可以自定义函数。
```scala
// 创建 DF
val df = spark.read.json("input/user.json")

// 注册一个 udf 函数: toUpper是函数名, 第二个参数是函数的具体实现
spark.udf.register("toUpper", (s: String) => s.toUpperCase)

df.createOrReplaceTempView("user")
spark.sql("select toUpper(username), age from user").show
```

### 用户自定会聚合函数
强类型的 Dataset 和弱类型的 DataFrame 都提供了相关的聚合函数， 如 count()，countDistinct()，avg()，max()，min()。除此之外，用户可以设定自己的自定义聚合函数。需要继承```UserDefinedAggregateFunction```。
#### DataFrame
```scala
// 用户自定会聚合函数：用户的平均年龄
class MyAvg extends UserDefinedAggregateFunction {
  // 聚合函数输入参数的数据结构
  override def inputSchema: StructType = {
    StructType(StructField("age", LongType) :: Nil)
  }

  // 聚合缓冲区中值的数据结构
  override def bufferSchema: StructType = {
    StructType(StructField("total", LongType) :: StructField("count", LongType) :: Nil)
  }

  // 聚合函数的结构类型
  override def dataType: DataType = DoubleType

  // 确定性: 同样的输入是否返回同样的输出（幂等性）
  override def deterministic: Boolean = true

  // 聚合函数初始化（缓存区初始化）
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 相同 Executor间的合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 不同 Executor 间的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
```
```scala
// 创建 DF
val userDf = spark.read.json("input/user.json")

// 注册自定义函数
spark.udf.register("myAvg", new MyAvg)

userDf.createOrReplaceTempView("user")
spark.sql("select myAvg(age) from user").show
```

#### DataSet
```scala
// DataSet 样例类
case class Emp(username: String, age: Long)

// BUF 样例类
case class AvgBuffer(var total: Long, var count: Long)

// 用户自定会聚合函数(强类型)：用户的平均年龄
// 继承 Aggregator，增加泛型
// abstract class Aggregator[-IN, BUF, OUT] extends Serializable{}
class MyAvg extends Aggregator[Emp, AvgBuffer, Double] {
  // 缓存区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L, 0L)
  }

  // 相同 Executor间的合并
  override def reduce(buffer: AvgBuffer, emp: Emp): AvgBuffer = {
    buffer.total += emp.age
    buffer.count += 1L
    buffer
  }

  // 不同 Executor 间的合并
  override def merge(buffer1: AvgBuffer, buffer2: AvgBuffer): AvgBuffer = {
    buffer1.total += buffer2.total
    buffer1.count += buffer2.count
    buffer1
  }

  // 计算最终的结果
  override def finish(buffer: AvgBuffer): Double = {
    buffer.total.toDouble / buffer.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
```
```scala
// 创建 DS
val userDf = spark.read.json("input/user.json").as[Emp]

// 创建聚合函数
val udaf = new MyAvg
// 将聚合函数转换为查询列
val column: TypedColumn[Emp, Double] = udaf.toColumn

// 使用 DSL 语法
userDS.select(column).show
```

## SparkSql 项目实战
















