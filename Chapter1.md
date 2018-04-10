## Chapter 1: Spark in a glance

### What is Spark & why Spark

**One Stack To Rule Them All**

Spark 是一个统一的(可以)在集群上并行数据处理的计算引擎，spark 可以运行在四种不同的模式下：standalone、local cluster、spark on yarn 和spark on mesos。 Spark有多种语言的 api binding(Scala, Java, Python, R). Spark 可以运行在笔记本和集群上面。Spark被设计为支持大多数的数据分析任务，通过支持流式处理（spark streaming）,机器学习（mllib）,实时查询（spark sql）,图计算（graphx）等各种大数据处理，构造了一个统一的数据处理引擎。统一计算引擎和一组一致的API也使得这些任务更容易和更高效地编写。首先，Spark提供了一致的，可组合的API，你可以使用这些API来构建应用程序，或使用较小的部分或从现有的库中构建应用程序。你也可以在上面编写自己的分析库。但是，可组合的API还不够：Spark的API也可以通过优化用户程序中组合在一起的不同库和函数来实现高性能。例如，如果你使用SQL查询加载数据，然后使用Spark的MLlib构建机器学习模型，则引擎可以将这些步骤合并为一次扫描数据。通用API和高性能执行的结合，无论你如何将它们组合在一起，都使得Spark成为开发和部署的强大平台。

#### MapReduce

MapReduce的缺陷与不足：

* 抽象层次低，需要手工编写代码来完成，使用上难以上手。
* 只提供两个操作，Map和Reduce，表达力欠缺。
* 一个Job只有Map和Reduce两个阶段（Phase），复杂的计算需要大量的Job完成，Job之间的依赖关系是由开发者自己管理的。
* 处理逻辑隐藏在代码细节中，没有整体逻辑
* 中间结果也放在HDFS文件系统中
* ReduceTask需要等待所有MapTask都完成后才可以开始
* 时延高，只适用Batch数据处理，对于交互式数据处理，实时数据处理的支持不够
* 对于迭代式数据处理性能比较差

Spark 的优点：

* 更高的抽象
* 可组合的 API 提供了相当的灵活性
* Spark提供了一个集群的分布式**内存**抽象，避免了每个处理数据的中间步骤都需要写到磁盘的问题。（同时通过 checkpoint、DAG、手动persist等方式保证数据不会丢失）Spark的批处理速度比MapReduce快近10倍，内存中的数据分析速度则快近100倍。
* 支持实时数据处理
* 惰性求值
* 为迭代式数据处理提供更好的支持（因为不用写到硬盘）

### The architecture of a Spark Application

![The architecture of a Spark Application](/images/spark-02.png)

* Application: Spark中的Application和 Hadoop MR中的概念是相似的，指的是用户编写的Spark应用程序。
* Driver: Spark 中的 Driver 即运行上述 Application 的 main() 函数并且创建 SparkSession，其中创建 SparkSession 的目的是为了准备 Spark 应用程序的运行环境。在 Spark 中由 SparkSession 负责和 Cluster Manager 通信，进行资源的申请、任务的分配和监控等；当 Executor 部分运行完毕后，Driver 负责将 SparkSession 关闭。
* Executors: Application 运行在 Worker 节点上的一个进程，该进程负责运行 Task，并且负责将数据存在内存或者磁盘上，每个 Application 都有各自独立的一批 Executor。
* Worker: 集群中任何可以运行 Application 代码的节点，类似于 YARN 的 NodeManager 节点。
* Cluster Manager: 指的是在集群上获取资源的外部服务（Hadoop Yarn: 由 YARN 中的 ResourceManager 负责资源的分配）

一个 job 的提交：

1. 构建Spark Application的运行环境，启动 SparkSession
2. SparkSession向资源管理器（Standalone，Mesos，Yarn）申请运行Executor资源，并启动StandaloneExecutorbackend，Executor向SparkSession申请Task
3. SparkSession将应用程序分发给Executor
4. SparkSession构建成DAG图，将DAG图分解成Stage，将TaskSet发送给Task Scheduler，最后由Task Scheduler将Task发送给Executor运行
5. Task在Executor上运行，运行完毕释放所有资源。

### Spark toolset

![Spark’s toolset](/images/spark-01.png)