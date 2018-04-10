## Chapter 2: Structured APIs

### Structured APIs overview

The majority of the Structured APIs apply to both **batch and streaming** computation. 
This means that when you work with the Structured APIs, it should be simple to migrate from batch to streaming with little to no effort.

### DataFrame

All `DataFrame`s are actually `Dataset`s of type `Row`.

```scala
type DataFrame = Dataset[Row]
```

DataFrame是：

* 分布式的数据集
* 类似关系型数据库中的table，或者 excel 里的一张 sheet，或者 python/R 里的 dataframe
* 拥有丰富的操作函数，类似于 rdd 中的算子
* 一个 dataframe 可以被注册成一张数据表，然后用 sql 在上面操作
* 丰富的创建方式
    * 已有的RDD
    * 结构化数据文件
    * JSON数据集
    * Hive表
    * 外部数据库
    * kafka
    * ...

### Dataset: Type-Safe Structured APIs

类型安全的Structured APIs，只支持通过JVM上的语言( Scala、Java ...)调用。

`DataFrame`并不是没有类型的，它的类型是由 Spark 在**运行时**维护的。而`Dataset`的类型信息可以在**编译期**确定。
Spark 的数据集都是不可变的，这样对开发者很友好，但是这样用户代码会倾向于创建新的、临时的数据集，这样可能导致 GC、创建新对象的压力对于 JVM 来说过大。`DataFrame`的实现会避免这种问题，而如果使用`Dataset`则完全没有这种优化。

什么时候才会用`Dataset`：

* 使用`DataFrame`做不到的功能
* 希望类型安全并且可以接受牺牲性能

### SQL tables and views

基本上就是`DataFrame`。

### RDD: Not a Structured API

RDD: Resilient Distributed Datasets

**Virtually everything in Spark is built on top of RDDs.**

事实上`DataFrame`、`Dataset`都会编译为`RDD`。