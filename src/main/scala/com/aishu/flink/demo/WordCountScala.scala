package com.aishu.flink.demo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object WordCountScala {
  def main(args: Array[String]): Unit = {
    // 1.初始化flink流处理的上下文环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 3.读取数据
    val stream = streamEnv.socketTextStream("127.0.0.1", 8888)

    // 4.数据转换和处理
    val result = stream.flatMap(_.split(""))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 5.结果打印
    result.print()

    // 6.stream处理的启动
    streamEnv.execute()
  }
}
