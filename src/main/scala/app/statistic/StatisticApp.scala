package app.statistic

import app.statistic.core.{PurchaseSource, StatisticCounter}
import app.statistic.model.PurchaseEvent
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}

object StatisticApp extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val purchaseSource: DataStream[PurchaseEvent] =
    env.addSource(new PurchaseSource)

  val purchaseKeyStream = purchaseSource.keyBy(_.country)

  val statisticPurchaseStream =
    purchaseKeyStream.flatMap(new StatisticCounter(1000000))

  statisticPurchaseStream.print()

  env.execute()
}
