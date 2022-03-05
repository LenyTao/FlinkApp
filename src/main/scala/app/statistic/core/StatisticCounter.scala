package app.statistic.core

import app.statistic.model.PurchaseEvent
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class StatisticCounter(val amount: Int)
    extends RichFlatMapFunction[PurchaseEvent, String] {

  private var lastCounterState: ValueState[Double] = _
  private var eventListState: ListState[PurchaseEvent] = _

  override def open(parameters: Configuration): Unit = {
    val lastAmountDescriptor =
      new ValueStateDescriptor[Double]("lastAmount", classOf[Double])

    lastCounterState = getRuntimeContext.getState[Double](lastAmountDescriptor)

    val lastListEventDescriptor =
      new ListStateDescriptor[PurchaseEvent]("lastList", classOf[PurchaseEvent])

    eventListState =
      getRuntimeContext.getListState[PurchaseEvent](lastListEventDescriptor)
  }

  override def flatMap(reading: PurchaseEvent, out: Collector[String]): Unit = {

    eventListState.add(reading)

    val lastAmount = lastCounterState.value()

    val newAmount = lastAmount + 1

    if (newAmount == amount) {

      val maxAmountPurchase = eventListState.get().toList.maxBy(_.amount)

      val maxCategory = eventListState.get.toList
        .map(event => event.category)
        .foldLeft(Map.empty[String, Int]) { (acc, category) =>
          if (acc.contains(category)) {
            acc.updated(category, acc(category) + 1)
          } else {
            acc.+(category -> 0)
          }
        }
        .maxBy(_._2)

      out.collect(s"""
                     |
                     |Cейчас в стране ${reading.country}
                     |
                     |В основном покупают товары в категории: ${maxCategory._1}
                     |
                     |За последние $amount покупок, товары в этой категории были куплены ${maxCategory._2} раз
                     |
                     |Самая дорогая покупка была в категории ${maxAmountPurchase.category}
                     |На сумму:
                     |${maxAmountPurchase.amount}
                     |""".stripMargin)

      this.eventListState.clear()
      this.lastCounterState.clear()
    } else {
      this.lastCounterState.update(newAmount)
    }
  }
}
