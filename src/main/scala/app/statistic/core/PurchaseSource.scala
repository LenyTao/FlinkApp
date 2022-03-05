package app.statistic.core

import app.statistic.model.PurchaseEvent
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class PurchaseSource extends RichParallelSourceFunction[PurchaseEvent] {

  override def run(sourceContext: SourceFunction.SourceContext[PurchaseEvent]): Unit = {

    while (true) {
      sourceContext.collect {
        EventGenerator.createPurchaseEvent
      }
    }
  }

  override def cancel(): Unit = {}

}
