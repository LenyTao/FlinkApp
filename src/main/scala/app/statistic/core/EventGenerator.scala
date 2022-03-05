package app.statistic.core

import app.statistic.model.PurchaseEvent

import java.util.concurrent.ThreadLocalRandom

object EventGenerator extends App {

  def getLimitedRandomizer(start: Double, end: Double, typeOfData: String) = {
    val random: ThreadLocalRandom = ThreadLocalRandom.current()
    typeOfData match {
      case "Double" => random.nextDouble(start, end + 1)
      case "Int"    => random.nextInt(start.toInt, end.toInt + 1)
    }
  }

  def getCategory: String = {

    val categoryId = getLimitedRandomizer(1, 5, "Int")

    categoryId match {
      case 1 => "Products"
      case 2 => "Clothing"
      case 3 => "Technology"
      case 4 => "Entertainment"
      case 5 => "Transport"
    }
  }

  def getCountry: String = {

    val countryId = getLimitedRandomizer(1, 5, "Int")

    countryId match {
      case 1 => "Russia"
      case 2 => "China"
      case 3 => "USA"
      case 4 => "Japan"
      case 5 => "Canada"
    }
  }

  def getCategorySum(category: String): Double = {

    category match {
      case "Products"      => getLimitedRandomizer(0, 10000, "Double")
      case "Clothing"      => getLimitedRandomizer(0, 50000, "Double")
      case "Technology"    => getLimitedRandomizer(0, 100000, "Double")
      case "Entertainment" => getLimitedRandomizer(0, 10000, "Double")
      case "Transport"     => getLimitedRandomizer(0, 1000, "Double")
    }

  }

  def createPurchaseEvent = {
    val country = getCountry
    val category = getCategory
    val amount = getCategorySum(category)
    PurchaseEvent(country,category, amount)
  }

}
