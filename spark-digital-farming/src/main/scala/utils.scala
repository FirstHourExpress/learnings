package utils

import java.sql.Date
import java.time.{Duration, LocalDate}
import org.apache.spark._

object Utils {
  def getDateRange(
    startDate: LocalDate,
    endDate: LocalDate
  ): Seq[Date] = {
    val daysBetween = Duration
      .between(
        startDate.atStartOfDay(),
        endDate.atStartOfDay()
      )
      .toDays

    val newRows = Seq.newBuilder[Date]
    // get all intermediate dates
    for (day <- 0L to daysBetween) {
      val date = Date.valueOf(startDate.plusDays(day))
      newRows += date
    }
    newRows.result()
  }
}