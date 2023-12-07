

case class CategoryBehaviorReport(
    station: String,
    fog: Boolean,
    rain: Boolean,
    snow: Boolean,
    hail: Boolean,
    thunder: Boolean,
    tornado: Boolean) {
  def clear = !fog && !rain && !snow && !hail && !thunder && !tornado
}