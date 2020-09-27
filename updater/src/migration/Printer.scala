package lv.uniso.migration

object Printer {

  def msg(text: String) : Unit = {
    val fmt = new java.text.SimpleDateFormat("dd.MM.yyyy HH:mm:ss")
    System.err.println(s"${fmt.format(new java.util.Date)} $text")
  }

}

