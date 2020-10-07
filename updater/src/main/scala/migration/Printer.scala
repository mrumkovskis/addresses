package lv.addresses.migration

import org.slf4j.LoggerFactory

object Printer {

  val log = LoggerFactory.getLogger("lv.addresses.updater")

  def info(text: String): Unit = {
    // val fmt = new java.text.SimpleDateFormat("dd.MM.yyyy HH:mm:ss")
    // System.err.println(s"${fmt.format(new java.util.Date)} $text")
    log.info(text)
  }

  def debug(text: String): Unit = {
    log.debug(text)
  }

  def error(text: String): Unit = {
    log.error(text: String)
  }

}

