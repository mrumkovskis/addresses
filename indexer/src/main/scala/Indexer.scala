package lv.addresses.indexer

import com.typesafe.scalalogging.Logger
import lv.addresses.index.Index._
import scala.collection.mutable.{ArrayBuffer => AB}


import java.util.Properties

case class Index(idxCode: scala.collection.mutable.HashMap[Int, Int],
                 index: MutableIndex)

trait Indexer {
  protected val logger: Logger

  def index(data: Iterator[(Int, Iterable[String])], synonyms: Properties): Index = {
    logger.info("Creating index...")
    val index = new MutableIndex(null, null)
    val idx_code = scala.collection.mutable.HashMap[Int, Int]()
    var idx = 0

    data.foreach {
      case (code, strings) =>
        strings.map(s => extractWords(s) -> normalize(s)).foreach { case (words, exact_words) =>
          words.foreach { w =>
            val (wcPrefix, exactStr) =
              if (w.contains("*")) {
                val i = w.indexOf('*') + 1
                (w.substring(0, i), w.substring(i))
              } else ("", w)
            index.updateChildren(w, idx, exact_words.contains(exactStr))
            //update synonyms
            Option(synonyms.getProperty(exactStr))
              .foreach(normalize(_)
                .foreach(extractWords(_)
                  .foreach { syn =>
                    index.updateChildren(wcPrefix + syn, idx, true)
                  }
                )
              )
          }
          idx_code += (idx -> code)
          idx += 1
          if (idx % 5000 == 0) logger.info(s"Strings processed: $idx")
        }
    }
    logger.info(s"Total objects processed: $idx; index statistics: ${index.statistics}")
    Index(idx_code, index)
  }

  /** Node word must be of one character length if it does not contains multiplier '*'.
   * Returns tuple - (path, word, first code) */
  def checkInvalidWords(idx: MutableIndex): AB[(String, String, Int)] = idx.checkInvalidWords

  /** Codes in node must be unique and in ascending order.
   * Returns (invalid path|word, codes) */
  def checkInvalidIndices(idx: MutableIndex): AB[(String, AB[Int])] = idx.checkInvalidIndices
}
