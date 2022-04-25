package lv.addresses.indexer

import java.util.Properties

import scala.language.postfixOps
import scala.collection.mutable.{ArrayBuffer => AB}

trait AddressIndexer { this: AddressFinder =>
  import lv.addresses.index.Index._
  import Constants._

  def maxEditDistance(word: String): Int = {
    if (word.forall(_.isDigit)) 0 //no fuzzy search for words with digits in them
    else {
      WordLengthEditDistances.getOrElse(word.length - (word.indexOf('*') + 1), DefaultEditDistance)
    }
  }

  case class Index(idxCode: scala.collection.mutable.HashMap[Int, Int],
                   index: MutableIndex)

  def index(addressMap: Map[Int, AddrObj],
            history: Map[Int, List[String]],
            synonyms: Properties,
            filter: AddrObj => Boolean): Index = {
    logger.info("Starting address indexing...")

    logger.info(s"Sorting ${addressMap.size} addresses...")

    val index = new MutableIndex(null, null)

    //(addressCode, ordering weight, full space separated unnaccented address)
    var idx = 0
    val sortedAddresses = {
      def typeOrder(code: Int, typ: Int) = typ match {
        case PIL if !addressMap.contains(addressMap(code).superCode) => 0 // top level cities
        case _ => typeOrderMap(typ)
      }
      val addresses = AB[(Int, Int, String)]()
      addressMap.foreach { case (code, addr) =>
        if (filter == null || filter(addr)) {
          addresses.append((code, typeOrder(code, addr.typ) * 100 + addr.depth(addressMap),
            addr.foldRight(addressMap)(AB[String]()){(b, o) => b += unaccent(o.name)}.mkString(" ")))
          idx += 1
        }
      }
      addresses.sortWith { case ((_, ord1, a1), (_, ord2, a2)) =>
        ord1 < ord2 || (ord1 == ord2 && (a1.length < a2.length || (a1.length == a2.length && a1 < a2)))
      }
    }

    logger.info("Creating index...")
    val idx_code = scala.collection.mutable.HashMap[Int, Int]()
    idx = 0
    sortedAddresses
      .foreach {
        case (code, _, name) =>
          val wordsLists = extractWords(name) ::
            history.getOrElse(code, Nil).map(extractWords)
          val normalizedWords = normalize(name)
          wordsLists foreach(_ foreach { w =>
            val (wcPrefix, exactStr) =
              if (w.contains("*")) {
                val i = w.indexOf('*') + 1
                (w.substring(0, i), w.substring(i))
              } else ("", w)
            index.updateChildren(w, idx, normalizedWords.contains(exactStr))
            //update synonyms
            Option(synonyms.getProperty(exactStr))
              .foreach(normalize(_)
                .foreach(extractWords(_)
                  .foreach { syn =>
                    index.updateChildren(wcPrefix + syn, idx, true)
                  }
                )
              )
          })
          idx_code += (idx -> code)
          idx += 1
          if (idx % 5000 == 0) logger.info(s"Addresses processed: $idx")
      }

    logger.info(s"Address objects processed: $idx; index statistics: ${index.statistics}")
    Index(idx_code, index)
  }

  /** Node word must be of one character length if it does not contains multiplier '*'.
   * Returns tuple - (path, word, first address code) */
  def invalidWords(idx: MutableIndex): AB[(String, String, Int)] = idx.invalidWords

  /** Address codes in node must be unique and in ascending order.
   * Returns (invalid path|word, address codes) */
  def invalidIndices(idx: MutableIndex): AB[(String, AB[Int])] = idx.invalidIndices
}
