package lv.addresses.indexer

import java.util.Properties

import scala.language.postfixOps
import scala.collection.mutable.{ArrayBuffer => AB}

private object Constants {
  val PIL = 104
  val NOV = 113
  val PAG = 105
  val CIE = 106
  val IEL = 107
  val NLT = 108
  val DZI = 109

  val writtenOrder = Map (
    IEL -> 0,
    NLT -> 1,
    DZI -> 2,
    CIE -> 3,
    PIL -> 4,
    PAG -> 5,
    NOV -> 6
  )
  val typeOrderMap = Map[Int, Int](
    NOV -> 1, //novads
    PIL -> 2, //pilsēta
    PAG -> 3, //pagasts
    CIE -> 4, //ciems
    IEL -> 5, //iela
    NLT -> 6, //nekustama lieta (māja)
    DZI -> 7 //dzivoklis
  )
  val big_unit_types = Set(PIL, NOV, PAG, CIE)

  val SEPARATOR_REGEXP = """[\s-,/\."'\n]"""

  val WordLengthEditDistances = Map[Int, Int](
    0 -> 0,
    1 -> 0,
    2 -> 0,
    3 -> 1,
    4 -> 1,
    5 -> 1
  )
  val DefaultEditDistance = 2
}

trait AddressIndexer { this: AddressFinder =>
  import lv.addresses.index.Index._
  import Constants._
  case class AddrObj(code: Int, typ: Int, name: String, superCode: Int, zipCode: String,
      words: Vector[String], coordX: BigDecimal = null, coordY: BigDecimal = null,
      atvk: String = null) {
    def foldLeft[A](z: A)(o: (A, AddrObj) => A): A =
      addressMap.get(superCode).map(ao => ao.foldLeft(o(z, this))(o)).getOrElse(o(z, this))
    def foldRight[A](z: A)(o: (A, AddrObj) => A): A =
      addressMap.get(superCode).map(ao => ao.foldRight(z)(o)).map(o(_, this)).getOrElse(o(z, this))
    def depth = foldLeft(0)((d, _) => d + 1)
  }

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
          addresses.append((code, typeOrder(code, addr.typ) * 100 + addr.depth,
            addr.foldRight(AB[String]()){(b, o) => b += unaccent(o.name)}.mkString(" ")))
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
          wordsLists foreach(_ foreach { w =>
            val exactStr = if (w.contains("*")) w.substring(w.indexOf('*') + 1) else w
            index.updateChildren(w, idx, normalize(name).contains(exactStr))
            //update synonyms
            Option(synonyms.getProperty(w))
              .foreach(extractWords(_).foreach(index.updateChildren(_, idx, true)))
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
