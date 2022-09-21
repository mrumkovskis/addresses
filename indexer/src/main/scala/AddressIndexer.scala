package lv.addresses.indexer

import java.util.Properties

import scala.language.postfixOps
import scala.collection.mutable.{ArrayBuffer => AB}
import lv.addresses.index.Index._
import Constants._

trait AddressIndexer extends Indexer { this: AddressFinder =>

  def maxEditDistance(word: String): Int = {
    if (word.forall(_.isDigit)) 0 //no fuzzy search for words with digits in them
    else {
      WordLengthEditDistances.getOrElse(word.length - (word.indexOf('*') + 1), DefaultEditDistance)
    }
  }

  def index(addressMap: Map[Int, AddrObj],
            history: Map[Int, List[String]],
            synonyms: Properties,
            filter: AddrObj => Boolean): Index = {
    logger.info("Starting address indexing...")

    logger.info(s"Sorting ${addressMap.size} addresses...")

    //(addressCode, ordering weight, full space separated unnaccented address)
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
        }
      }
      addresses.sortWith { case ((_, ord1, a1), (_, ord2, a2)) =>
        ord1 < ord2 || (ord1 == ord2 && (a1.length < a2.length || (a1.length == a2.length && a1 < a2)))
      }
    }

    index(
      sortedAddresses
        .iterator
        .map {
          case (code, _, name) => (code, name :: history.getOrElse(code, Nil))
        },
      synonyms
    )
  }
}
