package lv.addresses.indexer

import lv.addresses.indexer.AddressFields._

trait AddressResolver { this: AddressFinder =>
  /** Address format:
        <IEL> <NLT> - <DZI>, <CIEM>, <PAG>|<PIL>, <NOV>
        Examples:
          	Ancīši, Ancene, Asares pag., Aknīstes nov.
            Vīlandes iela 18 - 1, Rīga

  */
  def resolve(resolvable: String,
              fields: Set[String] = Set(StructData, LksKoordData, HistoryData, AtvkData)): ResolvedAddress = {
    import Constants._
    case class Basta(resolved: Option[MutableAddress]) extends Exception
    def to_str(addr: MutableAddress) = addr.address.toLowerCase
      .replace("\n", ", ")
      .replace("\"", "")
    def all_words_match(str: String, addr: MutableAddress) =
      (str
        .split(SEPARATOR_REGEXP)
        .filter(_ != "") zip addr.address.toLowerCase.split(SEPARATOR_REGEXP).filter(_ != ""))
        .forall {case (s1, s2) => s1 == s2}
    def full_resolve(addressString: String): Option[MutableAddress] =
      search(addressString)(2, nonFilteredIndex, fields) match {
        case Array(address) if all_words_match(addressString, address) =>
          Some(address) //only one match take that if all words matches
        case Array(a1, a2) if addressString == to_str(a1) ||
            (all_words_match(addressString, a1) && !all_words_match(addressString, a2)) =>
          Some(a1) //first match is better than second, so choose first
        case _ =>
          None
      }
     val addressString = resolvable.toLowerCase.replace("\"", "")
    ResolvedAddress(
      resolvable,
      full_resolve(addressString).orElse {
        try addressString
          .split(",")
          .map(_.trim)
          .foldRight(None: Option[MutableAddress]) { (a, resolvedAddr) =>
            full_resolve(a + resolvedAddr
              .map(", " + _.address.replace("\n", ", "))
              .mkString
              .toLowerCase
            ).orElse(throw Basta(resolvedAddr))
          }
        catch {
          case Basta(resolved) => resolved
        }
      }
    )
  }
}
