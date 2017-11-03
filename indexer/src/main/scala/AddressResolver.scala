package lv.addresses.indexer

trait AddressResolver { this: AddressFinder =>
  /** Address format:
        <IEL> <NLT> - <DZI>, <CIEM>, <PAG>|<PIL>, <NOV>
        Examples:
          	Ancīši, Ancene, Asares pag., Aknīstes nov.
            Vīlandes iela 18 - 1, Rīga

  */
  def resolve(resolvable: String): ResolvedAddress = {
    import Constants._
    case class Basta(resolved: Option[Address]) extends Exception
    def to_str(addr: Address) = addr.address.toLowerCase.replace("\n", ", ").replace("\"", "")
    def all_words_match(str: String, addr: Address) =
      (str.split(SEPARATOR_REGEXP).filter(_ != "") zip addr.address.toLowerCase.split(SEPARATOR_REGEXP))
        .forall {case (s1, s2) => s1 == s2}
    def full_resolve(addressString: String): Option[Address] = search(addressString)(2) match {
      case Array(address) if to_str(address) startsWith addressString =>
        Some(address) //only one match take that if beginning matches
      case Array(address, _) if addressString == to_str(address) =>
        Some(address) //exact match
      case Array(a1, a2) if all_words_match(addressString, a1) && !all_words_match(addressString, a2) =>
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
          .foldRight(None: Option[Address]) { (a, resolvedAddr) =>
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
