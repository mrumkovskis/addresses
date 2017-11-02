package lv.addresses.indexer

trait AddressResolver { this: AddressFinder =>
  /** Address format:
        <IEL> <NLT> - <DZI>, <CIEM>, <PAG>|<PIL>, <NOV>
        Examples:
          	Ancīši, Ancene, Asares pag., Aknīstes nov.
            Vīlandes iela 18 - 1, Rīga

  */
  def resolve(resolvable: String): ResolvedAddress = {
    case class Basta(resolved: Option[Address]) extends Exception
    def full_resolve(addressString: String): Option[Address] = search(addressString)(2) match {
      case Array(address) if address.address.toLowerCase.replace("\n", ", ") startsWith addressString =>
        Some(address) //only one match take that if beginning matches
      case Array(address, _) if addressString == address.address.toLowerCase.replace("\n", ", ") =>
        Some(address) //exact match
      case Array(a1, a2) if (a1.address.toLowerCase.replace("\n", ", ") startsWith addressString)
          && !(a2.address.toLowerCase.replace("\n", ", ") startsWith addressString) =>
        Some(a1) //first match is better than second, so choose first
      case _ =>
        None
    }
    val addressString = resolvable.toLowerCase
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
