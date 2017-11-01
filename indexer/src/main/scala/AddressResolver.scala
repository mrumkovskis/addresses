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
    val addressString = resolvable.toLowerCase
    search(addressString)(2) match {
      case Array(address) if address.address.toLowerCase.replace("\n", ", ") startsWith addressString =>
        ResolvedAddress(resolvable, Some(address)) //only one match take that
      case Array(address, _) if addressString == address.address.toLowerCase.replace("\n", ", ") => //exact match
        ResolvedAddress(resolvable, Some(address))
      case Array(a1, a2) if (a1.address.toLowerCase.replace("\n", ", ") startsWith addressString)
        && !(a2.address.toLowerCase.replace("\n", ", ") startsWith addressString) => //first match is better than second, so choose first
        ResolvedAddress(resolvable, Some(a1))
      case _ => ResolvedAddress(
        resolvable,
        try addressString.split(",").map(_.trim).foldRight(Option[Address](null)) {
          case (a, resolvedAddr) =>
            val resolvable = a + resolvedAddr.map("\n" + _.address).mkString
            search(resolvable)(1) match {
              case Array(address) if resolvable == address.address.toLowerCase => Some(address)
              case _ => throw Basta(resolvedAddr)
            }
        } catch {
          case Basta(resolved) => resolved
        }
      )
    }
  }
}
