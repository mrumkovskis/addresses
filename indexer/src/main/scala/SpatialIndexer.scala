package lv.addresses.indexer

trait SpatialIndexer { this: AddressFinder =>
  case class Node(code: Int, left: Node, right: Node)

  protected var _spatialIndex: Node = null

  def spatialIndex(addressMap: Map[Int, AddrObj]) = {
  }
}
