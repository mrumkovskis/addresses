package lv.addresses.indexer

trait SpatialIndexer { this: AddressFinder =>
  case class Node(code: Int, left: Node, right: Node)

  protected var _spatialIndex: Node = null

  def searchNearest(coordX: BigDecimal, coordY: BigDecimal, limit: Int = 1) = {

  }

  def spatialIndex(addressMap: Map[Int, AddrObj]) = {
    def kdtree(addresses: Seq[Int], depth: Int = 0): Node = addresses.headOption.map { _ =>
      val axis = depth % 2
      val sorted = addresses.sortWith { (c1, c2) =>
        val (a1, a2) = (addressMap(c1), addressMap(c2))
        if (axis == 0) a1.coordX <= a2.coordX else a1.coordY <= a2.coordY
      }
      val median = sorted.size / 2
      Node(
        sorted(median),
        kdtree(sorted.view(0, median), depth + 1),
        kdtree(sorted.view(median, sorted.size), depth + 1)
      )
    } getOrElse null
    print("Creating spatial index ... ")
    _spatialIndex = kdtree(addressMap
      .keysIterator
      .filter(c => addressMap
        .get(c)
        .exists(a => a.coordX != null && a.coordY != null))
      .toIndexedSeq)
    println("ok")
  }
}
