package lv.addresses.indexer

trait SpatialIndexer { this: AddressFinder =>

  import scala.collection.mutable.SortedSet

  case class Node(code: Int, left: Node, right: Node)

  protected var _spatialIndex: Node = null

  def searchNearest(coordX: BigDecimal, coordY: BigDecimal, limit: Int = 1) = {
  }

  def fullScanSearchNearest(coordX: BigDecimal, coordY: BigDecimal, limit: Int = 1) = {
    val realLimit = Math.min(limit, 20)
    val nearest = SortedSet[(Int, BigDecimal)]()(new Ordering[(Int, BigDecimal)]  {
      def compare(a: (Int, BigDecimal), b: (Int, BigDecimal)) =
        if (a._2 < b._2) -1 else if (a._2 > b._2) 1 else 0
    })
    def dist(px: BigDecimal, py: BigDecimal, ax: BigDecimal, ay: BigDecimal) =
      (px - ax).pow(2) + (py - ay).pow(2)
    var cnt = 0
    addressMap.foreach { case (c, o) =>
      cnt += 1
      if (o.coordX != null && o.coordY != null) {
        nearest += (c -> dist(coordX, coordY, o.coordX, o.coordY))
        if (nearest.size > realLimit) nearest.lastOption.foreach(nearest -= _)
      }
    }
    nearest.toList
  }

  def spatialIndex(addressMap: Map[Int, AddrObj]) = {
    val start = System.currentTimeMillis
    var nodeCount = 0
    def kdtree(addresses: Seq[Int], depth: Int = 0): Node = addresses match {
      case Seq() => null
      case Seq(c) =>
        nodeCount += 1
        Node(c, null, null)
      case _ =>
        val axis = depth % 2
        val sorted = addresses.sortWith { (c1, c2) =>
          val (a1, a2) = (addressMap(c1), addressMap(c2))
          if (axis == 0) a1.coordX <= a2.coordX else a1.coordY <= a2.coordY
        }
        val median = sorted.size / 2
        nodeCount += 1
        Node(
          sorted(median),
          kdtree(sorted.view(0, median), depth + 1),
          kdtree(sorted.view(median, sorted.size), depth + 1)
        )
    }

    println("Creating spatial index ... ")
    _spatialIndex = kdtree(addressMap
      .keysIterator
      .filter(c => addressMap
        .get(c)
        .exists { a => a.coordX != null && a.coordY != null })
      .toIndexedSeq)
    println(s"Spatial index created ($nodeCount addresses indexed in ${System.currentTimeMillis - start}ms)")
  }
}
