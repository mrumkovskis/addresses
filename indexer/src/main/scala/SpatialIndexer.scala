package lv.addresses.indexer

trait SpatialIndexer { this: AddressFinder =>

  import scala.collection.mutable.SortedSet

  case class Node(code: Int, left: Node, right: Node)

  protected var _spatialIndex: Node = null

  class Search(val limit: Int) {
    private val nearest = SortedSet[(AddrObj, BigDecimal)]()(new Ordering[(AddrObj, BigDecimal)]  {
      def compare(a: (AddrObj, BigDecimal), b: (AddrObj, BigDecimal)) =
        if (a._2 < b._2) -1 else if (a._2 > b._2) 1 else 0
    })
    private def dist(px: BigDecimal, py: BigDecimal, ax: BigDecimal, ay: BigDecimal) =
      (px - ax).pow(2) + (py - ay).pow(2)

    def searchNearest(coordLat: BigDecimal, coordLong: BigDecimal) = {
      val found = scala.collection.mutable.Set[AddrObj]()
      def search(node: Node, depth: Int = 0): AddrObj = if (node == null) null else {
        import node._
        val curr_addr = addressMap(code)
        def closest(addr: AddrObj, new_addr: AddrObj) =
          if (new_addr == null || found(new_addr)) if (found(addr)) null else addr
          else if (addr == null || found(addr)) new_addr
          else if (dist(coordLat, coordLong, addr.coordLat, addr.coordLong) <
              dist(coordLat, coordLong, new_addr.coordLat, new_addr.coordLong)) addr
          else new_addr
        def check_x_splitting_pane(addr: AddrObj) = addr == null ||
          dist(coordLat, coordLong, addr.coordLat, addr.coordLong) >= dist(coordLat, 0, curr_addr.coordLat, 0)
        def check_y_splitting_pane(addr: AddrObj) = addr == null ||
          dist(coordLat, coordLong, addr.coordLat, addr.coordLong) >= dist(0, coordLong, 0, curr_addr.coordLong)
        def traverse(left: Node, right: Node, check_splitting_pane_cross: AddrObj => Boolean,
            start_with_left: Boolean) = {
          val (first, second) = if (start_with_left) (left, right) else (right, left)
          val curr_best_addr = closest(curr_addr, search(first, depth + 1))
          if (check_splitting_pane_cross(curr_best_addr)) closest(curr_best_addr, search(second, depth + 1))
          else curr_best_addr
        }
        if (depth % 2 == 0) traverse(left, right, check_x_splitting_pane, coordLat <= curr_addr.coordLat) //x axis
        else traverse(left, right, check_y_splitting_pane, coordLong <= curr_addr.coordLong) //y axis
      }
      nearest.clear()
      1 to limit map { _ =>
        val nearest_addr = search(_spatialIndex)
        found += nearest_addr
        nearest_addr
      } foreach (a => nearest += (a -> dist(coordLat, coordLong, a.coordLat, a.coordLong)))
      nearest.toList
    }

    def searchNearestFullScan(coordLat: BigDecimal, coordLong: BigDecimal) = {
      nearest.clear()
      addressMap.foreach { case (c, o) =>
        if (o.coordLat != null && o.coordLong != null) {
          nearest += (o -> dist(coordLat, coordLong, o.coordLat, o.coordLong))
          if (nearest.size > limit) nearest.lastOption.foreach(nearest -= _)
        }
      }
      nearest.toList
    }
  }

  def spatialIndex(addressMap: Map[Int, AddrObj]) = {
    var nodeCount = 0
    def kdtree(addresses: Seq[Int], depth: Int = 0): Node = addresses match {
      case Seq() => null
      case Seq(c) =>
        nodeCount += 1
        Node(c, null, null)
      case _ =>
        val axis = depth % 2
        val sorted = addresses.sortBy { c =>
          val a = addressMap(c)
          if (axis == 0) a.coordLat else a.coordLong
        }
        val median = sorted.size / 2
        nodeCount += 1
        Node(
          sorted(median),
          kdtree(sorted.slice(0, median), depth + 1),
          kdtree(sorted.slice(median, sorted.size), depth + 1)
        )
    }

    logger.info("Creating spatial index ... ")
    _spatialIndex = kdtree(addressMap
      .keysIterator
      .filter(c => addressMap
        .get(c)
        .exists { a => a.coordLat != null && a.coordLong != null })
      .toIndexedSeq)
    logger.info(s"Spatial index created ($nodeCount addresses indexed).")
  }
}
