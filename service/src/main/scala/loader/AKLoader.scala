package lv.addresses.service.loader

import lv.addresses.index.Index.normalize
import lv.addresses.indexer.{AddrObj, Addresses}
import Loader.updateIsLeafFlag
import Loader._

import scala.jdk.CollectionConverters.EnumerationHasAsScala
import scala.util.Using

object AKLoader {
  def loadAddresses(addressZipFile: String, houseCoordFile: String, excludeList: Set[String]): Addresses = {
    def conv_pil_pag_nov(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(2), line(3).toInt, null,
      normalize(line(2)).toVector, null, null, line(12))
    def conv_cie_iel_raj(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(2), line(3).toInt, null,
      normalize(line(2)).toVector)
    def conv_nlt(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(7), line(5).toInt, line(9),
      normalize(line(7)).toVector)
    def conv_dziv(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(7), line(5).toInt, null,
      normalize(line(7)).toVector)

    val files: Map[String, (Array[String]) => AddrObj] =
      Map("AW_CIEMS.CSV" -> conv_cie_iel_raj _,
        "AW_DZIV.CSV" -> conv_dziv _,
        "AW_IELA.CSV" -> conv_cie_iel_raj _,
        "AW_NLIETA.CSV" -> conv_nlt _,
        "AW_NOVADS.CSV" -> conv_pil_pag_nov _,
        "AW_PAGASTS.CSV" -> conv_pil_pag_nov _,
        "AW_PILSETA.CSV" -> conv_pil_pag_nov _,
        "AW_RAJONS.CSV" -> conv_cie_iel_raj _).filter(t => !(excludeList contains t._1))

    logger.info(s"Loading addreses from file $addressZipFile, house coordinates from file $houseCoordFile...")
    var currentFile: String = null
    var converter: Array[String] => AddrObj = null
    val res = Using(new java.util.zip.ZipFile(addressZipFile)) { f =>
      val houseCoords = Option(houseCoordFile).flatMap(cf => Option(f.getEntry(cf)))
        .map{e =>
          logger.info(s"Loading house coordinates $houseCoordFile ...")
          scala.io.Source.fromInputStream(f.getInputStream(e))
        }.toList
        .flatMap(_.getLines().drop(1))
        .map{r =>
          val coords = r.split(";").map(_.drop(1).dropRight(1))
          coords(1).toInt -> (BigDecimal(coords(2)) -> BigDecimal(coords(3)))
        }.toMap
      val addressMap = f.entries.asScala
        .filter(files contains _.getName)
        .map(f => { logger.info(s"loading file: $f"); converter = files(f.getName); currentFile = f.getName; f })
        .map(f.getInputStream(_))
        .map(scala.io.Source.fromInputStream(_, "Cp1257"))
        .flatMap(_.getLines().drop(1))
        .filter(l => {
          val r = l.split(";")
          //use only existing addresses - status: EKS
          (if (Set("AW_NLIETA.CSV", "AW_DZIV.CSV") contains currentFile) r(2) else r(7)) == "#EKS#"
        })
        .map(r => converter(r.split(";").map(_.drop(1).dropRight(1))))
        .map(o => o.code -> houseCoords
          .get(o.code)
          .map(coords => o.copy(coordLat = coords._2).copy(coordLong = coords._1))
          .getOrElse(o))
        .toMap
      logger.info(s"${addressMap.size} addresses loaded.")
      addressMap
    }.get
    Addresses(updateIsLeafFlag(res), Map())
  }
}
