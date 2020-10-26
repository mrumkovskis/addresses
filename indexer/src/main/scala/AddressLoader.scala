package lv.addresses.indexer

import java.sql.{Connection, DriverManager}

import org.tresql._

import scala.jdk.CollectionConverters._
import scala.util.Using

trait AddressLoader { this: AddressFinder =>

  def loadAddresses(addressZipFile: String = addressFileName, hcf: String = houseCoordFile) = {
    def conv(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(2), line(3).toInt, null,
      normalize(line(2)).toVector)
    def conv_nlt(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(7), line(5).toInt, line(9),
      normalize(line(7)).toVector)
    def conv_dziv(line: Array[String]) = AddrObj(line(0).toInt, line(1).toInt, line(7), line(5).toInt, null,
      normalize(line(7)).toVector)

    val files: Map[String, (Array[String]) => AddrObj] =
      Map("AW_CIEMS.CSV" -> conv _,
        "AW_DZIV.CSV" -> conv_dziv _,
        "AW_IELA.CSV" -> conv _,
        "AW_NLIETA.CSV" -> conv_nlt _,
        "AW_NOVADS.CSV" -> conv _,
        "AW_PAGASTS.CSV" -> conv _,
        "AW_PILSETA.CSV" -> conv _,
        "AW_RAJONS.CSV" -> conv _).filter(t => !(blackList contains t._1))

    logger.info(s"Loading addreses from file $addressZipFile, house coordinates from file $houseCoordFile...")
    var currentFile: String = null
    var converter: Array[String] => AddrObj = null
    Using(new java.util.zip.ZipFile(addressZipFile)) { f =>
      val houseCoords = Option(hcf).flatMap(cf => Option(f.getEntry(cf)))
        .map{e =>
          logger.info(s"Loading house coordinates $hcf ...")
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
          .map(coords => o.copy(coordX = coords._1).copy(coordY = coords._2))
          .getOrElse(o))
        .toMap
      logger.info(s"${addressMap.size} addresses loaded.")
      addressMap
    }.get
  }

  /** Returns from database actual and historical addresses. */
  def loadAddressesFromDb(conf: DbConfig): (Map[Int, AddrObj], Map[Int, List[String]]) = {
    import conf._
    Class.forName(driver)
    Using(DriverManager.getConnection(url, user, password)) { conn =>
      implicit val res = tresqlResources withConn conn

      logger.info(s"Loading house coordinates")
      val houseCoords =
        Query("art_eka_geo {vieta_cd, koord_x, koord_y}")
          .map(row => row.int("vieta_cd") ->
            (row.bigdecimal("koord_x") -> row.bigdecimal("koord_y")))
          .toMap
      logger.info(s"House coordinates loaded: ${houseCoords.size} objects")

      logger.info("Loading address objects")
      val queries = List(
        "art_vieta [statuss = 'EKS' & tips_cd in ?] {kods, tips_cd, vkur_cd, nosaukums, null zip_code}",
        "art_nlieta [statuss = 'EKS' & tips_cd in ?] {kods, tips_cd, vkur_cd, nosaukums, atrib zip_code}",
        "art_dziv [statuss = 'EKS' & tips_cd in ?] {kods, tips_cd, vkur_cd, nosaukums, null zip_code}",
      )

      val addressObjs: Map[Int, AddrObj] =
        queries
          .map(Query(_, Constants.typeOrderMap.keys))
          .map {
            _.map { row =>
              val kods = row.long("kods").intValue()
              val (koordX, koordY) = houseCoords.getOrElse(kods, (null, null))
              val name = row.string("nosaukums")
              val obj = AddrObj(kods, row.long("tips_cd").intValue, name,
                row.long("vkur_cd").intValue, row.string("zip_code"),
                normalize(name).toVector, koordX, koordY
              )
              kods -> obj
            }
          }
          .reduce(_ ++ _)
          .toMap
      logger.info(s"Addresses loaded: ${addressObjs.size} objects")

      (addressObjs, loadAddressHistoryFromDb(conn))
    }.get
  }

  def loadAddressHistoryFromDb(conn: Connection)(implicit resources: Resources): Map[Int, List[String]] = {
    logger.info("Loading address history")
    val history =
      Query("arg_adrese_arh {adr_cd, string_agg(std, ?)} (adr_cd)", "\n")(resources withConn conn)
        .map(row => row.int(0) -> row.string(1).split("\n").toList.distinct)
        .toMap
    logger.info(s"Address history loaded: ${history.size} objects")
    history
  }
}
