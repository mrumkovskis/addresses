package lv.addresses.service.loader

import lv.addresses.indexer.{AddrObj, Addresses}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.jdk.CollectionConverters.EnumerationHasAsScala
import scala.util.{Failure, Success, Try, Using}

object OpenDataLoader {
  import Loader._
  def loadAddresses(addressFile: String, addressHistoryFile: String, historySince: LocalDate = null): Addresses = {
    import lv.addresses.index.Index.normalize
    val structure = Map(
      "AW_PILSETA.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "atvk" -> 12,
        "exists" -> 7,
      ),
      "AW_NOVADS.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "atvk" -> 12,
        "exists" -> 7,
      ),
      "AW_PAGASTS.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "atvk" -> 12,
        "exists" -> 7,
      ),
      "AW_CIEMS.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "exists" -> 7,
      ),
      "AW_IELA.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 2,
        "super_code" -> 3,
        "exists" -> 7,
      ),
      "AW_EKA.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 7,
        "super_code" -> 5,
        "zip_code" -> 9,
        "koord_x" -> 17,
        "koord_y" -> 18,
        "exists" -> 2,
      ),
      "AW_DZIV.CSV" -> Map(
        "code" -> 0,
        "typ" -> 1,
        "name" -> 7,
        "super_code" -> 5,
        "exists" -> 2,
      ),
      "AW_PILSETA_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
        "end_date" -> 4,
      ),
      "AW_NOVADS_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
        "end_date" -> 4,
      ),
      "AW_PAGASTS_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
        "end_date" -> 4,
      ),
      "AW_CIEMS_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
        "end_date" -> 4,
      ),
      "AW_IELA_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
        "end_date" -> 4,
      ),
      "AW_EKA_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 6,
        "end_date" -> 5,
      ),
      "AW_DZIV_HIS.CSV" -> Map(
        "code" -> 0,
        "name" -> 5,
        "end_date" -> 4,
      ),
    )

    val dateFormatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")

    def convertAddress(struct: Map[String, Int], line: Array[String]) = {
      def bd(s: String) = Try(BigDecimal(s)) match {
        case Success(value) => Option(value)
        case Failure(_) =>
          logger.warn(s"Cannot convert coordinate '$s' to decimal number in line ${line.mkString(";")}")
          None
      }
      if (line(struct("exists")) == "EKS")
        AddrObj(
          code      = line(struct("code")).toInt,
          typ       = line(struct("typ")).toInt,
          name      = line(struct("name")),
          superCode = line(struct("super_code")).toInt,
          zipCode   = struct.get("zip_code").map(line).orNull,
          words     = normalize(line(struct("name"))).toVector,
          coordX    = struct.get("koord_x").map(line).filter(_.nonEmpty).flatMap(bd).orNull,
          coordY    = struct.get("koord_y").map(line).filter(_.nonEmpty).flatMap(bd).orNull,
          atvk      = struct.get("atvk").map(line).orNull,
        )
      else null
    }

    def convertHistory(struct: Map[String, Int], line: Array[String]) = {
      def d(s: String) = Try(LocalDate.parse(s, dateFormatter)) match {
        case Success(value) => value
        case Failure(_) =>
          if (s.nonEmpty)
            logger.warn(s"Cannot parse address end date '$s' in line ${line.mkString(";")}")
          LocalDate.now()
      }
      if (historySince == null || historySince.isBefore(d(line(struct("end_date")))))
        line(struct("code")).toInt -> line(struct("name"))
      else null
    }

    def createAddresses(addressIt: Iterator[AddrObj]) =
      updateIsLeafFlag(addressIt.map(o => o.code -> o).toMap)

    def createHistory(historyIt: Iterator[(Int, String)]) = {
      val (m, c) =
        historyIt.foldLeft(Map[Int, List[String]]() -> 0) { case ((res, c), (code, name)) =>
          (res ++ Seq((code, name :: res.getOrElse(code, Nil))), c + 1)
        }
//        Mutable map does not seem to give any performance benefit over immutable map folding
//        historyIt.foldLeft(scala.collection.mutable.Map[Int, List[String]]() -> 0) { case ((res, c), (code, name)) =>
//          (res.addOne(code, name :: res.getOrElse(code, Nil)), c + 1)
//        }
//      val im = m.toMap
      val im = m
//      logger.debug(s"MM: ${im.size}, $c")
      im
    }

    def processZipFile[T, V](file: String,
                          lineFun: (Map[String, Int], Array[String]) => T,
                          resultTransformFun: Iterator[T] => V) =
      Using(new java.util.zip.ZipFile(file)) { af =>
        val it =
          af.entries().asScala
            .flatMap { zipEntry =>
              structure.get(zipEntry.getName).map { struct =>
                logger.info(s"loading file $zipEntry")
                Source.fromInputStream(af.getInputStream(zipEntry), "UTF-8")
                  .getLines()
                  .drop(1)  // drop header row with column names
                  .map { row =>
                    val l =
                      row
                        .split(";")
                        .map(_.drop(1).dropRight(1)) // remove enclosing # from values
                    lineFun(struct, l)
                  }
                  .filter(_ != null) // filter out non existing objects
              }.getOrElse(Iterator.empty)
            }
        resultTransformFun(it)
      }.get

    logger.info(s"Loading addresses from file $addressFile ...")
    val addrObjs =
      processZipFile(addressFile, convertAddress, createAddresses)

    logger.info(s"Loading addresses history from file $addressHistoryFile ...")
    val addrHistory =
      processZipFile(addressHistoryFile, convertHistory, createHistory)

    Addresses(addrObjs, addrHistory)
  }
}