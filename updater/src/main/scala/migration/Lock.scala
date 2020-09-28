package lv.uniso.migration

import java.nio.channels._
import java.nio.file._

class Lock (file_name: String) {

  val fc: FileChannel = FileChannel.open(Paths.get(file_name), StandardOpenOption.CREATE, StandardOpenOption.APPEND)

  def acquire (): FileLock = {
    val lock = fc.tryLock(0, 1, false)
    if (lock == null) {
      Printer.msg(s"Another instance is already running (lockfile $file_name)")
      System.exit(42)
    }
    lock
  }

  def release (): Unit = {
    fc.close()
  }

  val lck: FileLock = acquire ()

}
