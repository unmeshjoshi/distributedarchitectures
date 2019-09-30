package org.dist.kvstore

import java.io.File
import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.{Collections, StringTokenizer}

import scala.util.control.Breaks

class ColumnFamilyStore(table:String, columnFamily:String, dataFileDirectories:List[String]) {
  /*
           * Get all data files associated with old Memtables for this table.
           * These files are named as follows <Table>-1.db, ..., <Table>-n.db. Get
           * the max which in this case is n and increment it to use it for next
           * index.
           */
  val indices: util.List[Integer] = new util.ArrayList[Integer]
  for (directory <- dataFileDirectories.toList) {
    val fileDir: File = new File(directory)
    val files: Array[File] = fileDir.listFiles
    for (file <- files) {
      val filename: String = file.getName
      val tblCfName: Array[String] = getTableAndColumnFamilyName(filename)
      if (tblCfName(0) == table && tblCfName(1) == columnFamily) {
        val index: Int = getIndexFromFileName(filename)
        indices.add(index)
      }
    }
  }
  Collections.sort(indices)
  val value: Int = if (indices.size > 0) indices.get(indices.size - 1)
  else 0
  fileIndexGenerator_.set(value)
  memtable = new AtomicReference[Memtable](new Memtable(table, columnFamily))

  private def getTableAndColumnFamilyName(filename: String) = {
    val st = new StringTokenizer(filename, "-")
    val values = new Array[String](2)
    var i = 0
    while ( {
      st.hasMoreElements
    }) Breaks.breakable {
      if (i == 0) values(i) = st.nextElement.asInstanceOf[String]
      else if (i == 1) {
        values(i) = st.nextElement.asInstanceOf[String]
        Breaks.break() //todo: break is not supported
      }
      i += 1
    }
    values
  }

  /* This is used to generate the next index for a SSTable */
  private val fileIndexGenerator_ = new AtomicInteger(0)

  private[kvstore] def getNextFileName = { // Psuedo increment so that we do not generate consecutive numbers
    fileIndexGenerator_.incrementAndGet
    table + "-" + columnFamily + "-" + fileIndexGenerator_.incrementAndGet
  }

  private var memtable: AtomicReference[Memtable] = null
  private val ssTables_ = new util.HashSet[String]
  /* Modification lock used for protecting reads from compactions. */
  private val lock_ = new ReentrantReadWriteLock(true)


  import scala.util.control.Breaks
  import scala.util.control.Breaks.breakable

  protected def getIndexFromFileName(filename: String): Int = {
    /*
            * File name is of the form <table>-<column family>-<index>-Data.db.
            * This tokenizer will strip the .db portion.
            */ val st = new StringTokenizer(filename, "-")
    /*
             * Now I want to get the index portion of the filename. We accumulate
             * the indices and then sort them to get the max index.
             */ val count = st.countTokens
    var i = 0
    var index: String = null
    while ( {
      st.hasMoreElements
    }) breakable {
      index = st.nextElement.asInstanceOf[String]
      if (i == (count - 2)) {
        Breaks.break() //todo: break is not supported}
        i += 1
      }
    }

    index.toInt
  }
}
