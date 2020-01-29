package org.dist.patterns.replicatedlog

import java.io.File

import org.dist.patterns.wal.Wal

class ReplicatedKVStore(walDir:File) {
  val replicatedWal = new ReplicatedWal(walDir)

}
