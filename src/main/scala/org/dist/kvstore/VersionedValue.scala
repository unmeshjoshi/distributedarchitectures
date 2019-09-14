package org.dist.kvstore

case class VersionedValue(value:String, version:Int = VersionGenerator.getNextVersion) {}
