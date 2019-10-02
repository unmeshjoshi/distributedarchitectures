package org.dist.queue.common

import org.apache.log4j.Logger
import org.dist.queue.utils.Utils

trait Logging {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  protected var logIdent = ""

  private def msgWithLogIdent(msg: String) = logIdent + msg

  def trace(msg: => String): Unit = {
    if (logger.isTraceEnabled())
      logger.trace(msgWithLogIdent(msg))
  }
  def trace(e: => Throwable): Any = {
    if (logger.isTraceEnabled())
      logger.trace(logIdent,e)
  }
  def trace(msg: => String, e: => Throwable) = {
    if (logger.isTraceEnabled())
      logger.trace(msgWithLogIdent(msg),e)
  }
  def swallowTrace(action: => Unit) {
    Utils.swallow(logger.trace, action)
  }

  def debug(msg: => String): Unit = {
    if (logger.isDebugEnabled())
      logger.debug(msgWithLogIdent(msg))
  }
  def debug(e: => Throwable): Any = {
    if (logger.isDebugEnabled())
      logger.debug(logIdent,e)
  }
  def debug(msg: => String, e: => Throwable) = {
    if (logger.isDebugEnabled())
      logger.debug(msgWithLogIdent(msg),e)
  }
  def swallowDebug(action: => Unit) {
    Utils.swallow(logger.debug, action)
  }

  def info(msg: => String): Unit = {
    if (logger.isInfoEnabled())
      logger.info(msgWithLogIdent(msg))
  }
  def info(e: => Throwable): Any = {
    if (logger.isInfoEnabled())
      logger.info(logIdent,e)
  }
  def info(msg: => String,e: => Throwable) = {
    if (logger.isInfoEnabled())
      logger.info(msgWithLogIdent(msg),e)
  }
  def swallowInfo(action: => Unit) {
    Utils.swallow(logger.info, action)
  }

  def warn(msg: => String): Unit = {
    logger.warn(msgWithLogIdent(msg))
  }
  def warn(e: => Throwable): Any = {
    logger.warn(logIdent,e)
  }
  def warn(msg: => String, e: => Throwable) = {
    logger.warn(msgWithLogIdent(msg),e)
  }
  def swallowWarn(action: => Unit) {
    Utils.swallow(logger.warn, action)
  }
  def swallow(action: => Unit) = swallowWarn(action)

  def error(msg: => String): Unit = {
    logger.error(msgWithLogIdent(msg))
  }
  def error(e: => Throwable): Any = {
    logger.error(logIdent,e)
  }
  def error(msg: => String, e: => Throwable) = {
    logger.error(msgWithLogIdent(msg),e)
  }
  def swallowError(action: => Unit) {
    Utils.swallow(logger.error, action)
  }

  def fatal(msg: => String): Unit = {
    logger.fatal(msgWithLogIdent(msg))
  }
  def fatal(e: => Throwable): Any = {
    logger.fatal(logIdent,e)
  }
  def fatal(msg: => String, e: => Throwable) = {
    logger.fatal(msgWithLogIdent(msg),e)
  }
}
