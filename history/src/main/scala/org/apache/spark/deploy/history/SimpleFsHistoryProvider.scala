/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import java.util.zip.{ZipEntry, ZipOutputStream}
import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.xml.Node

import com.google.common.io.ByteStreams
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.status.{AppStatusStore, AppStatusListener, AppHistoryServerPlugin, ElementTrackingStore}
import org.apache.spark.status.config._
import org.apache.spark.status.api.v1.ApplicationInfo
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.kvstore.{KVStore, InMemoryStore}
import org.apache.spark.util.{Clock, SystemClock, Utils}


/**
 * A class that provides application history from event logs stored in the file system.
 * This provider will render the history application UI of specifically requested
 * applications only.  No scanning or caching of event logs from a filesystem is performed.
 *
 */

private[history] class SimpleFsHistoryProvider(conf: SparkConf, clock: Clock)
  extends ApplicationHistoryProvider with Logging {

  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }
  
  import SimpleFsHistoryProvider._
  
  type ReplayEventsFilter = (String) => Boolean
  val SELECT_ALL_FILTER: ReplayEventsFilter = { (eventString: String) => true }

  private val logDir = conf.getOption("spark.history.fs.logDirectory")
    .getOrElse(DEFAULT_LOG_DIR)

  private val HISTORY_UI_ACLS_ENABLE = conf.getBoolean("spark.history.ui.acls.enable", false)
  private val HISTORY_UI_ADMIN_ACLS = conf.get("spark.history.ui.admin.acls", "")
  private val HISTORY_UI_ADMIN_ACLS_GROUPS = conf.get("spark.history.ui.admin.acls.groups", "")
  logInfo(s"History server ui acls " + (if (HISTORY_UI_ACLS_ENABLE) "enabled" else "disabled") +
    "; users with admin permissions: " + HISTORY_UI_ADMIN_ACLS.toString +
    "; groups with admin permissions" + HISTORY_UI_ADMIN_ACLS_GROUPS.toString)

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fs = new Path(logDir).getFileSystem(hadoopConf)

  private def initialize(): Unit = {}
  
  override def getEventLogsUnderProcess(): Int = 0

  override def getLastUpdatedTime(): Long = 0
  
  override def getListing(): Iterator[ApplicationInfo] = { List().iterator }
  
  override def onUIDetached(appId: String, attemptId: Option[String], ui: SparkUI): Unit = { }
  
  def getApplicationInfoWrapper(appId: String): Option[ApplicationInfoWrapper] = {
    val eventsFilter: ReplayEventsFilter = { eventString =>
      eventString.startsWith(APPL_START_EVENT_PREFIX) ||
      eventString.startsWith(APPL_END_EVENT_PREFIX)
    }
    val fileStatus = fs.getFileStatus(new Path(logDir, appId))
    val bus = new ReplayListenerBus()
    val listener = new AppListingListener(fileStatus, clock)
    bus.addListener(listener)
    replay(fileStatus, bus, eventsFilter)

    listener.applicationInfo
  }

  override def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    getApplicationInfoWrapper(appId).map(_ info)
  }

  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    getApplicationInfoWrapper(appId).flatMap(appInfo => {
      appInfo.attempts.find(_.info.attemptId == attemptId).map(attemptInfo => {
        val conf = this.conf.clone()
        val secManager = new SecurityManager(conf)
        secManager.setAcls(HISTORY_UI_ACLS_ENABLE)
        secManager.setAdminAcls(attemptInfo.adminAcls.getOrElse(""))
        secManager.setViewAcls(attemptInfo.info.sparkUser, attemptInfo.adminAclsGroups.getOrElse(""))
        secManager.setAdminAclsGroups(attemptInfo.adminAclsGroups.getOrElse(""))
        secManager.setViewAclsGroups(attemptInfo.viewAclsGroups.getOrElse(""))

        val kvstore = createInMemoryStore(attemptInfo)
        val ui = SparkUI.create(None, new AppStatusStore(kvstore), conf, secManager, appInfo.info.name,
          HistoryServer.getAttemptURI(appInfo.info.id, attemptInfo.info.attemptId),
          attemptInfo.info.startTime.getTime(),
          attemptInfo.info.appSparkVersion
        )

        LoadedAppUI(ui)
      })
    })
  }
  
 
  /**
   * Replays the events in the specified log file on the supplied `ReplayListenerBus`.
   * `ReplayEventsFilter` determines what events are replayed.
   */
  private def replay(
      eventLog: FileStatus,
      bus: ReplayListenerBus,
      eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Unit = {
    val logPath = eventLog.getPath()
    val isCompleted = !logPath.getName().endsWith(EventLoggingListener.IN_PROGRESS)
    logInfo(s"Replaying log path: $logPath")
    // Note that the eventLog may have *increased* in size since when we grabbed the filestatus,
    // and when we read the file here.  That is OK -- it may result in an unnecessary refresh
    // when there is no update, but will not result in missing an update.  We *must* prevent
    // an error the other way -- if we report a size bigger (ie later) than the file that is
    // actually read, we may never refresh the app.  FileStatus is guaranteed to be static
    // after it's created, so we get a file size that is no bigger than what is actually read.
    Utils.tryWithResource(EventLoggingListener.openEventLog(logPath, fs)) { in =>
      logInfo("Trying to replay bus: $isCompleted, $eventsFilter")
      bus.replay(in, logPath.toString, !isCompleted, eventsFilter)
      logInfo(s"Finished parsing $logPath")
    }
  }
 
  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {

    /**
     * This method compresses the files passed in, and writes the compressed data out into the
     * [[OutputStream]] passed in. Each file is written as a new [[ZipEntry]] with its name being
     * the name of the file being compressed.
     */
    def zipFileToStream(file: Path, entryName: String, outputStream: ZipOutputStream): Unit = {
      // Guarantee we are using the Hadoop FS
      val fs = file.getFileSystem(hadoopConf)
      val inputStream = fs.open(file, 1 * 1024 * 1024) // 1MB Buffer
      try {
        outputStream.putNextEntry(new ZipEntry(entryName))
        ByteStreams.copy(inputStream, outputStream)
        outputStream.closeEntry()
      } finally {
        inputStream.close()
      }
    }

   getApplicationInfo(appId) match {
      case Some(appInfo) =>
        try {
          // If no attempt is specified, or there is no attemptId for attempts, return all attempts
          appInfo.attempts.filter { attempt =>
            attempt.attemptId.isEmpty || attemptId.isEmpty || attempt.attemptId.get == attemptId.get
          }.foreach { attempt =>
            val logPath = new Path(logDir)
            zipFileToStream(logPath, appInfo.name, zipStream)
          }
        } finally {
          zipStream.close()
        }
      case None => throw new SparkException(s"Logs for $appId not found.")
    }
  }
  
  private def createInMemoryStore(attempt: AttemptInfoWrapper): KVStore = {
    val store = new InMemoryStore()
    val status = fs.getFileStatus(new Path(logDir, attempt.logPath))
    rebuildAppStore(store, status, attempt.info.lastUpdated.getTime())
    store
  }
  
  /**
   * Rebuilds the application state store from its event log.
   */
  private def rebuildAppStore(
      store: KVStore,
      eventLog: FileStatus,
      lastUpdated: Long): Unit = {
    // Disable async updates, since they cause higher memory usage, and it's ok to take longer
    // to parse the event logs in the SHS.
    val replayConf = conf.clone().set(ASYNC_TRACKING_ENABLED, false)
    val trackingStore = new ElementTrackingStore(store, replayConf)
    val replayBus = new ReplayListenerBus()
    val listener = new AppStatusListener(trackingStore, replayConf, false,
      lastUpdateTime = Some(lastUpdated))
    replayBus.addListener(listener)

    for {
      plugin <- loadPlugins()
      listener <- plugin.createListeners(conf, trackingStore)
    } replayBus.addListener(listener)

    try {
      replay(eventLog, replayBus)
      trackingStore.close(false)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          trackingStore.close()
        }
        throw e
    }
  }
  
  private def loadPlugins(): Iterable[AppHistoryServerPlugin] = {
    ServiceLoader.load(classOf[AppHistoryServerPlugin], Utils.getContextOrSparkClassLoader).asScala
  }

  override def getEmptyListingHtml(): Seq[Node] = {
    <p>
      This History Provider (org.apache.spark.deploy.history.SimpleFsHistoryProvider)
      does not support scanning the 
      <span style="font-style:italic">spark.history.fs.logDirectory</span>
      directory for existing event logs.  Please browse to
      history/<span style="font-style:italic">applicationId</span>
      to view the UI for a particular application
    </p>
  }

  override def getConfig(): Map[String, String] = {
    Map("Event log directory" -> logDir.toString)
  }

  override def stop(): Unit = {}
  
   /**
   * Return true when the application has completed.
   */
  private def isApplicationCompleted(entry: FileStatus): Boolean = {
    !entry.getPath().getName().endsWith(EventLoggingListener.IN_PROGRESS)
  }

   /**
   * String description for diagnostics
   * @return a summary of the component state
   */
  override def toString: String = {
    """
      | FsHistoryProvider: logdir=$logDir,
    """.stripMargin
   }
}

private[history] object SimpleFsHistoryProvider {
  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"

  private val NOT_STARTED = "<Not Started>"

  private val APPL_START_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationStart\""

  private val APPL_END_EVENT_PREFIX = "{\"Event\":\"SparkListenerApplicationEnd\"" 
}
