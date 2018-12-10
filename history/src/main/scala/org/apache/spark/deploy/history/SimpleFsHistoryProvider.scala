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

import java.io.{FileNotFoundException, IOException}
//import java.util.UUID
//import java.util.concurrent.{Executors, ExecutorService, Future, TimeUnit}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.mutable
import scala.xml.Node

import com.google.common.io.ByteStreams
//import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}
import org.apache.hadoop.fs.{FileStatus, Path}
//import org.apache.hadoop.fs.permission.FsAction
//import org.apache.hadoop.hdfs.DistributedFileSystem
//import org.apache.hadoop.hdfs.protocol.HdfsConstants
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.ReplayListenerBus._
import org.apache.spark.ui.SparkUI
import org.apache.spark.deploy.history.ApplicationHistoryProvider
//import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * A class that provides application history from event logs stored in the file system.
 * This provider will render the history application UI of specifically requested
 * applications only.  No scanning or caching of event logs from a filesystem is performed.
 *
 */

private[history] class SimpleFsHistoryProvider(conf: SparkConf)
  extends ApplicationHistoryProvider with Logging {

  import SimpleFsHistoryProvider._

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

  private[history] def initialize(): Unit = {}
  
  override def getEventLogsUnderProcess(): Int = 0

  override def getLastUpdatedTime(): Long = 0
  
  //Return an empty list iterator
  override def getListing(): Iterator[FsApplicationHistoryInfo] = List().iterator
  
  def getApplicationAttemptInfo(appFileStatus: FileStatus): Option[FsApplicationAttemptInfo] = {
 
    // When getting the Application Info, we really only want the key details - Name, ID, start time, stop time
    val eventsFilter: ReplayEventsFilter = { eventString =>
      eventString.startsWith(APPL_START_EVENT_PREFIX) ||
      eventString.startsWith(APPL_END_EVENT_PREFIX)
    }
     
    val appListener = replay(appFileStatus, isApplicationCompleted(appFileStatus), new ReplayListenerBus(), eventsFilter)
    appListener.appId.map(appId => {
      new FsApplicationAttemptInfo(
        appFileStatus.getPath().getName(),
        appListener.appName.getOrElse(NOT_STARTED),
        appId,
        appListener.appAttemptId,
        appListener.startTime.getOrElse(-1L),
        appListener.endTime.getOrElse(-1L),
        0L,
        appListener.sparkUser.getOrElse(NOT_STARTED),
        isApplicationCompleted(appFileStatus),
        appFileStatus.getLen()
      )
    }) 
  }
  
  override def getApplicationInfo(appId: String): Option[FsApplicationHistoryInfo] = {
    try {
      val appAttemptInfo = getApplicationAttemptInfo(fs.getFileStatus(new Path(logDir, appId)))
      appAttemptInfo.map(attemptInfo => {
        new FsApplicationHistoryInfo(attemptInfo.appId, attemptInfo.name, List(attemptInfo))
      })
    } catch {
      case e: FileNotFoundException =>
        logError(s"File Not Found when trying to load log ${logDir}/${appId}", e)
        None
    }
  }

  
  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    try {
      val appFileStatus = fs.getFileStatus(new Path(logDir, appId))
      
      getApplicationInfo(appId).flatMap(appInfo => {
        appInfo.attempts.find(_.attemptId == attemptId).map(attemptInfo => {
          val replayBus = new ReplayListenerBus()
            
          val ui = {
            val conf = this.conf.clone()
            val appSecManager = new SecurityManager(conf)
            SparkUI.createHistoryUI(conf, replayBus, appSecManager, attemptInfo.name,
              HistoryServer.getAttemptURI(attemptInfo.appId, attemptInfo.attemptId),
              attemptInfo.startTime
            )
            // Do not call ui.bind() to avoid creating a new server for each application
          }
          val appListener = replay(appFileStatus, isApplicationCompleted(appFileStatus), replayBus)
          
          ui.getSecurityManager.setAcls(HISTORY_UI_ACLS_ENABLE)
          // make sure to set admin acls before view acls so they are properly picked up
          val adminAcls = HISTORY_UI_ADMIN_ACLS + "," + appListener.adminAcls.getOrElse("")
          ui.getSecurityManager.setAdminAcls(adminAcls)
          ui.getSecurityManager.setViewAcls(attemptInfo.sparkUser, appListener.viewAcls.getOrElse(""))
          val adminAclsGroups = HISTORY_UI_ADMIN_ACLS_GROUPS + "," +
            appListener.adminAclsGroups.getOrElse("")
          ui.getSecurityManager.setAdminAclsGroups(adminAclsGroups)
          ui.getSecurityManager.setViewAclsGroups(appListener.viewAclsGroups.getOrElse(""))
              
          LoadedAppUI(ui, () => false)
        })
      })
    } catch {
        case e: FileNotFoundException =>
          logError(s"File Not Found when trying to load log ${logDir}/${appId}", e)
          None
        case e: Exception =>
          logError(s"Exception encountered when attempting to load application log ${logDir}/${appId}", e)
          None
    }
  }
  
 
  /**
   * Replays the events in the specified log file on the supplied `ReplayListenerBus`. Returns
   * an `ApplicationEventListener` instance with event data captured from the replay.
   * `ReplayEventsFilter` determines what events are replayed and can therefore limit the
   * data captured in the returned `ApplicationEventListener` instance.
   */
  private def replay(
      eventLog: FileStatus,
      appCompleted: Boolean,
      bus: ReplayListenerBus,
      eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): ApplicationEventListener = {
    val logPath = eventLog.getPath()
    logInfo(s"Replaying log path: $logPath")
    // Note that the eventLog may have *increased* in size since when we grabbed the filestatus,
    // and when we read the file here.  That is OK -- it may result in an unnecessary refresh
    // when there is no update, but will not result in missing an update.  We *must* prevent
    // an error the other way -- if we report a size bigger (ie later) than the file that is
    // actually read, we may never refresh the app.  FileStatus is guaranteed to be static
    // after it's created, so we get a file size that is no bigger than what is actually read.
    val logInput = EventLoggingListener.openEventLog(logPath, fs)
    try {
      val appListener = new ApplicationEventListener
      bus.addListener(appListener)
      bus.replay(logInput, logPath.toString, !appCompleted, eventsFilter)
      appListener
    } finally {
      logInput.close()
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
            val logPath = new Path(logDir, attempt.logPath)
            zipFileToStream(logPath, attempt.logPath, zipStream)
          }
        } finally {
          zipStream.close()
        }
      case None => throw new SparkException(s"Logs for $appId not found.")
    }
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
