package org.apache.livy.utils

import java.util
import java.util.Collections

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.service.Service.STATE
import org.apache.hadoop.yarn.api.protocolrecords.{GetApplicationsRequest, GetApplicationsResponse}
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.livy.Logging


object LivyYarnClient extends Logging {


  var yarnClient: LivyYarnClient = null;

  def newYarnClient = {
    info("init new yarn client")
    val c = new LivyYarnClient()
    c.init(new YarnConfiguration())
    c.start()
    c
  }

  def getInstance(): LivyYarnClient = {
    synchronized {
      if (yarnClient == null || yarnClient.getServiceState != STATE.STARTED) {
        yarnClient = newYarnClient
      } else if ((System.currentTimeMillis() - yarnClient.getStartTime) >= (6 * 60 * 60 * 1000)) {
        closeYarnClient(yarnClient)
        yarnClient = newYarnClient
      }
      yarnClient
    }
  }

  def closeYarnClient(yarnClient: YarnClient): Unit = {
    try {
      yarnClient.close
    } catch {
      case e: Exception => {
        warn(s"ignore yarn client close exception: ${e.getMessage}")
      }
    }

  }
}

class LivyYarnClient extends YarnClientImpl {

  val logger = LogFactory.getLog(classOf[LivyYarnClient])

  def getApplicationsByTags(tags: util.Set[String]): util.List[ApplicationReport] = {

    var result = Collections.emptyList[ApplicationReport]()
    try {
      val request: GetApplicationsRequest = GetApplicationsRequest.newInstance()
      request.setApplicationTags(tags)
      val response: GetApplicationsResponse = rmClient.getApplications(request)
      result = response.getApplicationList
    } catch {
      case e: Exception => {
        logger.error(s"get error ${e.toString} when getApplicationsByTags: ${tags}")
      }
    }
    result
  }

}
