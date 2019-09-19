import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object SessionStat {




  def main(args: Array[String]): Unit = {

    //获取过滤条件

    val jSONObject = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    //获取过滤条件对应的json串

    val taskParam = JSONObject.fromObject(jSONObject)

    //创建一个全局的UUID
    val taskUUID = UUID.randomUUID().toString

    // 创建SparkConf对象

    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    //创建sparkSession

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取原始数据的RDD（只过滤了时间）
    val oriSessionRDD = getOriSessionRDD(sparkSession,taskParam)

    //通过map方法把session作为key用于后面的操作
    val sessionId2RDD = oriSessionRDD.map(item =>(item.session_id,item))

    //根据key分组
    val sessionId2GroupRDD = sessionId2RDD.groupByKey()

    //把分组后的数据做一个缓存
    sessionId2GroupRDD.cache()

    sessionId2GroupRDD.foreach(println(_))

  }
  def getOriSessionRDD(sparkSession: SparkSession, taskParam: JSONObject)= {
    val startTime = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    import sparkSession.implicits._
    val dataFrame = sparkSession.sql("select * from user_visit_action where date >=  '"+startTime+ "' and date <= '"+endTime+"'")

    dataFrame.as[UserVisitAction].rdd
  }
}
