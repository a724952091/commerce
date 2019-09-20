import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


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

    //获取第一部分聚合的数据
    val userId2AggerInfo = getSessionAggerInfo(sparkSession,sessionId2GroupRDD)
    //与userInfo联立获取全部数据
    val sessionId2FullInfo = getSessionId2FullInfo(sparkSession,userId2AggerInfo)

    //注册 累加器
    val accumulator = new SessionAccumulator
    sparkSession.sparkContext.register(accumulator)
    //根据过滤条件进行数据过滤并做累加
    val sessionIDFilterRDD = getFilterRDD(taskParam,sessionId2FullInfo,accumulator)

    //对数据进行内存缓存
    sessionIDFilterRDD.persist(StorageLevel.MEMORY_ONLY)

    //触发transformation类型算子
    sessionIDFilterRDD.collect()
    //计算比例并保存到数据库
    getAndSave_SessionRatio(sparkSession,sessionIDFilterRDD,accumulator.value,taskUUID)


  }
  def getAndSave_SessionRatio(sparkSession: SparkSession, sessionIDFilterRDD: RDD[(String, String)], value: mutable.HashMap[String, Int], taskUUID: String): Unit = {

    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    //获得所有的访问时长
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s,0).toDouble
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s,0).toDouble
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s,0).toDouble
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s,0).toDouble
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s,0).toDouble
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m,0).toDouble
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m,0).toDouble
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m,0).toDouble
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m,0).toDouble


    //获得所有的访问步长
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    //做计算

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    //将统计结果封装到一个caseclass里

    val sessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    import sparkSession.implicits._

    val sessionAggreRDD = sparkSession.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggreRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_aggr_stat")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }
  def getOriSessionRDD(sparkSession: SparkSession, taskParam: JSONObject)= {
    val startTime = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    import sparkSession.implicits._
    val dataFrame = sparkSession.sql("select * from user_visit_action where date >=  '"+startTime+ "' and date <= '"+endTime+"'")


    dataFrame.as[UserVisitAction].rdd
  }
  // 获取session_id search_keyword click_category visit_length step_length store_time 数据 通过一些简单的计算逻辑
  def getSessionAggerInfo(sparkSession: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggerInfo = sessionId2GroupRDD.map {
      case (session_id, iterableInfo) => {
        var user_id = -1l

        var start_time: Date = null
        var end_time: Date = null

        var step_length = 0

        val click_category = new StringBuffer("")
        val search_keyword = new StringBuffer("")

        for (action <- iterableInfo) {
          if (user_id == -1l) {
            user_id = action.user_id
          }
          val actionTime = DateUtils.parseTime(action.action_time)
          if (start_time == null || start_time.after(actionTime)) {
            start_time = actionTime
          }
          if (end_time == null || end_time.before(actionTime)) {
            end_time = actionTime
          }
          val serachKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(serachKeyword) && !search_keyword.toString.contains(serachKeyword)) {
            search_keyword.append(serachKeyword + ",")
          }
          val clickcategory = action.click_category_id
          if (clickcategory != -1 && !clickcategory.toString.contains(clickcategory)) {
            click_category.append(clickcategory + ",")
          }
          step_length += 1
        }
        val searchKW = StringUtils.trimComma(search_keyword.toString)
        val clickCG = StringUtils.trimComma(click_category.toString)
        val visit_length = (end_time.getTime - start_time.getTime) / 1000

        val aggerInfo = Constants.FIELD_SESSION_ID + "=" + session_id + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCG + "|" + Constants.FIELD_VISIT_LENGTH + "=" + visit_length + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + step_length + "|" + Constants.FIELD_START_TIME + "=" + start_time

        (user_id, aggerInfo)
      }
    }
    userId2AggerInfo
  }
  def getSessionId2FullInfo(sparkSession: SparkSession, userId2AggerInfo: RDD[(Long, String)]) ={
    //把user_info表数据查询出来
    val sql ="select * from user_info"
    import  sparkSession.implicits._

    val userid2UserInfo = sparkSession.sql(sql).as[UserInfo].rdd.map(item=>(item.user_id,item))

    //联立user_info表把剩下的几个字段合并进来

    val sessionId2FullInfo = userId2AggerInfo.join(userid2UserInfo).map {
      case (user_id, (aggerInfo, userInfo)) => {
        val age = userInfo.age
        val profession = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggerInfo +"|"+ Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + profession + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val session_id = StringUtils.getFieldFromConcatString(aggerInfo, "\\|", Constants.FIELD_SESSION_ID)
        (session_id, fullInfo)
      }
    }
    sessionId2FullInfo
  }


  def getFilterRDD(taskParam: JSONObject, sessionId2FullInfo: RDD[(String, String)],  sessionAggrStatAccumulator:SessionAccumulator) = {
    def calculateVisitLength(visitLength: Long) {
      if (visitLength >= 1 && visitLength <= 3) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
      } else if (visitLength >= 4 && visitLength <= 6) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
      } else if (visitLength >= 7 && visitLength <= 9) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
      } else if (visitLength >= 10 && visitLength <= 30) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
      } else if (visitLength > 30 && visitLength <= 60) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
      } else if (visitLength > 60 && visitLength <= 180) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
      } else if (visitLength > 180 && visitLength <= 600) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
      } else if (visitLength > 600 && visitLength <= 1800) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
      } else if (visitLength > 1800) {
        sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
      }
    }

    // 计算访问步长范围
    def calculateStepLength(stepLength: Long) {
      if (stepLength >= 1 && stepLength <= 3) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
      } else if (stepLength >= 4 && stepLength <= 6) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
      } else if (stepLength >= 7 && stepLength <= 9) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
      } else if (stepLength >= 10 && stepLength <= 30) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
      } else if (stepLength > 30 && stepLength <= 60) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
      } else if (stepLength > 60) {
        sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
      }
    }

    val startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)

    val profession = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS)
    val categoryId = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS)


    val filterInfo: String = (if(startAge!=null)Constants.PARAM_START_AGE+"="+startAge+"|" else "")+(if(endAge != null)Constants.PARAM_END_AGE+"="+endAge+"|" else "")+
      (if(profession != null)Constants.PARAM_PROFESSIONALS+"="+profession+"|" else "")+(if(cities != null)Constants.PARAM_CITIES+"="+cities+"|" else "")+
      (if(sex != null)Constants.PARAM_SEX+"="+sex+"|" else "")+(if(keywords != null)Constants.PARAM_KEYWORDS+"="+keywords+"|" else "")+
      (if(categoryId != null)Constants.PARAM_CATEGORY_IDS+"=" +categoryId +"|" else "")


   if(filterInfo.endsWith("\\|")) {
     filterInfo.substring(0, filterInfo.length - 1)
   }
    val sessionIDFilter = sessionId2FullInfo.filter {
      case (session_id, fullInfo) => {
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }
        if(success){
          sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
          val visit_length = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong
          val step_length = StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visit_length)
          calculateStepLength(step_length)
        }
        success
      }
    }
    sessionIDFilter
  }


}
