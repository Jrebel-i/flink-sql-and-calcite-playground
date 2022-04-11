package me.lmagic233.playground.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author kangqi
 * @date 2022/1/11
 */
object WindowTVFTwoPhaseAggr {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(4)

    val tableEnv = StreamTableEnvironment.create(streamEnv, EnvironmentSettings.newInstance().build())
    // tableEnv.getConfig.getConfiguration.setString("table.exec.mini-batch.enabled", "true")
    // tableEnv.getConfig.getConfiguration.setString("table.exec.mini-batch.allow-latency", "1 s")
    // tableEnv.getConfig.getConfiguration.setString("table.exec.mini-batch.size", "100")
    // tableEnv.getConfig.getConfiguration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE")
    // tableEnv.getConfig.getConfiguration.setString("table.optimizer.distinct-agg.split.enabled", "true")

    tableEnv.executeSql(
      s"""
         |CREATE TABLE datagen_order_log (
         |  order_id BIGINT,
         |  merchandise_id BIGINT,
         |  ts AS CURRENT_TIMESTAMP,
         |  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
         |) WITH (
         |  'connector' = 'datagen',
         |  'rows-per-second' = '10',
         |  'fields.order_id.kind' = 'random',
         |  'fields.order_id.min' = '1',
         |  'fields.order_id.max' = '6553600',
         |  'fields.merchandise_id.kind' = 'random',
         |  'fields.merchandise_id.min' = '1',
         |  'fields.merchandise_id.max' = '1024'
         |)
         |""".stripMargin
    )

//    tableEnv.executeSql(
//      s"""
//         |SELECT window_start, window_end, merchandise_id, COUNT(order_id)
//         |FROM TABLE(
//         |  TUMBLE(TABLE datagen_order_log, DESCRIPTOR(ts), INTERVAL '10' SECONDS)
//         |) GROUP BY window_start, window_end, merchandise_id
//         |""".stripMargin
//    )
    tableEnv.executeSql(
      s"""
         |SELECT a.order_id,b.order_id from datagen_order_log a full join datagen_order_log b on a.order_id=b.order_id
         |""".stripMargin
    )
  }
}
