#!/bin/bash

#
# ===========================================================================
# 程序名称:     
# 功能描述:     预处理原始日志
# 输入参数:     运行日期
# 数据源  :     /data/weblog/preprocess/input/2016-03-07  
# 目标路径:     /data/weblog/preprocess/output/2016-03-07
# 代码审核:     
# 修改人名:
# 修改日期:
# 修改原因:
# 修改列表: 
# ===========================================================================

#set java env
export JAVA_HOME=/home/hadoop/apps/jdk1.7.0_51
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH

#set hadoop env
export HADOOP_HOME=/home/hadoop/apps/hadoop-2.6.1
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH


#清洗程序类名
preprocess_class="cn.itcast.bigdata.weblog.pre.WeblogPreProcess"

#待处理日志存放的目录
log_pre_input=/data/weblog/preprocess/input

#预处理输出结果(raw)目录
log_pre_output=/data/weblog/preprocess/output



#获取时间信息
day_01=`date -d'-1 day' +%Y-%m-%d`
syear=`date --date=$day_01 +%Y`
smonth=`date --date=$day_01 +%m`
sday=`date --date=$day_01 +%d`


#读取日志文件的目录，判断是否有当日待处理的目录(如：2016-03-18)
files=`hadoop fs -ls $log_pre_input | grep $day_01 | wc -l`
if [ $files -gt 0 ]; then
#提交mr任务job运行
echo "running..    hadoop jar weblog.jar $preprocess_class $log_pre_input/$day_01 /$log_pre_output/$day_01"
hadoop jar weblog.jar $preprocess_class $log_pre_input/$day_01 $log_pre_output/$day_01
fi

#如果失败
#发送邮件或短信，人为来干预


