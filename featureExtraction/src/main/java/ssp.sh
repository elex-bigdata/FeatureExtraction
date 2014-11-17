#!/bin/sh
basedir='/home/hadoop/wuzhongju/ssp/'
hadoop jar /home/hadoop/wuzhongju/ssp/featureExtraction-1.0.jar com.elex.ssp.Scheduler 1 6
today=`date -d now +%Y%m%d`
todaydir=$basedir$today
yestoday=$basedir`date -d yesterday +%Y%m%d`
rm -rf $yestoday
rm -rf $yestoday'.gz2'
mkdir $todaydir
hadoop fs -getmerge /user/hive/warehouse/odin.db/feature_export $todaydir'/feature.txt'
hadoop fs -getmerge /user/hive/warehouse/odin.db/profile_export $todaydir'/profile.txt'
hadoop fs -getmerge /user/hive/warehouse/odin.db/user_keyword_export $todaydir'/userkeyword.txt'
cd $basedir
tar -czf $today'.gz2' $today
scp $today'.gz2' elex@10.102.66.212:/data/odin_model
