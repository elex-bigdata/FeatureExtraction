#!/bin/sh
# 根据起止日期（中间跨月），返回以空格分隔的日期串，保存在list中返回
# 参数：起止日期,终止日期
get_day_list()
{
	list=$1
	local thisday=$(($1 + 1))
	local last=$thisday
	
	while [ $thisday -le $2 ]; do
		local year=`echo $thisday|cut -c 1-4`
		local month=`echo $thisday|cut -c 5-6`
		local day=`echo $thisday|cut -c 7-8`
		
		# 计算当月最大天数
		local max=31
		case $month in
			# 1|3|5|7|8|10|12) max=31;;
			4|6|9|11)	max=30;;
			2) max=28;;
		esac
		
		#echo $year $month $day $max
		
		#闰年：能被400整除，或者能被4整除而不能被100整除
		if [ $max -eq 28 ]; then 
			# 好象有问题？getdays.ksh 19000227 19000302
			if [ `expr $year%400` -eq 0 -o  `expr $year%4` -eq 0 -a `expr $year%100` -ne 0  ]; then
				max=29;
			fi
		fi
		
		# 如果日期超出当月最大天数，调整到下月1号
		if [ $day -gt $max ]; then
			day=1;
			if [ $month -eq 12 ]; then
				month=1
				year=$(($year+1))
			else
				month=$(($month+1))
			fi
			
			#echo $year $month $day $max          
			thisday=$(($year*10000 + $month*100 + $day))
			#echo "this: ${thisday}"
		fi
		
		# 将日期添加到list后面
		list="${list} ${thisday}"
		
		last=$thisday
		thisday=$(($thisday+1))
	done
	
	#echo $list
}

#frnext='20141025'
#to='20141113'
list=""
get_day_list $1 $2
day_list=$list 		# 函数调用得到
for day in $day_list
do
  echo "alter table log_merge drop partition (day='"$day"');"
  echo "alter table log_merge2 drop partition (day='"$day"');"
  echo "alter table feature drop partition (day='"$day"');"
  echo "alter table profile drop partition (day='"$day"');"
  echo "alter table query_en drop partition (day='"$day"');"
  echo "alter table query_en2 drop partition (day='"$day"')"
done
