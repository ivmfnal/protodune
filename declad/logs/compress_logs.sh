#!/bin/bash

compress_dir()
{
        echo "  compressing directory $1 (keep $2) ..."
	cd $1
        n=$2
	for f in *; do
                if [ -f $f ] && [ -f ${f}.1 ]; then
			echo "    rotating file $f ..."
			rm -f ${f}.${n}.gz
			i=$n
			while [ $i -gt 1 ]
			do
				j=$(($i-1))
				mv ${f}.${j}.gz ${f}.${i}.gz 2>/dev/null
				i=$j
			done
			gzip ${f}.1
		fi
	done
        cd - > /dev/null
}

compress_area()
{
	if [ -d $1 ]
	then
		unit=${3:-day}
		echo compressing area $1 "(retention: $2 $unit) ..."
		for d in `find $1 -type d -print`; do
			compress_dir $d $2
		done
		case $unit in
			day) 
				find $1 -type f -mtime $2 -delete
				;;
			hour) 
				find $1 -type f -mmin $(($2 * 60)) -delete
				;;
			*)	echo unknown time unit $3
		esac
	else
		echo area $1 not found, skipping
	fi
}

cd $1
compress_area month 30 
compress_area week 7 
compress_area day 24 hour
