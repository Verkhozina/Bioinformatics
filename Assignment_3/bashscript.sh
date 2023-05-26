#!/bin/bash

fastqc ./biofiles/input2.fastq.gz -o ./biofiles
minimap2 -d ./biofiles/input.mmi ./biofiles/input.fna
samtools faidx ./biofiles/input.fna
minimap2 -a ./biofiles/input.mmi ./biofiles/input2.fastq.gz > output.sam
samtools view -b ./output.sam > ./output.bam
samtools flagstat ./output.bam > ./flagstat.txt
check=$(python3 check_flagstat.py flagstat.txt)
echo "ReadQC = $check"
if [ "$check" = "GOOD" ]
then
  echo "Starting sort"
  samtools sort -o ./output.sorted.bam ./output.bam
  echo "Starting freebayes"
  freebayes -f ./biofiles/input.fna ./output.sorted.bam > ./result.vcf
  echo "Finished"
else
  echo "Failed"
fi
