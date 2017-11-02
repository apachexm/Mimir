#!/bin/bash
nproc=4
testdir=wc-cb-test
infile=./dataset
exedir=../../examples
gendir=../../generator
wordcnt=1048576

mkdir $testdir
cd $testdir

mkdir dataset
mpiexec -n $nproc $gendir/gen_words $wordcnt dataset/infile   \
    --zipf-n 1000 --zipf-alpha 0.5 -disorder -exchange > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in generator."
    cd ..
    rm -rf $testdir
    exit 1
fi

python ../wordcount.py $infile wc.out > /dev/null
cat wc.out | sort -k1 -k2 -n > wc.sort1.out

mpiexec -n $nproc $exedir/wc_cb wc.out $infile > /dev/null
cat wc.out$nproc.* | sort -k1 -k2 -n > wc.sort2.out
diff wc.sort1.out wc.sort2.out > /dev/null
if [ $? -ne 0 ]
then
    echo "Error in wc_cb."
    cd ..
    rm -rf $testdir
    exit 1
fi

cd ..
rm -rf $testdir

echo "Done"

exit 0
