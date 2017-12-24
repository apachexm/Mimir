#!/bin/bash
nproc=4
testdir=bfs-test
infile=../datasets/graph_1024v
exedir=../../examples
root=0
N=1024

mkdir $testdir
cd $testdir

python ../bfs.py $root $N $infile bfs.out1

mkdir results
mpiexec -n $nproc $exedir/bfs_join $root $N ./results/ $infile
if [ $? -ne 0 ]
then
    echo "Error in bfs."
    cd ..
    rm -rf $testdir
    exit 1
fi

python ../bfs-p2l.py $root $N results bfs.out2
diff bfs.out1 bfs.out2
if [ $? -ne 0 ]
then
    echo "Error in bfs."
    cd ..
    rm -rf $testdir
    exit 1
fi


cd ..
rm -rf $testdir

echo "Done"

exit 0
