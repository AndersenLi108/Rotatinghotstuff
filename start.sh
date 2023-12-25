#!/bin/sh

echo "start.sh <shard_id> <N> <f> <B> <K>"

rm ./TXs
touch TXs
python3 ./bdtbft/core/tx_generator.py --shard_num $1 --tx_num 20000
python3 run_trusted_key_gen.py --N $2 --f $3


# llall python3      

shard_id=0
while [ "$shard_id" -lt $1 ]; do
    node_id=0
    while [ "$node_id" -lt $(( $2 - 0 )) ]; do
        echo "start node $node_id in shard $shard_id..."
        rm "./TXs_file/TXs_$(($shard_id * $2 + $node_id))"
        cp ./TXs "./TXs_file/TXs_$(($shard_id * $2 + $node_id))"
        python3 run_socket_node.py --sid 'sidA' --id $node_id --shard_id $shard_id --shard_num $1 --N $2 --f $3 --S 1 --T 1 --B $4 --F 1000  --K $5 &
        node_id=$(( node_id + 1 ))
    done
    shard_id=$(( shard_id + 1 ))
done
