
username="labuser1"
pass="labuser1"


./script_lib.sh stopAll
./script_lib.sh clearAll


for i in `seq 38 42`
do
	sshpass -p $pass ssh $username@nsl-$i 'mkdir ~/raft -p'
	cat config.json.template | sed "s/<node_id>/"$((i-37))"/g" > /tmp/config.json
	sshpass -p $pass scp /tmp/config.json $username@nsl-$i:~/raft/config.json
	sshpass -p $pass scp raft_main $username@nsl-$i:~/raft/
done


./script_lib.sh startAll