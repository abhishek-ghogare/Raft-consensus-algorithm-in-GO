
username="labuser1"
pass="labuser1"


./script_lib.sh stopAll
./script_lib.sh clearAll


for i in `seq 66 70`
do
	sshpass -p $pass ssh $username@osl-$i 'mkdir ~/raft -p'
	sshpass -p $pass scp config.json $username@osl-$i:~/config.json
	sshpass -p $pass scp raft_main $username@osl-$i:~/
	sshpass -p $pass scp ../../../bin/pp $username@osl-$i:~/
done


./script_lib.sh startAll


