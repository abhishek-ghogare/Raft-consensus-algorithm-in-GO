
username="labuser1"
pass="labuser1"

# Execute a command $1 on host $2
# execute <cmd> <host>
function execute {
	echo "executing on $2 cmd:$1"
	sshpass -p $pass ssh $username@$2 $1
}
# Execute a command $1 on all
# executeOnAll <cmd>
function executeOnAll {
	for i in `seq 66 70`
	do
		execute "$1" "osl-$i"
	done
}

# Start raft_main process
# startServer <host>
function startServer {
	id=$((`cut -d '-' -f 2 <<<$1`-65))
	sshpass -p $pass ssh $username@$1 "exec 2>>./raft/debug.log 1>>./raft/debug.log ; nohup ./raft_main -clean_start -id $id |& ./pp 2>&1 &" &
}
function resumeServer {
	id=$((`cut -d '-' -f 2 <<<$1`-65))
	sshpass -p $pass ssh $username@$1 'exec 2>>raft/raft_$(($(cut -d '-' -f 2 <<<$1)-65))/debug.log 1>>raft/raft_$(($(cut -d '-' -f 2 <<<$1)-65))/debug.log ; nohup ./raft_main -id $(($(cut -d '-' -f 2 <<<$1)-65) |& ./pp 2>&1 &' &
}

# Stops raft_main process
# stopServer <host>
function stopServer {
	execute 'killall raft_main' $1
}


function startAll {
	for i in `seq 66 70`
	do
		echo "starting osl-$i"
		startServer "osl-$i"
	done
}

function resumeAll {
	for i in `seq 66 70`
	do
		echo "resuming osl-$i"
		resumeServer "osl-$i"
	done	
}





case "$1" in
	start)
		startServer $2
		;;
	stop)
		stopServer $2
		;;
	resume)
		resumeServer $2
		;;
	startAll)
		startAll
		;;
	stopAll)
		executeOnAll 'killall raft_main'
		;;
	resumeAll)
		resumeAll
		;;
	clearAll)
		executeOnAll "rm -rf raft"
		;;
	-3All)
		executeOnAll 'killall raft_main -3'
		;;
	getlogs)
		mkdir logs -p
		for i in `seq 66 70`
		do
			echo "getting logs of osl-$i"
			execute "cat raft/debug.log" osl-$i  > logs/osl-$i
		done
		;;
	*)
		echo "Unknown command"
		echo "$0 [ start | stop | resume | startAll | stopAll | resumeAll | clearAll | -3All | getlogs ] <host>"
		;;
esac
