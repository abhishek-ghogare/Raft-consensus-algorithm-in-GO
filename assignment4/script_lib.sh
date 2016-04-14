
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
	for i in `seq 38 42`
	do
		execute "$1" "nsl-$i"
	done
}

# Start raft_main process
# startServer <host>
function startServer {
	sshpass -p $pass ssh $username@$1 'cd ~/raft ; exec 2>>debug.log 1>>debug.log ; nohup ./raft_main -clean_start 2>&1 &' &
}
function resumeServer {
	sshpass -p $pass ssh $username@$1 'cd ~/raft ; nohup ./raft_main 2>&1 &' &
}

# Stops raft_main process
# stopServer <host>
function stopServer {
	execute 'killall raft_main' $1
}


function startAll {
	for i in `seq 38 42`
	do
		echo "starting nsl-$i"
		startServer "nsl-$i"
	done
}

function resumeAll {
	for i in `seq 38 42`
	do
		echo "resuming nsl-$i"
		resumeServer "nsl-$i"
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
	*)
		echo "Unknown command"
		echo "$0 [start|stop|resume|startAll|stopAll|resumeAll|clearAll|-3All] <host>"
		;;
esac
