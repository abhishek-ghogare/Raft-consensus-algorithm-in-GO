
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
		echo "executing on nsl-$i cmd:$1"
		execute "$1" "nsl-$i"
	done
}

# Start raft_main process
# startServer <host>
function startServer {
	sshpass -p $pass ssh $username@$1 'cd ~/raft ; nohup ./raft_main -clean_start &' &
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




case "$1" in
	start)
		startServer $2
		;;
	stop)
		stopServer $2
		;;
	startAll)
		startAll
		;;
	stopAll)
		executeOnAll 'killall raft_main'
		;;
	*)
		echo "Unknown command"
		echo "$0 [start|stop] <host>"
		;;
esac
