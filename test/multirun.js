var childproc = require("child_process");

const GENERATION_INTERVAL = 500;
const WORKERS_NUMBER = 20;
const MESSAGES_COUNT = 1000;

var cprocs = [];
for (var i = 1; i <= WORKERS_NUMBER; i++) {
    cprocs.push(
        childproc.fork("../index.js", [
            "--loggingLevel=info",
            "--logFilePath=./out.log",
            "--interval=" + GENERATION_INTERVAL,
            "--maxGenerated=" + MESSAGES_COUNT,
            "--nodeId=N-" + (i)
        ])
    );
}

function onExit() {
    cprocs.forEach(function killChild(child) {
        child.kill("SIGTERM");
    });
    process.exit(0);
}
process.on("SIGTERM", onExit);
process.on('SIGINT', onExit);
process.on('SIGQUIT', onExit);


//unique messages processed count
//> fgrep 'handling' out.log | awk '{print $7}' | sort -u | wc -l