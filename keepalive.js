const { spawn } = require('child_process');

const toRun = process.argv.slice(2);
if (toRun.length) {
    const cmd = toRun[0];
    const args = toRun.slice(1);

    const proc = spawn(cmd, args);

    proc.stdout.on('data', (data) => process.stdout.write(data));
    proc.stderr.on('data', (data) => process.stderr.write(data));
    proc.on('close', (code) => process.exit(code));

    setInterval(() => console.log('Still running...'), 30000);
}