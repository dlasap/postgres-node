const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require('worker_threads');

function chunkify<T>(a: Array<T>, n: number, balanced = true) {
  if (n < 2) return [a];

  var len = a.length,
    out = [],
    i = 0,
    size;

  if (len % n === 0) {
    size = Math.floor(len / n);
    while (i < len) {
      out.push(a.slice(i, (i += size)));
    }
  } else if (balanced) {
    while (i < len) {
      size = Math.ceil((len - i) / n--);
      out.push(a.slice(i, (i += size)));
    }
  } else {
    n--;
    size = Math.floor(len / n);
    if (len % size === 0) size--;
    while (i < size * n) {
      out.push(a.slice(i, (i += size)));
    }
    out.push(a.slice(size * n));
  }

  return out;
}
if (isMainThread) {
  module.exports = function chunkifyAsync<T = Array<any>>(
    a: Array<T>,
    n: number,
    balanced = true
  ) {
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename, {
        workerData: {
          a,
          n,
          balanced,
        },
      });
      worker.on('message', resolve);
      worker.on('error', reject);
      worker.on('exit', (code: any) => {
        if (code !== 0)
          reject(new Error(`Worker stopped with exit code ${code}`));
      });
    });
  };
} else {
  const { a, n, balanced = true } = workerData;
  parentPort.postMessage(chunkify(a, n, balanced));
}
