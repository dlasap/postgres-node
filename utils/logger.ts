const { createLogger, format, transports } = require("winston");
const { combine, timestamp, label, printf } = format;

const myFormat = printf(({ level, message, label, timestamp }: any) => {
  return `${timestamp} [${label}] ${level}: ${message}`;
});

const createCustomLogger = (log_label: string) => {
  return createLogger({
    format: combine(label({ label: log_label }), timestamp(), myFormat),
    transports: [new transports.Console()],
  });
};

export default createCustomLogger;
