use log::{LevelFilter, Metadata, Record};
use std::sync::Once;

pub struct SimpleLogger {
    level: LevelFilter,
}

static LOGGER: SimpleLogger = SimpleLogger {
    level: LevelFilter::Info,
};
static INIT: Once = Once::new();

impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            println!("[{}] {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

pub fn init() {
    INIT.call_once(|| {
        let level = std::env::var("LOG_LEVEL")
            .ok()
            .and_then(|l| l.parse().ok())
            .unwrap_or(LevelFilter::Info);
        unsafe {
            // Cast to mutable reference via const pointer since LOGGER is static
            let logger = &mut *( &LOGGER as *const _ as *mut SimpleLogger );
            logger.level = level;
            log::set_logger(logger).expect("set logger");
            log::set_max_level(level);
        }
    });
}
