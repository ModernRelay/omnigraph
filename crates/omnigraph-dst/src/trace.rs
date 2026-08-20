//! The TRACE-diff instrument: capture a universe's full TRACE log
//! (thread-scoped, no timestamps, no ANSI — nothing benignly time-varying in
//! the text) and locate the first REAL divergence between two logs.
//! Thread-scoped capture is sufficient because the whole universe — including
//! Lance's inlined compute under deterministic mode — runs on one thread.

/// Run `f` under a thread-scoped TRACE subscriber; return its result + log.
pub fn capture_trace<T>(f: impl FnOnce() -> T) -> (T, String) {
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
        type Writer = Buf;
        fn make_writer(&'a self) -> Buf {
            self.clone()
        }
    }

    let buf = Buf(Arc::new(Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .without_time()
        .with_ansi(false)
        .with_target(true)
        .with_writer(buf.clone())
        .finish();
    let result = tracing::subscriber::with_default(subscriber, f);
    let log = String::from_utf8_lossy(&buf.0.lock().unwrap()).into_owned();
    (result, log)
}

/// First divergence between two trace logs whose digit-stripped forms differ
/// (pure-digit differences — durations, sizes — are benign noise, not
/// schedule divergence). Returns (line index, printable context).
pub fn first_trace_divergence(a: &str, b: &str, context: usize) -> Option<(usize, String)> {
    fn stripped(s: &str) -> String {
        s.chars().filter(|c| !c.is_ascii_digit()).collect()
    }
    let a_lines: Vec<&str> = a.lines().collect();
    let b_lines: Vec<&str> = b.lines().collect();
    let n = a_lines.len().max(b_lines.len());
    for i in 0..n {
        let la = a_lines.get(i).copied().unwrap_or("<EOF>");
        let lb = b_lines.get(i).copied().unwrap_or("<EOF>");
        if stripped(la) != stripped(lb) {
            let lo = i.saturating_sub(context);
            let mut out = format!(
                "first divergence at trace line {i} (A: {} lines, B: {} lines)\n",
                a_lines.len(),
                b_lines.len()
            );
            for j in lo..(i + context).min(n) {
                let marker = if j == i { ">>" } else { "  " };
                out.push_str(&format!(
                    "{marker} A[{j}]: {}\n{marker} B[{j}]: {}\n",
                    a_lines.get(j).copied().unwrap_or("<EOF>"),
                    b_lines.get(j).copied().unwrap_or("<EOF>")
                ));
            }
            return Some((i, out));
        }
    }
    None
}
