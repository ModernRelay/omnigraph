//! Supplied command input. Read one line without consuming a following payload.
use std::io::{self, Read};

pub(crate) struct Input<'a> {
    pub(crate) reader: &'a mut dyn Read,
    pub(crate) is_terminal: bool,
}

pub(crate) fn read_line(reader: &mut dyn Read) -> io::Result<String> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0];
        match reader.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    String::from_utf8(line).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        )
    })
}
