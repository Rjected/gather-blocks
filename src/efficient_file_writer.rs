use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

/// A buffered writer that writes to a file efficiently by buffering writes.
///
/// This writer will write to a file, and when the number of writes performed exceeds the number of
/// writes per file, it will create a new file and write to that file. This is useful for writing
/// large amounts of data to disk without having to worry about the file size.
///
/// tbh im not sure that this is even efficient, but it's a good exercise in using the `Write`
/// trait and a BufWriter
///
/// # Examples
/// ```
/// use efficient_file_writer::EfficientFileWriter;
/// use std::path::Path;
/// use std::io::Write;
/// let mut writer = EfficientFileWriter::new("test".to_string(), 10, Path::new(".")).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
/// writer.write("test".as_bytes()).unwrap();
///
/// // clean up the batched files before asserts
/// std::fs::remove_file("test.0").unwrap();
/// std::fs::remove_file("test.10").unwrap();
/// assert_eq!(writer.current_file_name(), "test.10");
/// ```
/// # Panics
/// This writer will panic if the number of writes per file is 0.
///
/// # Errors
/// This writer will return an error if the file cannot be created.
pub struct EfficientFileWriter<'a> {
    /// The name to prepend to the current file number
    base_name: String,
    /// The directory to write to
    directory: &'a Path,
    /// The number of writes per file
    writes_per_file: usize,
    /// The total number of writes performed
    writes_performed: usize,
    /// The current file being written to
    current_file: File,
    /// The current buffer being written to
    current_buffer: BufWriter<File>,
}

impl<'a> EfficientFileWriter<'a> {
    /// Create a new EfficientFileWriter
    pub fn new(
        base_name: String,
        writes_per_file: usize,
        directory: &'a Path,
    ) -> Result<Self, std::io::Error> {
        // create the directory if it doesn't exist
        std::fs::create_dir_all(directory)?;

        // create the file
        let file = File::create(directory.join(format!("{}.{}", base_name, 0)))?;
        let buffer = BufWriter::new(file.try_clone()?);
        Ok(Self {
            base_name,
            directory,
            writes_per_file,
            writes_performed: 0,
            current_file: file,
            current_buffer: buffer,
        })
    }

    /// Write an object that can be converted to a byte array. Write it as a single line in the
    /// file. return a Result<usize, std::io::Error>
    pub fn write<T: AsRef<[u8]>>(&mut self, data: T) -> Result<usize, std::io::Error> {
        // write the data to the buffer
        let mut bytes_written = self.current_buffer.write(data.as_ref())?;
        // write a newline to the buffer
        bytes_written += self.current_buffer.write(b"\n")?;
        // increment the number of writes performed
        self.writes_performed += 1;
        // if the number of writes performed is greater than the number of writes per file, create a new file
        if self.writes_performed % self.writes_per_file == 0 {
            // create the file
            let file = File::create(
                self.directory
                    .join(format!("{}.{}", self.base_name, self.writes_performed)),
            )?;
            let buffer = BufWriter::new(file.try_clone()?);
            // set the current file and buffer to the new file and buffer
            self.current_file = file;
            self.current_buffer = buffer;
        }
        Ok(bytes_written)
    }

    /// Flush the current buffer
    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        self.current_buffer.flush()
    }

    /// Return the name of the file the writer should be writing to
    pub fn current_file_name(&self) -> String {
        let current_file_number =
            self.writes_performed - (self.writes_performed % self.writes_per_file);
        format!("{}.{}", self.base_name, current_file_number)
    }
}

impl<'a> Drop for EfficientFileWriter<'a> {
    fn drop(&mut self) {
        // flush the buffer
        self.flush().unwrap();
    }
}

impl<'a> Write for EfficientFileWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.write(buf)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_file_request_test() {
        // test that the writer creates a new file after the number of writes per file is exceeded
        let mut writer = EfficientFileWriter::new("test".to_string(), 10, Path::new(".")).unwrap();
        for _ in 0..11 {
            writer.write("test".as_bytes()).unwrap();
        }
        // clean up the batched files before asserts
        std::fs::remove_file("test.0").unwrap();
        std::fs::remove_file("test.10").unwrap();
        assert_eq!(writer.current_file_name(), "test.10");
    }
}
