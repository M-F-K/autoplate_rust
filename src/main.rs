use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, Write};
use std::path::Path;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use suppaftp::FtpStream;
use suppaftp::types::FileType;
use quick_xml::events::Event;
use quick_xml::Reader;
use tempfile::NamedTempFile;
use zip::ZipArchive;

// LicensePlate represents a license plate record
#[derive(Debug, Clone)]
struct LicensePlate {
    plate: String,
    timestamp: SystemTime,
}

// Vehicle represents the XML structure
#[derive(Debug, Default)]
struct Vehicle {
    license_plate: String,
}

// ProgressReader wraps a reader and reports progress
struct ProgressReader<R: Read> {
    reader: R,
    total: u64,
    current: u64,
    last_print: u64,
}

impl<R: Read> ProgressReader<R> {
    fn new(reader: R, total: u64) -> Self {
        Self {
            reader,
            total,
            current: 0,
            last_print: 0,
        }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.current += n as u64;

        // Print progress every 1%
        if self.total > 0 {
            let percent_done = (self.current * 100) / self.total;
            if percent_done > self.last_print {
                self.last_print = percent_done;
                print!(
                    "\rDownloading: {}% ({} / {} bytes)",
                    percent_done, self.current, self.total
                );
                io::stdout().flush().ok();
            }
        }

        Ok(n)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check for command line argument
    let args: Vec<String> = env::args().collect();
    
    // Initialize database (using HashMap as in-memory storage)
    let mut db: HashMap<String, LicensePlate> = HashMap::new();
    
    if args.len() > 1 {
        // Use local file provided as argument
        let filename = &args[1];
        println!("Using local file: {}", filename);
        
        let path = Path::new(filename);
        if !path.exists() {
            return Err(format!("File not found: {}", filename).into());
        }
        
        let mut file = File::open(path)?;
        process_zip_file(&mut file, &mut db)?;
    } else {
        // Download from FTP server
        let mut temp_file = download_from_ftp()?;
        
        // Reset file pointer to beginning
        temp_file.seek(io::SeekFrom::Start(0))?;
        
        process_zip_file(&mut temp_file, &mut db)?;
    }

    // Display results
    display_results(&db);

    Ok(())
}

fn download_from_ftp() -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    // Connect to FTP server
    println!("Connecting to FTP server...");
    let mut ftp_stream = FtpStream::connect("5.44.137.84:21")?;
    ftp_stream.login("anonymous", "anonymous")?;
    
    // Set binary transfer mode
    ftp_stream.transfer_type(FileType::Binary)?;

    // Change to target directory
    ftp_stream.cwd("/ESStatistikListeModtag")?;

    // Find newest zip file
    let entries = ftp_stream.list(None)?;
    
    let mut newest_zip: Option<(String, SystemTime, u64)> = None;
    
    for entry_line in &entries {
        // Parse FTP LIST output (simplified parsing)
        let parts: Vec<&str> = entry_line.split_whitespace().collect();
        if parts.len() < 9 {
            continue;
        }
        
        let filename = parts[8..].join(" ");
        if !filename.ends_with(".zip") {
            continue;
        }
        
        // Extract size (5th column in LIST format)
        let size: u64 = parts[4].parse().unwrap_or(0);
        
        // For simplicity, we'll use current time as modification time
        // In production, you'd parse the date from parts[5], parts[6], parts[7]
        let mod_time = SystemTime::now();
        
        if let Some((_, existing_time, _)) = newest_zip {
            if mod_time > existing_time {
                newest_zip = Some((filename, mod_time, size));
            }
        } else {
            newest_zip = Some((filename, mod_time, size));
        }
    }

    let (zip_name, zip_time, zip_size) = newest_zip
        .ok_or("No zip files found in directory")?;

    let dt: DateTime<Utc> = zip_time.into();
    println!("Downloading: {} ({})", zip_name, dt.to_rfc3339());
    println!("File size: {:.2} MB", zip_size as f64 / (1024.0 * 1024.0));

    // Download zip file to temporary file
    let mut temp_file = NamedTempFile::new()?;
    
    // Get a reader for the remote file
    let reader = ftp_stream.retr_as_stream(&zip_name)?;
    
    // Create progress reader
    let mut progress_reader = ProgressReader::new(reader, zip_size);
    
    // Stream download to temp file with progress
    let written = io::copy(&mut progress_reader, &mut temp_file)?;
    
    println!("\n✓ Downloaded {} bytes", written);
    
    // Finalize the transfer
    ftp_stream.finalize_retr_stream(progress_reader.reader)?;

    // Quit FTP connection
    let _ = ftp_stream.quit();
    
    Ok(temp_file)
}

fn process_zip_file<R: Read + Seek>(
    file: &mut R,
    db: &mut HashMap<String, LicensePlate>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut archive = ZipArchive::new(file)?;

    let mut processed_count = 0;

    for i in 0..archive.len() {
        let zip_file = archive.by_index(i)?;
        
        if zip_file.is_dir() {
            continue;
        }

        println!(
            "Processing: {} ({:.2} KB)",
            zip_file.name(),
            zip_file.size() as f64 / 1024.0
        );

        // Stream parse XML directly from zip without loading into memory
        let mut reader = Reader::from_reader(BufReader::new(zip_file));
        reader.trim_text(true);

        let mut buf = Vec::new();
        let mut in_vehicle = false;
        let mut current_vehicle = Vehicle::default();
        let mut in_license_plate = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    match e.name().as_ref() {
                        b"Vehicle" => {
                            in_vehicle = true;
                            current_vehicle = Vehicle::default();
                        }
                        b"LicensePlate" => {
                            in_license_plate = true;
                        }
                        _ => {}
                    }
                }
                Ok(Event::Text(e)) => {
                    if in_vehicle && in_license_plate {
                        if let Ok(text) = e.unescape() {
                            current_vehicle.license_plate = text.into_owned();
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    match e.name().as_ref() {
                        b"LicensePlate" => {
                            in_license_plate = false;
                        }
                        b"Vehicle" => {
                            in_vehicle = false;
                            
                            if !current_vehicle.license_plate.is_empty() {
                                let plate = LicensePlate {
                                    plate: current_vehicle.license_plate.clone(),
                                    timestamp: SystemTime::now(),
                                };
                                db.insert(plate.plate.clone(), plate);
                                processed_count += 1;

                                // Progress indicator
                                if processed_count % 1000 == 0 {
                                    println!("  Processed {} plates...", processed_count);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => {
                    eprintln!(
                        "Warning: XML parse error at position {}: {}",
                        reader.buffer_position(),
                        e
                    );
                    break;
                }
                _ => {}
            }
            buf.clear();
        }
    }

    println!("\n✓ Successfully processed {} license plates", processed_count);
    Ok(())
}

fn display_results(db: &HashMap<String, LicensePlate>) {
    let mut plates: Vec<String> = db.keys().cloned().collect();
    plates.sort();

    println!("\n=== License Plates in Database ({} total) ===", plates.len());
    
    for (i, plate) in plates.iter().enumerate() {
        println!("{}. {}", i + 1, plate);
        if i >= 9 {
            println!("... and {} more", plates.len() - 10);
            break;
        }
    }
}
