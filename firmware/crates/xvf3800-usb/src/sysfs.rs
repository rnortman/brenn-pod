//! A USB device as sysfs describes it: one directory under
//! `/sys/bus/usb/devices` (or wherever a caller resolved it to), read with
//! `std::fs` alone.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

/// One USB device, from the attributes of its sysfs directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsbDevice {
    /// The directory the attributes were read from.
    pub dir: PathBuf,
    /// The device's port path, which is the directory's name (`3-2.2`): stable for
    /// as long as the board stays in the same socket, and different for two boards
    /// in two sockets.
    pub port: String,
    /// `busnum`.
    pub bus: u16,
    /// `devnum`: the address on the bus, reassigned on every enumeration.
    pub address: u16,
    /// `idVendor`.
    pub vendor: u16,
    /// `idProduct`.
    pub product: u16,
    /// `bcdDevice`, the device's release number as its descriptor gives it.
    pub bcd_device: u16,
    /// `serial`, absent for a device that reports no serial number.
    pub serial: Option<String>,
}

impl UsbDevice {
    /// Read the device whose sysfs directory is `dir`.
    ///
    /// Every attribute but `serial` must be present and well formed; a missing or
    /// malformed one is an error naming the file, never a default.
    pub fn read(dir: &Path) -> io::Result<Self> {
        let port = dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{} has no port name", dir.display()),
                )
            })?
            .to_string();
        Ok(Self {
            dir: dir.to_path_buf(),
            port,
            bus: decimal(dir, "busnum")?,
            address: decimal(dir, "devnum")?,
            vendor: hex4(dir, "idVendor")?,
            product: hex4(dir, "idProduct")?,
            bcd_device: hex4(dir, "bcdDevice")?,
            serial: optional(dir, "serial")?,
        })
    }

    /// The usbfs node for this device under `dev_bus_usb` (`/dev/bus/usb` outside
    /// tests), named as the kernel names it: three-digit bus and address.
    pub fn node_path(&self, dev_bus_usb: &Path) -> PathBuf {
        dev_bus_usb
            .join(format!("{:03}", self.bus))
            .join(format!("{:03}", self.address))
    }

    /// `vendor:product`, as the kernel prints a USB id.
    pub fn usb_id(&self) -> String {
        format!("{:04x}:{:04x}", self.vendor, self.product)
    }
}

impl fmt::Display for UsbDevice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at port {} (bus {} address {})",
            self.usb_id(),
            self.port,
            self.bus,
            self.address
        )
    }
}

fn attribute(dir: &Path, name: &str) -> io::Result<String> {
    let path = dir.join(name);
    std::fs::read_to_string(&path)
        .map(|text| text.trim_end_matches('\n').to_string())
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))
}

fn malformed(dir: &Path, name: &str, text: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{}: malformed value {text:?}", dir.join(name).display()),
    )
}

fn decimal(dir: &Path, name: &str) -> io::Result<u16> {
    let text = attribute(dir, name)?;
    text.parse().map_err(|_| malformed(dir, name, &text))
}

fn hex4(dir: &Path, name: &str) -> io::Result<u16> {
    let text = attribute(dir, name)?;
    if text.len() != 4 {
        return Err(malformed(dir, name, &text));
    }
    u16::from_str_radix(&text, 16).map_err(|_| malformed(dir, name, &text))
}

fn optional(dir: &Path, name: &str) -> io::Result<Option<String>> {
    match attribute(dir, name) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A device directory as the kernel lays it out, under `root`.
    pub(crate) fn device_dir(root: &Path, port: &str, attrs: &[(&str, &str)]) -> PathBuf {
        let dir = root.join(port);
        std::fs::create_dir_all(&dir).expect("device dir");
        for (name, value) in attrs {
            std::fs::write(dir.join(name), format!("{value}\n")).expect("attribute");
        }
        dir
    }

    pub(crate) const FLEX: [(&str, &str); 6] = [
        ("busnum", "3"),
        ("devnum", "28"),
        ("idVendor", "2886"),
        ("idProduct", "0022"),
        ("bcdDevice", "0100"),
        ("serial", "EXAMPLE-SERIAL"),
    ];

    #[test]
    fn a_device_with_a_serial_number_reads_whole() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = device_dir(root.path(), "3-2.2", &FLEX);
        let device = UsbDevice::read(&dir).expect("read");
        assert_eq!(
            device,
            UsbDevice {
                dir: dir.clone(),
                port: "3-2.2".into(),
                bus: 3,
                address: 28,
                vendor: 0x2886,
                product: 0x0022,
                bcd_device: 0x0100,
                serial: Some("EXAMPLE-SERIAL".into()),
            }
        );
        assert_eq!(device.usb_id(), "2886:0022");
        assert_eq!(
            device.node_path(Path::new("/dev/bus/usb")),
            Path::new("/dev/bus/usb/003/028")
        );
        assert_eq!(
            device.to_string(),
            "2886:0022 at port 3-2.2 (bus 3 address 28)"
        );
    }

    #[test]
    fn a_device_without_a_serial_number_reads_as_none() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = device_dir(root.path(), "1-4", &FLEX[..5]);
        assert_eq!(UsbDevice::read(&dir).expect("read").serial, None);
    }

    #[test]
    fn a_malformed_devnum_is_an_error_naming_the_file() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut attrs = FLEX;
        attrs[1] = ("devnum", "twenty-eight");
        let dir = device_dir(root.path(), "3-2.2", &attrs);
        let err = UsbDevice::read(&dir).expect_err("malformed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("devnum"), "{err}");
        assert!(err.to_string().contains("twenty-eight"), "{err}");
    }

    #[test]
    fn a_missing_or_short_id_is_an_error_not_a_default() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = device_dir(root.path(), "3-2.2", &[FLEX[0], FLEX[1], FLEX[3], FLEX[4]]);
        let err = UsbDevice::read(&dir).expect_err("no idVendor");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(err.to_string().contains("idVendor"), "{err}");

        let mut attrs = FLEX;
        attrs[3] = ("idProduct", "22");
        let dir = device_dir(root.path(), "3-2.3", &attrs);
        let err = UsbDevice::read(&dir).expect_err("short id");
        assert!(err.to_string().contains("idProduct"), "{err}");
    }
}
