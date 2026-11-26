//! Volumes of one file system for the tests of the file-system table: a loop device on Linux, a
//! disk image on macOS, and a virtual disk on Windows. Each volume has its own directory next to
//! the test binary, and its drop unmounts the volume and removes the directory. A volume needs
//! root on Linux and an administrator on Windows.

#[cfg(target_os = "linux")]
use alloc::borrow::ToOwned as _;
use alloc::string::String;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The bytes of a volume. The image files are sparse, so a volume uses only the blocks that a test
/// writes. XFS needs at least 300 MiB, and ReFS needs more than 1 GiB.
const SIZE: u64 = 2 << 30;

/// A file system of the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileSystem {
    #[cfg(target_os = "linux")]
    Ext4,
    #[cfg(target_os = "linux")]
    Xfs,
    #[cfg(target_os = "macos")]
    Apfs,
    #[cfg(target_os = "macos")]
    Hfs,
    #[cfg(target_os = "windows")]
    Ntfs,
    #[cfg(target_os = "windows")]
    Refs,
    ExFat,
}

impl FileSystem {
    const fn name(self) -> &'static str {
        match self {
            #[cfg(target_os = "linux")]
            Self::Ext4 => "ext4",
            #[cfg(target_os = "linux")]
            Self::Xfs => "xfs",
            #[cfg(target_os = "macos")]
            Self::Apfs => "apfs",
            #[cfg(target_os = "macos")]
            Self::Hfs => "hfs",
            #[cfg(target_os = "windows")]
            Self::Ntfs => "ntfs",
            #[cfg(target_os = "windows")]
            Self::Refs => "refs",
            Self::ExFat => "exfat",
        }
    }
}

/// A mounted volume of one file system.
pub(super) struct Volume {
    #[cfg(target_os = "linux")]
    file_system: FileSystem,
    /// The directory of the volume: its image and its mount point.
    base: PathBuf,
    mount: PathBuf,
    /// The loop device of the image.
    #[cfg(target_os = "linux")]
    device: String,
}

impl Volume {
    /// The mount point of the volume.
    pub(super) fn path(&self) -> &Path {
        &self.mount
    }

    /// A new directory next to the test binary for a volume of `file_system`, with an empty mount
    /// point in it.
    fn base(file_system: FileSystem) -> (PathBuf, PathBuf) {
        let mut base = std::env::current_exe().expect("the test knows its binary");
        base.pop();
        base.push(alloc::format!(
            "rsasync-volume-{}-{}",
            file_system.name(),
            std::process::id()
        ));
        drop(fs::remove_dir_all(&base));
        let mount = base.join("mount");
        fs::create_dir_all(&mount).expect("the test makes the directory of its volume");
        (base, mount)
    }
}

/// Runs `program` with `args`, and gives its standard output. A failed run fails the test with
/// its output.
fn run<I: IntoIterator<Item = S>, S: AsRef<OsStr>>(program: &str, args: I) -> String {
    let output = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("{program} does not start: {error}"));
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    assert!(
        output.status.success(),
        "{program} gave {}: {}{}",
        output.status,
        text(&output.stdout),
        text(&output.stderr)
    );
    text(&output.stdout)
}

#[cfg(target_os = "linux")]
impl Volume {
    /// A new volume of `file_system` on a loop device. ext4 commits its journal only each 600 s,
    /// so only a sync commits it while a test runs.
    pub(super) fn new(file_system: FileSystem) -> Self {
        let (base, mount) = Self::base(file_system);
        let image = base.join("image");
        fs::File::create(&image)
            .and_then(|file| file.set_len(SIZE))
            .expect("the test makes the image of its volume");
        let device = run("losetup", [OsStr::new("--find"), "--show".as_ref(), image.as_ref()])
            .trim()
            .to_owned();
        match file_system {
            FileSystem::Ext4 => run("mkfs.ext4", ["-q", "-F", &device]),
            FileSystem::Xfs => run("mkfs.xfs", ["-q", "-f", &device]),
            FileSystem::ExFat => run("mkfs.exfat", [&device]),
        };
        let volume = Self {
            file_system,
            base,
            mount,
            device,
        };
        volume.mount();
        volume
    }

    /// Mounts the device of the volume at its mount point.
    pub(super) fn mount(&self) {
        let options = match self.file_system {
            FileSystem::Ext4 => "commit=600",
            FileSystem::Xfs | FileSystem::ExFat => "defaults",
        };
        run(
            "mount",
            [
                OsStr::new("-o"),
                options.as_ref(),
                self.device.as_ref(),
                self.mount.as_ref(),
            ],
        );
    }

    /// Unmounts the volume.
    pub(super) fn unmount(&self) {
        run("umount", [&self.mount]);
    }
}

#[cfg(target_os = "linux")]
impl Drop for Volume {
    fn drop(&mut self) {
        drop(Command::new("umount").arg(&self.mount).output());
        drop(Command::new("losetup").args(["-d", &self.device]).output());
        drop(fs::remove_dir_all(&self.base));
    }
}

#[cfg(target_os = "macos")]
impl Volume {
    /// A new volume of `file_system` in a disk image, which `hdiutil` makes and attaches with no
    /// root and out of the view of the Finder.
    pub(super) fn new(file_system: FileSystem) -> Self {
        let (base, mount) = Self::base(file_system);
        let image = base.join("image.dmg");
        let format = match file_system {
            FileSystem::Apfs => "APFS",
            FileSystem::Hfs => "HFS+J",
            FileSystem::ExFat => "ExFAT",
        };
        let size = alloc::format!("{}m", SIZE >> 20);
        run(
            "hdiutil",
            [
                OsStr::new("create"),
                "-quiet".as_ref(),
                "-size".as_ref(),
                size.as_ref(),
                "-fs".as_ref(),
                format.as_ref(),
                "-volname".as_ref(),
                "rsasync".as_ref(),
                image.as_ref(),
            ],
        );
        run(
            "hdiutil",
            [
                OsStr::new("attach"),
                "-quiet".as_ref(),
                "-nobrowse".as_ref(),
                "-mountpoint".as_ref(),
                mount.as_ref(),
                image.as_ref(),
            ],
        );
        Self { base, mount }
    }
}

#[cfg(target_os = "macos")]
impl Drop for Volume {
    fn drop(&mut self) {
        drop(
            Command::new("hdiutil")
                .args([
                    OsStr::new("detach"),
                    "-quiet".as_ref(),
                    "-force".as_ref(),
                    self.mount.as_ref(),
                ])
                .output(),
        );
        drop(fs::remove_dir_all(&self.base));
    }
}

#[cfg(target_os = "windows")]
impl Volume {
    /// A new volume of `file_system` on a virtual disk, which `diskpart` makes, attaches, formats,
    /// and mounts at the mount point.
    pub(super) fn new(file_system: FileSystem) -> Self {
        let (base, mount) = Self::base(file_system);
        let image = base.join("image.vhdx");
        let script = alloc::format!(
            "create vdisk file=\"{image}\" maximum={size} type=expandable\nselect vdisk \
             file=\"{image}\"\nattach vdisk\ncreate partition primary\nformat fs={fs} quick \
             label=rsasync\nassign mount=\"{mount}\"\n",
            image = image.display(),
            size = SIZE >> 20,
            fs = file_system.name(),
            mount = mount.display(),
        );
        diskpart(&base, &script);
        Self { base, mount }
    }
}

/// Runs the `diskpart` script `script` from a file in `base`.
#[cfg(target_os = "windows")]
fn diskpart(base: &Path, script: &str) -> String {
    let path = base.join("diskpart.txt");
    fs::write(&path, script).expect("the test writes its diskpart script");
    run("diskpart", [OsStr::new("/s"), path.as_ref()])
}

#[cfg(target_os = "windows")]
impl Drop for Volume {
    fn drop(&mut self) {
        let image = self.base.join("image.vhdx");
        let script = alloc::format!("select vdisk file=\"{image}\"\ndetach vdisk\n", image = image.display());
        let path = self.base.join("diskpart.txt");
        if fs::write(&path, script).is_ok() {
            drop(Command::new("diskpart").arg("/s").arg(&path).output());
        }
        drop(fs::remove_dir_all(&self.base));
    }
}
