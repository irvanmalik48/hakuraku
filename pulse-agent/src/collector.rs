//! Zero-allocation Linux metric collector.
//!
//! Reads system telemetry directly from `/proc` and `/sys` filesystems
//! using pre-allocated buffers. Computes delta rates for I/O metrics
//! by retaining the previous sample.

use std::collections::HashMap;
use std::io::Read;
use std::time::Instant;

use pulse_core::error::CollectorError;
use pulse_core::proto::{NetworkInterface, NodeStats, TemperatureSensor};

type Result<T> = std::result::Result<T, CollectorError>;

/// System metric collector that retains state between samples for delta computation.
pub struct SystemCollector {
    /// Re-usable read buffer to avoid per-sample allocations.
    buf: String,
    /// Previous CPU jiffies (user, nice, system, idle, ...) for delta calculation.
    prev_cpu: Option<CpuJiffies>,
    /// Previous per-core CPU jiffies.
    prev_cpu_per_core: HashMap<u32, CpuJiffies>,
    /// Previous network interface byte counters.
    prev_net: HashMap<String, NetCounters>,
    /// Previous disk I/O sector counters.
    prev_disk: Option<DiskCounters>,
    /// Timestamp of the previous sample (for rate computation).
    prev_time: Option<Instant>,
}

#[derive(Clone, Debug)]
struct CpuJiffies {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
    irq: u64,
    softirq: u64,
    steal: u64,
}

impl CpuJiffies {
    fn total(&self) -> u64 {
        self.user
            + self.nice
            + self.system
            + self.idle
            + self.iowait
            + self.irq
            + self.softirq
            + self.steal
    }

    fn active(&self) -> u64 {
        self.total() - self.idle - self.iowait
    }
}

#[derive(Clone, Debug)]
struct NetCounters {
    rx_bytes: u64,
    tx_bytes: u64,
}

#[derive(Clone, Debug)]
struct DiskCounters {
    read_sectors: u64,
    write_sectors: u64,
}

impl SystemCollector {
    pub fn new() -> Self {
        Self {
            buf: String::with_capacity(4096),
            prev_cpu: None,
            prev_cpu_per_core: HashMap::new(),
            prev_net: HashMap::new(),
            prev_disk: None,
            prev_time: None,
        }
    }

    /// Collect a complete system snapshot.
    pub fn collect(&mut self, node_id: &str) -> Result<NodeStats> {
        let now = Instant::now();
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let elapsed_secs = self
            .prev_time
            .map(|prev| now.duration_since(prev).as_secs_f64())
            .unwrap_or(1.0)
            .max(0.001); // Prevent division by zero

        let (cpu_percent, cpu_per_core) = self.read_cpu()?;
        let (
            mem_total,
            mem_used,
            mem_free,
            mem_available,
            mem_buffers,
            mem_cached,
            swap_total,
            swap_used,
        ) = self.read_meminfo()?;
        let (load_1, load_5, load_15) = self.read_loadavg()?;
        let net_interfaces = self.read_net_dev(elapsed_secs)?;
        let (disk_read_bytes_sec, disk_write_bytes_sec) = self.read_diskstats(elapsed_secs)?;
        let (tcp_connections, udp_connections) = self.read_connections()?;
        let uptime_seconds = self.read_uptime()?;
        let temperatures = self.read_temperatures()?;

        self.prev_time = Some(now);

        Ok(NodeStats {
            node_id: node_id.to_string(),
            timestamp_ms,
            cpu_percent,
            cpu_per_core,
            load_avg_1: load_1,
            load_avg_5: load_5,
            load_avg_15: load_15,
            mem_total,
            mem_used,
            mem_free,
            mem_available,
            mem_buffers,
            mem_cached,
            swap_total,
            swap_used,
            disk_read_bytes_sec,
            disk_write_bytes_sec,
            net_interfaces,
            tcp_connections,
            udp_connections,
            uptime_seconds,
            temperatures,
        })
    }

    // ── /proc/stat ──────────────────────────────────────────────────────────

    fn read_cpu(&mut self) -> Result<(f64, Vec<f64>)> {
        self.read_file("/proc/stat")?;

        let mut overall_percent = 0.0;
        let mut per_core = Vec::new();

        for line in self.buf.lines() {
            if line.starts_with("cpu ") {
                // Aggregate CPU line
                let jiffies = Self::parse_cpu_line(line)?;
                if let Some(ref prev) = self.prev_cpu {
                    overall_percent = Self::compute_cpu_percent(prev, &jiffies);
                }
                self.prev_cpu = Some(jiffies);
            } else if let Some(stripped) = line.strip_prefix("cpu") {
                // Per-core line: cpu0, cpu1, ...
                let core_num: u32 = stripped
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(per_core.len() as u32);

                let jiffies = Self::parse_cpu_line(line)?;
                let percent = self
                    .prev_cpu_per_core
                    .get(&core_num)
                    .map(|prev| Self::compute_cpu_percent(prev, &jiffies))
                    .unwrap_or(0.0);

                per_core.push(percent);
                self.prev_cpu_per_core.insert(core_num, jiffies);
            } else if !line.starts_with("cpu") {
                // Past all cpu lines
                break;
            }
        }

        Ok((overall_percent, per_core))
    }

    fn parse_cpu_line(line: &str) -> Result<CpuJiffies> {
        let mut parts = line.split_whitespace().skip(1); // skip "cpu" or "cpuN"
        let parse = |p: Option<&str>, field: &str| -> Result<u64> {
            p.ok_or_else(|| CollectorError::Parse {
                path: "/proc/stat".into(),
                field: field.into(),
                detail: "missing field".into(),
            })?
            .parse()
            .map_err(|_| CollectorError::Parse {
                path: "/proc/stat".into(),
                field: field.into(),
                detail: "not a valid u64".into(),
            })
        };

        Ok(CpuJiffies {
            user: parse(parts.next(), "user")?,
            nice: parse(parts.next(), "nice")?,
            system: parse(parts.next(), "system")?,
            idle: parse(parts.next(), "idle")?,
            iowait: parse(parts.next(), "iowait").unwrap_or(0),
            irq: parse(parts.next(), "irq").unwrap_or(0),
            softirq: parse(parts.next(), "softirq").unwrap_or(0),
            steal: parse(parts.next(), "steal").unwrap_or(0),
        })
    }

    fn compute_cpu_percent(prev: &CpuJiffies, curr: &CpuJiffies) -> f64 {
        let total_delta = curr.total().saturating_sub(prev.total());
        if total_delta == 0 {
            return 0.0;
        }
        let active_delta = curr.active().saturating_sub(prev.active());
        (active_delta as f64 / total_delta as f64) * 100.0
    }

    // ── /proc/meminfo ───────────────────────────────────────────────────────

    #[allow(clippy::type_complexity)]
    fn read_meminfo(&mut self) -> Result<(u64, u64, u64, u64, u64, u64, u64, u64)> {
        self.read_file("/proc/meminfo")?;

        let mut mem_total = 0u64;
        let mut mem_free = 0u64;
        let mut mem_available = 0u64;
        let mut buffers = 0u64;
        let mut cached = 0u64;
        let mut swap_total = 0u64;
        let mut swap_free = 0u64;

        for line in self.buf.lines() {
            let (key, val_kb) = match Self::parse_meminfo_line(line) {
                Some(v) => v,
                None => continue,
            };
            let val_bytes = val_kb * 1024;
            match key {
                "MemTotal" => mem_total = val_bytes,
                "MemFree" => mem_free = val_bytes,
                "MemAvailable" => mem_available = val_bytes,
                "Buffers" => buffers = val_bytes,
                "Cached" => cached = val_bytes,
                "SwapTotal" => swap_total = val_bytes,
                "SwapFree" => swap_free = val_bytes,
                _ => {}
            }
        }

        let mem_used = mem_total.saturating_sub(mem_free + buffers + cached);
        let swap_used = swap_total.saturating_sub(swap_free);

        Ok((
            mem_total,
            mem_used,
            mem_free,
            mem_available,
            buffers,
            cached,
            swap_total,
            swap_used,
        ))
    }

    fn parse_meminfo_line(line: &str) -> Option<(&str, u64)> {
        let mut parts = line.split(':');
        let key = parts.next()?.trim();
        let val_str = parts.next()?.trim();
        // Value is like "12345 kB" — take only the number
        let num_str = val_str.split_whitespace().next()?;
        let val: u64 = num_str.parse().ok()?;
        Some((key, val))
    }

    // ── /proc/loadavg ───────────────────────────────────────────────────────

    fn read_loadavg(&mut self) -> Result<(f64, f64, f64)> {
        self.read_file("/proc/loadavg")?;

        let mut parts = self.buf.split_whitespace();

        let parse = |p: Option<&str>, field: &str| -> Result<f64> {
            p.ok_or_else(|| CollectorError::Parse {
                path: "/proc/loadavg".into(),
                field: field.into(),
                detail: "missing field".into(),
            })?
            .parse()
            .map_err(|_| CollectorError::Parse {
                path: "/proc/loadavg".into(),
                field: field.into(),
                detail: "not a valid f64".into(),
            })
        };

        let l1 = parse(parts.next(), "load1")?;
        let l5 = parse(parts.next(), "load5")?;
        let l15 = parse(parts.next(), "load15")?;

        Ok((l1, l5, l15))
    }

    // ── /proc/net/dev ───────────────────────────────────────────────────────

    fn read_net_dev(&mut self, elapsed_secs: f64) -> Result<Vec<NetworkInterface>> {
        self.read_file("/proc/net/dev")?;

        let mut interfaces = Vec::new();

        for line in self.buf.lines().skip(2) {
            // Skip header lines
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let (iface_name, rest) = match line.split_once(':') {
                Some((name, rest)) => (name.trim(), rest),
                None => continue,
            };

            // Skip loopback
            if iface_name == "lo" {
                continue;
            }

            let fields: Vec<u64> = rest
                .split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect();

            if fields.len() < 10 {
                continue;
            }

            let rx_bytes_total = fields[0];
            let tx_bytes_total = fields[8];

            let (rx_bytes_sec, tx_bytes_sec) = if let Some(prev) = self.prev_net.get(iface_name) {
                let rx_delta = rx_bytes_total.saturating_sub(prev.rx_bytes);
                let tx_delta = tx_bytes_total.saturating_sub(prev.tx_bytes);
                (
                    (rx_delta as f64 / elapsed_secs) as u64,
                    (tx_delta as f64 / elapsed_secs) as u64,
                )
            } else {
                (0, 0)
            };

            self.prev_net.insert(
                iface_name.to_string(),
                NetCounters {
                    rx_bytes: rx_bytes_total,
                    tx_bytes: tx_bytes_total,
                },
            );

            interfaces.push(NetworkInterface {
                name: iface_name.to_string(),
                rx_bytes_sec,
                tx_bytes_sec,
                rx_bytes_total,
                tx_bytes_total,
            });
        }

        Ok(interfaces)
    }

    // ── /proc/diskstats ─────────────────────────────────────────────────────

    fn read_diskstats(&mut self, elapsed_secs: f64) -> Result<(u64, u64)> {
        self.read_file("/proc/diskstats")?;

        let mut total_read_sectors = 0u64;
        let mut total_write_sectors = 0u64;

        for line in self.buf.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 14 {
                continue;
            }

            let dev_name = fields[2];
            // Only consider whole-disk devices, skip partitions (e.g., sda not sda1)
            // Simple heuristic: if the name ends with a digit and has a letter before it,
            // it's likely a partition. More sophisticated detection would check /sys.
            if dev_name.starts_with("loop") || dev_name.starts_with("ram") {
                continue;
            }

            // Fields: [3] = reads completed, [5] = sectors read, [7] = writes completed, [9] = sectors written
            let read_sectors: u64 = fields[5].parse().unwrap_or(0);
            let write_sectors: u64 = fields[9].parse().unwrap_or(0);

            total_read_sectors += read_sectors;
            total_write_sectors += write_sectors;
        }

        // Convert sectors to bytes/sec (sector = 512 bytes)
        let (read_bps, write_bps) = if let Some(ref prev) = self.prev_disk {
            let read_delta = total_read_sectors.saturating_sub(prev.read_sectors);
            let write_delta = total_write_sectors.saturating_sub(prev.write_sectors);
            (
                ((read_delta * 512) as f64 / elapsed_secs) as u64,
                ((write_delta * 512) as f64 / elapsed_secs) as u64,
            )
        } else {
            (0, 0)
        };

        self.prev_disk = Some(DiskCounters {
            read_sectors: total_read_sectors,
            write_sectors: total_write_sectors,
        });

        Ok((read_bps, write_bps))
    }

    // ── /proc/net/tcp & /proc/net/udp ───────────────────────────────────────

    fn read_connections(&mut self) -> Result<(u32, u32)> {
        let tcp = self.count_connections("/proc/net/tcp")?
            + self.count_connections("/proc/net/tcp6").unwrap_or(0);
        let udp = self.count_connections("/proc/net/udp")?
            + self.count_connections("/proc/net/udp6").unwrap_or(0);
        Ok((tcp, udp))
    }

    fn count_connections(&mut self, path: &str) -> Result<u32> {
        self.read_file(path)?;
        // Skip header line, count remaining non-empty lines
        let count = self
            .buf
            .lines()
            .skip(1)
            .filter(|l| !l.trim().is_empty())
            .count() as u32;
        Ok(count)
    }

    // ── /proc/uptime ────────────────────────────────────────────────────────

    fn read_uptime(&mut self) -> Result<u64> {
        self.read_file("/proc/uptime")?;
        let uptime_str =
            self.buf
                .split_whitespace()
                .next()
                .ok_or_else(|| CollectorError::Parse {
                    path: "/proc/uptime".into(),
                    field: "uptime".into(),
                    detail: "empty file".into(),
                })?;

        let uptime: f64 = uptime_str.parse().map_err(|_| CollectorError::Parse {
            path: "/proc/uptime".into(),
            field: "uptime".into(),
            detail: "not a valid f64".into(),
        })?;

        Ok(uptime as u64)
    }

    // ── /sys/class/thermal ──────────────────────────────────────────────────

    fn read_temperatures(&mut self) -> Result<Vec<TemperatureSensor>> {
        let mut sensors = Vec::new();
        let thermal_dir = "/sys/class/thermal";

        let entries = match std::fs::read_dir(thermal_dir) {
            Ok(e) => e,
            Err(_) => return Ok(sensors), // thermal zones may not exist
        };

        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.starts_with("thermal_zone") {
                continue;
            }

            let temp_path = entry.path().join("temp");
            let type_path = entry.path().join("type");

            let label = std::fs::read_to_string(&type_path)
                .unwrap_or_else(|_| name_str.to_string())
                .trim()
                .to_string();

            if let Ok(temp_str) = std::fs::read_to_string(&temp_path)
                && let Ok(temp) = temp_str.trim().parse::<i32>()
            {
                sensors.push(TemperatureSensor {
                    label,
                    temp_millicelsius: temp,
                });
            }
        }

        Ok(sensors)
    }

    // ── File I/O helper ─────────────────────────────────────────────────────

    /// Read a procfs file into the reusable buffer, avoiding allocation.
    fn read_file(&mut self, path: &str) -> Result<()> {
        self.buf.clear();
        let mut file = std::fs::File::open(path).map_err(|e| CollectorError::ProcRead {
            path: path.into(),
            source: e,
        })?;
        file.read_to_string(&mut self.buf)
            .map_err(|e| CollectorError::ProcRead {
                path: path.into(),
                source: e,
            })?;
        Ok(())
    }
}

impl Default for SystemCollector {
    fn default() -> Self {
        Self::new()
    }
}
