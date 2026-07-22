# Installation Guide — 伯楽 (Hakuraku) Telemetry System

This document provides a production-grade, step-by-step installation guide for setting up the Hakuraku central monitoring server and deploying the telemetry collection agent on target Linux hosts.

---

## Table of Contents
1. [Prerequisites & System Requirements](#1-prerequisites--system-requirements)
2. [Network Topology & Port Allocations](#2-network-topology--port-allocations)
3. [Domain Configuration & DNS](#3-domain-configuration--dns)
4. [Central Server Deployment (Step-by-Step)](#4-central-server-deployment-step-by-step)
5. [Telemetry Agent Deployment](#5-telemetry-agent-deployment)
   - [Method A: Docker-based Agent (Recommended)](#method-a-docker-based-agent-recommended)
   - [Method B: Systemd-based Native Agent](#method-b-systemd-based-native-agent)
6. [Security Hardening](#6-security-hardening)
7. [Post-Installation Verification](#7-post-installation-verification)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Prerequisites & System Requirements

### Central Server Node
* **Operating System**: Linux (Ubuntu 22.04 LTS+, Debian 12+, Rocky Linux 9+, or Alpine Linux 3.18+).
* **Hardware Requirements**: Minimum 1 vCPU, 1 GB RAM, 10 GB SSD.
* **Dependencies**:
  * Docker Engine (v24.0.0+)
  * Docker Compose (v2.20.0+)
  * Git

### Telemetry Target Node (Agent)
* **Operating System**: Linux kernel 5.4+ (required for stable `/proc` and `/sys` interfaces).
* **Hardware Overhead**: < 8 MB RAM, < 1% CPU utilization under normal operation.
* **Privileges**: root access (or custom system user with access to `/proc` and `/sys`).

---

## 2. Network Topology & Port Allocations

Ensure your firewall rules (e.g., UFW or security groups) are configured as follows:

| Port | Protocol | Source | Destination | Purpose |
|---|---|---|---|---|
| **80** | TCP | Any | Server | Caddy HTTP (Let's Encrypt validation & redirect) |
| **443** | TCP | Any | Server | Caddy HTTPS (Dashboard, REST API, WebSocket) |
| **50051** | TCP | Telemetry Agents | Server | gRPC Ingestion Port (Keep behind Caddy or open only to trusted IPs) |
| **3000** | TCP | Localhost | Server | Internal Axum HTTP Port (Exposed via reverse proxy) |

---

## 3. Domain Configuration & DNS

Hakuraku uses **Caddy** to automatically provision and renew TLS certificates via Let's Encrypt / ZeroSSL.

1. Point an `A` record for your subdomain (e.g., `monitor.example.com`) to the public IP address of your central server.
2. Verify DNS propagation:
   ```bash
   dig +short monitor.example.com
   ```

---

## 4. Central Server Deployment (Step-by-Step)

### Step 4.1: Clone the Codebase
SSH into your server and clone the repository:
```bash
git clone https://github.com/irvanmalik48/hakuraku.git /opt/hakuraku
cd /opt/hakuraku
```

### Step 4.2: Generate Secrets
Hakuraku uses HMAC-SHA256 request authentication. Generate a secure, cryptographically random 64-character hex secret:
```bash
openssl rand -hex 32
```
*Save this secret. It will be used by the server to verify agent signatures, and must be distributed to all agents.*

### Step 4.3: Configure Environment Variables
Create the production environment file:
```bash
cp .env.example .env
```

Edit `.env` using your preferred text editor (e.g., `nano` or `vim`) and update the values:
```env
# Central Authentication Secret (generated in Step 4.2)
PULSE_AUTH_SECRET=921b79ae40d21a2c3a5ef57ba619f... # Paste your hex string here

# Your DNS Domain (used by Caddy for automatic HTTPS)
PULSE_DOMAIN=monitor.example.com

# CORS Configuration (restricts dashboard HTTP origins)
PULSE_CORS_ALLOWED_ORIGINS=https://monitor.example.com

# Database Connection URI (PostgreSQL)
DATABASE_URL=postgres://pulse:secure_password_here@postgres:5432/pulse

# Internal Server Ports (Keep default)
PULSE_GRPC_PORT=50051
PULSE_HTTP_PORT=3000

# Optional: VictoriaMetrics Integration URL
# Uncomment to enable sub-second time-series metrics storage
# VICTORIAMETRICS_URL=http://victoriametrics:8428
```

### Step 4.4: Deploy Services
Run the Docker Compose stack. This will pull, build, and deploy the server, PostgreSQL, Caddy, and VictoriaMetrics:
```bash
docker compose -f docker-compose.yml up -d --build
```

Verify that all containers are healthy:
```bash
docker compose ps
```

---

## 5. Telemetry Agent Deployment

Run the agent on every host you wish to monitor.

### Method A: Docker-based Agent (Recommended)
This method isolates the agent binary, using read-only volumes to mount the host's performance interfaces.

1. Create a workspace directory on the target host:
   ```bash
   mkdir -p /opt/pulse-agent
   cd /opt/pulse-agent
   ```
2. Create a `docker-compose.yml` configuration:
   ```yaml
   version: '3.8'

   services:
     pulse-agent:
       image: ghcr.io/irvanmalik48/hakuraku-agent:latest
       container_name: pulse-agent
       restart: always
       read_only: true
       user: "1000"
       volumes:
         - /proc:/host/proc:ro
         - /sys:/host/sys:ro
       environment:
         - PULSE_AUTH_SECRET=your_auth_secret_here
         - PULSE_NODE_ID=target-node-01
         - PULSE_SERVER_ADDR=https://monitor.example.com
         - PULSE_INTERVAL_MS=1000
         - PROC_PATH=/host/proc
         - SYS_PATH=/host/sys
   ```
3. Start the container:
   ```bash
   docker compose up -d
   ```

### Method B: Systemd-based Native Agent
For environments without Docker, compile the static binary and run it via systemd.

1. **Build the static musl binary**:
   On a machine with Rust/Cargo installed:
   ```bash
   cargo build --release -p pulse-agent
   ```
   Copy the binary to the target host:
   ```bash
   scp target/release/pulse-agent root@target-host:/usr/local/bin/pulse-agent
   ```

2. **Configure environment credentials**:
   Create a secure configuration file `/etc/pulse-agent.env` on the target host:
   ```env
   PULSE_AUTH_SECRET=your_auth_secret_here
   PULSE_NODE_ID=target-node-01
   PULSE_SERVER_ADDR=https://monitor.example.com
   PULSE_INTERVAL_MS=1000
   ```
   Lock permissions to root only:
   ```bash
   chmod 600 /etc/pulse-agent.env
   ```

3. **Install the systemd Service**:
   Create a service unit file at `/etc/systemd/system/pulse-agent.service`:
   ```ini
   [Unit]
   Description=Hakuraku Telemetry Agent
   After=network.target

   [Service]
   Type=simple
   EnvironmentFile=/etc/pulse-agent.env
   ExecStart=/usr/local/bin/pulse-agent
   Restart=always
   RestartSec=5
   LimitNOFILE=65535

   # Security hardening
   ProtectSystem=full
   ProtectHome=true
   NoNewPrivileges=true

   [Install]
   WantedBy=multi-user.target
   ```

4. **Enable and Start the Agent**:
   ```bash
   systemctl daemon-reload
   systemctl enable --now pulse-agent
   ```

---

## 6. Security Hardening

To ensure your monitoring system is completely secure, implement the following steps:

### Firewall Configuration (UFW)
Only expose ports explicitly routed to Caddy. Block all external requests to PostgreSQL or directly to internal server ports:
```bash
# Allow SSH
ufw allow 22/tcp

# Allow HTTP and HTTPS
ufw allow 80/tcp
ufw allow 443/tcp

# Deny database and internal ports to public network
ufw deny 5432/tcp
ufw deny 3000/tcp

# Enable firewall
ufw enable
```

---

## 7. Post-Installation Verification

### Step 7.1: Verify Server Health
Query the public REST health check endpoint:
```bash
curl -I https://monitor.example.com/health
```
*Expected response: HTTP/2 200*

### Step 7.2: Verify Database Connectivity
Query the readiness health check endpoint:
```bash
curl https://monitor.example.com/readyz
```
*Expected response:*
```json
{"status":"ready","database":"connected"}
```

### Step 7.3: Verify Agent Connections
Watch the server logs to confirm telemetry streams are active and authentication is successful:
```bash
docker compose -f /opt/hakuraku/docker-compose.yml logs -f pulse-server
```
You should see stream connection entries:
```text
info: agent stream connected remote=192.168.1.5:41021
```

---

## 8. Troubleshooting

### Replay Attack Failures (Nonce Rejection)
If you see logs like `unauthenticated(replayed nonce)` or `expired timestamp`:
1. Check the system time on both the central server and the target agent nodes:
   ```bash
   timedatectl
   ```
2. Enable NTP sync on the target nodes to resolve clock drift:
   ```bash
   timedatectl set-ntp true
   ```

### gRPC Handshake Timeouts
If the agent is failing to connect with network errors:
1. Confirm the public address `PULSE_SERVER_ADDR` resolves to the server IP.
2. Verify Caddy logs for certificates errors:
   ```bash
   docker compose logs caddy
   ```
3. Test TCP reachability to port 443 from the agent:
   ```bash
   nc -z -v -w5 monitor.example.com 443
   ```
