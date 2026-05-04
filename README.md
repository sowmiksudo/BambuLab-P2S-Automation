# 🖨️ BambuLab P2S Air-Gapped Orchestrator

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)

A fully local, air-gapped orchestration gateway and headless spooler for Bambu Lab P2S 3D printers. 

This project bypasses the restrictions of region-locked (Chinese variant) firmwares, providing a zero-trust, cloud-free web dashboard for live telemetry, WebRTC camera streaming, and direct `.gcode.3mf` file execution via Implicit FTPS and MQTT.

## 🌊 The Community Gap (Why this exists)
Current open-source solutions for Bambu Lab printers either require heavy monolithic smart-home servers (like Home Assistant) or are raw Python libraries with no UI. Furthermore, standard network tools fail against Bambu's strict, undocumented hardware quirks. 

This project is a **Headless Spooler & API Gateway**. It allows anyone on the local network to drag-and-drop sliced files from any device straight to the printer, while simultaneously acting as a REST/WebSocket backend for automated print farms and robotics labs.

---

## ✨ Core Features
* **The Headless Spooler:** Drag and drop `.gcode.3mf` files directly into the web UI. The backend handles the complex Implicit FTPS upload and triggers the MQTT execution command automatically.
* **Real-Time Telemetry:** Bi-directional WebSockets stream live nozzle, bed, and chamber temperatures, plus print progress, directly to a dark-mode Tailwind UI.
* **Zero-Latency WebRTC Video:** Integrates `go2rtc` to convert the printer's encrypted RTSPS feed into a native, sub-second latency WebRTC stream.
* **Enterprise Hardware Bypasses:** Natively solves Bambu's firmware quirks, including:
  * Embedded C++ JSON parser whitespace crashing.
  * FTPS SNI (Server Name Indication) rejection on local IPs.
  * SSL Session ID Reuse enforcement on the FTP Data Channel (Error 10054).

---

## 🏗️ Architecture Stack
* **Backend:** Python + FastAPI + Uvicorn
* **Hardware Protocols:** `paho-mqtt` (v1 callback API), Custom `ImplicitFTP_TLS` subclass
* **Frontend:** Vanilla HTML/JS, Tailwind CSS (via CDN)
* **Video Microservice:** [go2rtc](https://github.com/AlexxIT/go2rtc)

---

## 🚀 Installation & Setup (Bare Metal MVP)

Currently, the MVP runs natively on your machine without Docker.

### 1. Printer Preparation
1. On your Bambu P2S touchscreen, navigate to Network Settings.
2. Enable **LAN Only Mode** (Note the IP address and Access Code).
3. Enable **Developer Mode** (Required to accept MQTT `write` commands like light toggles).

### 2. Environment Setup
Clone the repository and set up a Python virtual environment:
```bash
git clone [https://github.com/sowmiksudo/BambuLab-P2S-Automation.git](https://github.com/sowmiksudo/BambuLab-P2S-Automation.git)
cd BambuLab-P2S-Automation
```

Install the required dependencies:
```bash
pip install -r requirements.txt
```

### 3. Configuration
Create a `.env` file in the root directory and add your printer's credentials:
```env
PRINTER_IP=192.168.0.101
PRINTER_SERIAL=YOUR_PRINTER_SERIAL
ACCESS_CODE=YOUR_LAN_ACCESS_CODE
```

### 4. Running the Video Service (go2rtc)
1. Download the `go2rtc` executable for your OS from their [releases page](https://github.com/AlexxIT/go2rtc/releases).
2. Place the executable in the project root next to the `go2rtc.yaml` file.
3. Open a separate terminal window and run it:
   ```bash
   # Windows
   .\\go2rtc.exe
   ```

### 5. Running the Gateway
In your activated Python environment, start the FastAPI server:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open your browser and navigate to `http://localhost:8000` to access the dashboard!

---

## 🗺️ Roadmap
- [ ] Complete Dockerization (`docker-compose.yml`) for instant print-farm deployment.
- [ ] Add G-Code thumbnail extraction and display on the dashboard.
- [ ] Implement an AI/ML predictive maintenance layer based on time-series thermal data.

---

## 🤝 Sponsoring & Contributing
If this tool saved your print farm from cloud outages or region-lock headaches, consider supporting the development! 

Pull requests are welcome. For major architectural changes, please open an issue first to discuss what you would like to change.

Author: Shayer Mahmud Sowmik
Originally created for [Bot Engineers](https://github.com/bot-engineers), Bangladesh.
