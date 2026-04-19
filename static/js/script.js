const socket = io();

// UI Elements
const statusBadge = document.getElementById('printer-status');
const nozzleTemp = document.getElementById('nozzle-temp');
const bedTemp = document.getElementById('bed-temp');
const chamberTemp = document.getElementById('chamber-temp');
const progressBar = document.getElementById('progress-bar');
const progressText = document.getElementById('progress-text');

// Handle incoming socket messages
socket.on('printer_update', (data) => {
    // Update status
    statusBadge.textContent = data.status || 'Unknown';
    if(data.status && data.status.toLowerCase() === 'printing') {
        statusBadge.style.color = '#00E676';
        statusBadge.style.background = 'rgba(0, 200, 83, 0.2)';
        statusBadge.style.borderColor = 'rgba(0, 200, 83, 0.3)';
    } else {
        statusBadge.style.color = '#aaa';
        statusBadge.style.background = 'rgba(255, 255, 255, 0.1)';
        statusBadge.style.borderColor = 'rgba(255, 255, 255, 0.1)';
    }

    // Update Metrics
    if(data.nozzle_temp !== undefined) nozzleTemp.textContent = Math.round(data.nozzle_temp);
    if(data.bed_temp !== undefined) bedTemp.textContent = Math.round(data.bed_temp);
    if(data.chamber_temp !== undefined) chamberTemp.textContent = Math.round(data.chamber_temp);

    // Update Progress
    if(data.progress !== undefined) {
        const perc = parseInt(data.progress);
        progressBar.style.width = `${perc}%`;
        progressText.textContent = `${perc}%`;
    }
});

function toggleLight(action) {
    fetch(`/api/light/${action}`, {
        method: 'POST'
    }).then(res => res.json())
      .then(data => console.log(`Light ${action} success`, data))
      .catch(err => console.error(err));
}
