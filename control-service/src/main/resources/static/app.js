(() => {
    const MAX_FEED_ROWS = 500;
    const RATE_HISTORY_LEN = 60;

    const statusBadge = document.getElementById('status-badge');
    const statusText = document.getElementById('status-text');
    const startBtn = document.getElementById('start-btn');
    const stopBtn = document.getElementById('stop-btn');
    const clearBtn = document.getElementById('clear-btn');
    const saveBtn = document.getElementById('save-config-btn');
    const saveStatus = document.getElementById('save-status');
    const feedBody = document.getElementById('feed-body');
    const interrogateOverlay = document.getElementById('interrogate-overlay');
    const interrogateBody = document.getElementById('interrogate-body');
    const interrogateCloseBtn = document.getElementById('interrogate-close-btn');
    const pauseInterrogateBtn = document.getElementById('pause-interrogate-btn');

    const rateHistory = [];
    let generationPaused = false;

    const TILE_FONT_MAX = 24;
    const TILE_FONT_MIN = 12;
    const META_FONT_MAX = 16;
    const META_FONT_MIN = 11;

    // Sets text on a dashboard box and shrinks its font until the text fits, so long numbers
    // (e.g. total volume as it grows) never overflow the box.
    function setFittedText(el, text, maxFont, minFont) {
        el.textContent = text;
        el.style.fontSize = maxFont + 'px';
        let size = maxFont;
        while (el.scrollWidth > el.clientWidth && size > minFont) {
            size -= 1;
            el.style.fontSize = size + 'px';
        }
    }

    // --- Tabs ---
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
            btn.classList.add('active');
            document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
        });
    });

    // --- Status polling ---
    // Config inputs are only ever written from the server on initial load and right after a
    // Start/Save response (so the UI can snap to server-clamped values). The periodic status poll
    // must NOT touch them, or it would overwrite whatever the user is currently typing every few
    // seconds with whatever config is still saved server-side.
    function applyConfigToForm(config) {
        document.getElementById('cfg-max-accounts').value = config.maxAccounts;
        document.getElementById('cfg-checkpoint').value = config.checkpointIntervalMs;
        document.getElementById('cfg-state-bloat').checked = config.stateBloat;
        document.getElementById('cfg-webhook-enabled').checked = config.webhookEnabled;
        document.getElementById('cfg-webhook-url').value = config.webhookUrl;
        document.getElementById('cfg-rate-limit').value = config.maxTransactionsPerSecond;
        document.getElementById('cfg-task-memory').value = config.taskManagerMemoryMb;
        document.getElementById('cfg-parallelism').value = config.parallelism;
        document.getElementById('cfg-checkpointing-mode').value = config.checkpointingMode;
        document.getElementById('cfg-restart-attempts').value = config.restartAttempts;
        document.getElementById('cfg-restart-delay').value = config.restartDelaySeconds;
    }

    function collectConfigFromForm() {
        return {
            maxAccounts: parseInt(document.getElementById('cfg-max-accounts').value, 10) || 4000,
            checkpointIntervalMs: parseInt(document.getElementById('cfg-checkpoint').value, 10) || 60000,
            stateBloat: document.getElementById('cfg-state-bloat').checked,
            webhookEnabled: document.getElementById('cfg-webhook-enabled').checked,
            webhookUrl: document.getElementById('cfg-webhook-url').value,
            maxTransactionsPerSecond: parseInt(document.getElementById('cfg-rate-limit').value, 10) || 1000,
            taskManagerMemoryMb: parseInt(document.getElementById('cfg-task-memory').value, 10) || 512,
            parallelism: parseInt(document.getElementById('cfg-parallelism').value, 10) || 2,
            checkpointingMode: document.getElementById('cfg-checkpointing-mode').value,
            restartAttempts: parseInt(document.getElementById('cfg-restart-attempts').value, 10) || 0,
            restartDelaySeconds: parseInt(document.getElementById('cfg-restart-delay').value, 10) || 10
        };
    }

    function applyStatus(data, opts) {
        opts = opts || {};
        statusBadge.className = 'badge ' + data.status;
        statusText.textContent = data.status;
        const running = data.status === 'RUNNING';
        const transitioning = data.status === 'STARTING' || data.status === 'STOPPING';
        startBtn.disabled = running || transitioning;
        stopBtn.disabled = !running || transitioning;
        clearBtn.disabled = transitioning;
        saveBtn.disabled = running || transitioning;
        pauseInterrogateBtn.disabled = !running || transitioning;
        setGenerationPaused(running && !!data.generationPaused);

        setFittedText(document.getElementById('meta-partition-key'), data.partitionKey || '—', META_FONT_MAX, META_FONT_MIN);
        setFittedText(document.getElementById('meta-parallelism'), data.parallelism != null ? String(data.parallelism) : '—', META_FONT_MAX, META_FONT_MIN);
        setFittedText(document.getElementById('meta-flink-parallelism'), data.parallelism != null ? String(data.parallelism) : '—', META_FONT_MAX, META_FONT_MIN);

        if (data.config) {
            setFittedText(document.getElementById('meta-max-accounts'), data.config.maxAccounts.toLocaleString(), META_FONT_MAX, META_FONT_MIN);
            if (opts.syncForm) {
                applyConfigToForm(data.config);
            }
        }

        // A Stop (or a fresh Start/Clear Data) cancels the whole pipeline, which makes any open
        // interrogation stale; drop it rather than leave the feed looking paused forever.
        if (!running && !interrogateOverlay.classList.contains('hidden')) {
            interrogateOverlay.classList.add('hidden');
            interrogateBody.innerHTML = '';
        }
    }

    async function refreshStatus(opts) {
        try {
            const res = await fetch('/api/job/status');
            applyStatus(await res.json(), opts);
        } catch (e) {
            // server may be restarting; ignore transient failures
        }
    }

    startBtn.addEventListener('click', async () => {
        const config = collectConfigFromForm();
        startBtn.disabled = true;
        const res = await fetch('/api/job/start', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        applyStatus(await res.json(), {syncForm: true});
    });

    stopBtn.addEventListener('click', async () => {
        stopBtn.disabled = true;
        const res = await fetch('/api/job/stop', {method: 'POST'});
        applyStatus(await res.json());
    });

    saveBtn.addEventListener('click', async () => {
        const config = collectConfigFromForm();
        saveBtn.disabled = true;
        saveStatus.textContent = '';
        saveStatus.className = 'hint save-status';
        try {
            const res = await fetch('/api/job/config', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                throw new Error(err.error || ('HTTP ' + res.status));
            }
            applyStatus(await res.json(), {syncForm: true});
            saveStatus.textContent = 'Saved.';
            saveStatus.classList.add('ok');
        } catch (e) {
            saveStatus.textContent = 'Save failed: ' + e.message;
            saveStatus.classList.add('error');
        }
        setTimeout(() => { saveStatus.textContent = ''; saveStatus.className = 'hint save-status'; }, 4000);
    });

    clearBtn.addEventListener('click', async () => {
        const running = statusBadge.classList.contains('RUNNING');
        const message = running
            ? 'Clear all data? This restarts the job and resets every account to a zero balance.'
            : 'Clear all data? This resets every account and the live feed/stats.';
        if (!window.confirm(message)) return;

        clearBtn.disabled = true;
        resetLocalUiState();
        const res = await fetch('/api/job/clear-data', {method: 'POST'});
        applyStatus(await res.json());
    });

    function resetLocalUiState() {
        feedBody.innerHTML = '';
        rateHistory.length = 0;
        refreshStats();
    }

    // --- Live feed via WebSocket ---
    function connectWebSocket() {
        const proto = location.protocol === 'https:' ? 'wss' : 'ws';
        const ws = new WebSocket(proto + '://' + location.host + '/ws/transactions');
        ws.onmessage = (event) => {
            const tx = JSON.parse(event.data);
            addFeedRow(tx);
        };
        ws.onclose = () => setTimeout(connectWebSocket, 2000);
        ws.onerror = () => ws.close();
    }

    function addFeedRow(tx) {
        const row = document.createElement('tr');
        row.className = tx.processingStatus;
        const time = new Date(tx.timestamp).toLocaleTimeString();
        row.innerHTML =
            '<td>' + time + '</td>' +
            '<td>' + tx.accountId + '</td>' +
            '<td class="mono">' + tx.amount.toFixed(2) + '</td>' +
            '<td>' + tx.processingStatus + '</td>' +
            '<td class="mono">' + tx.newBalance.toFixed(2) + '</td>' +
            '<td><button class="small interrogate-btn' + (generationPaused ? '' : ' hidden') + '" data-tx-id="' + tx.transactionId + '">Interrogate</button></td>';
        feedBody.insertBefore(row, feedBody.firstChild);
        while (feedBody.rows.length > MAX_FEED_ROWS) {
            feedBody.deleteRow(feedBody.rows.length - 1);
        }
    }

    // --- Interrogate mode ---
    // Reflects the current pause state into the toolbar toggle and every row's Interrogate button.
    // Rows only offer Interrogate once generation is actually paused, so "pause first, then drill
    // into whichever rows you like" is the only path through this UI.
    function setGenerationPaused(paused) {
        generationPaused = paused;
        pauseInterrogateBtn.textContent = paused ? 'Resume Generation' : 'Pause for Interrogation';
        pauseInterrogateBtn.classList.toggle('primary', !paused);
        pauseInterrogateBtn.classList.toggle('danger', paused);
        feedBody.querySelectorAll('.interrogate-btn').forEach(btn => btn.classList.toggle('hidden', !paused));
    }

    pauseInterrogateBtn.addEventListener('click', async () => {
        pauseInterrogateBtn.disabled = true;
        try {
            const url = generationPaused ? '/api/interrogate/resume' : '/api/interrogate/pause';
            const res = await fetch(url, {method: 'POST'});
            const data = await res.json();
            if (!res.ok) {
                throw new Error(data.error || ('HTTP ' + res.status));
            }
            setGenerationPaused(data.paused);
            if (!data.paused) {
                interrogateOverlay.classList.add('hidden');
                interrogateBody.innerHTML = '';
            }
        } catch (e) {
            alert('Could not change pause state: ' + e.message);
        } finally {
            pauseInterrogateBtn.disabled = false;
        }
    });

    feedBody.addEventListener('click', async (event) => {
        const btn = event.target.closest('.interrogate-btn');
        if (!btn) return;
        btn.disabled = true;
        try {
            const res = await fetch('/api/interrogate/' + btn.dataset.txId, {method: 'POST'});
            const data = await res.json();
            if (!res.ok) {
                throw new Error(data.error || ('HTTP ' + res.status));
            }
            setGenerationPaused(true);
            renderInterrogationReport(data);
            interrogateOverlay.classList.remove('hidden');
        } catch (e) {
            alert('Could not interrogate transaction: ' + e.message);
        } finally {
            btn.disabled = false;
        }
    });

    function renderInterrogationReport(report) {
        const tx = report.transaction;
        const time = new Date(tx.timestamp).toLocaleString();

        let html = '';
        html += '<div class="meta-row">';
        html += metaItem('Key ID (' + report.partitionField + ')', tx.accountId);
        html += metaItem('Transaction ID', tx.transactionId);
        html += metaItem('Time', time);
        html += metaItem('Amount', tx.amount.toFixed(2));
        html += metaItem('Classification', report.classification);
        html += metaItem('Routing', 'keyGroup ' + report.keyGroup + ' → subtask ' + report.assignedSubtask + ' of ' + report.parallelism
            + ' (max parallelism ' + report.maxParallelism + ')', true);
        html += '</div>';

        html += '<h3>Flink SQL walkthrough (illustrative &mdash; this job uses the DataStream API, not Table/SQL)</h3>';
        (report.steps || []).forEach(step => {
            html += '<div class="interrogate-step">';
            html += '<h4>' + escapeHtml(step.title) + '</h4>';
            html += '<pre>' + escapeHtml(step.sql) + '</pre>';
            html += '<div class="result">&rarr; ' + escapeHtml(step.result) + '</div>';
            html += '</div>';
        });

        interrogateBody.innerHTML = html;
    }

    function metaItem(label, value, wide) {
        return '<div class="meta-item' + (wide ? ' wide' : '') + '"><span class="meta-label">' + escapeHtml(label) + '</span>' +
            '<span class="meta-value">' + escapeHtml(String(value)) + '</span></div>';
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    // Closing the report just hides it — generation stays paused (browse another row, or hit
    // Resume Generation on the toolbar) until the user explicitly unpauses.
    function closeInterrogation() {
        interrogateOverlay.classList.add('hidden');
        interrogateBody.innerHTML = '';
    }

    interrogateCloseBtn.addEventListener('click', closeInterrogation);

    // --- Stats + charts ---
    function fmtMoney(n) {
        return '$' + Number(n).toLocaleString(undefined, {maximumFractionDigits: 0});
    }

    async function refreshStats() {
        try {
            const res = await fetch('/api/stats');
            const stats = await res.json();

            setFittedText(document.getElementById('stat-total'), stats.totalTransactions.toLocaleString(), TILE_FONT_MAX, TILE_FONT_MIN);
            setFittedText(document.getElementById('stat-rate'), stats.transactionsPerSecond.toFixed(1), TILE_FONT_MAX, TILE_FONT_MIN);
            setFittedText(document.getElementById('stat-volume'), fmtMoney(stats.totalVolume), TILE_FONT_MAX, TILE_FONT_MIN);
            setFittedText(document.getElementById('stat-highvalue'), stats.highValueCount.toLocaleString(), TILE_FONT_MAX, TILE_FONT_MIN);
            setFittedText(document.getElementById('stat-overdraft'), stats.overdraftCount.toLocaleString(), TILE_FONT_MAX, TILE_FONT_MIN);
            setFittedText(document.getElementById('stat-overdraft-rate'), stats.overdraftRatePercent.toFixed(2) + '%', TILE_FONT_MAX, TILE_FONT_MIN);

            const topBody = document.getElementById('top-accounts-body');
            topBody.innerHTML = '';
            (stats.topAccounts || []).forEach(a => {
                const row = document.createElement('tr');
                row.innerHTML = '<td>' + a.accountId + '</td><td class="mono">' + a.balance.toFixed(2) + '</td>';
                topBody.appendChild(row);
            });

            rateHistory.push(stats.transactionsPerSecond);
            if (rateHistory.length > RATE_HISTORY_LEN) rateHistory.shift();
            drawLineChart(document.getElementById('chart-rate'), rateHistory);

            drawBarChart(document.getElementById('chart-status'), [
                {label: 'Standard', value: stats.standardCount, color: '#2f6feb'},
                {label: 'High-Value', value: stats.highValueCount, color: '#d97706'},
                {label: 'Overdraft', value: stats.overdraftCount, color: '#dc2626'}
            ]);
        } catch (e) {
            // ignore transient failures
        }
    }

    // --- Minimal dependency-free canvas charts ---
    function prepareCanvas(canvas) {
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        const width = rect.width || canvas.clientWidth || 300;
        const height = rect.height || canvas.clientHeight || 160;
        canvas.width = width * dpr;
        canvas.height = height * dpr;
        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, width, height);
        return {ctx, width, height};
    }

    function drawLineChart(canvas, values) {
        const {ctx, width, height} = prepareCanvas(canvas);
        const pad = 8;
        if (values.length < 2) return;
        const max = Math.max(1, ...values);
        const stepX = (width - pad * 2) / (values.length - 1);

        ctx.strokeStyle = '#2f6feb';
        ctx.lineWidth = 2;
        ctx.beginPath();
        values.forEach((v, i) => {
            const x = pad + i * stepX;
            const y = height - pad - (v / max) * (height - pad * 2);
            if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.stroke();
    }

    function drawBarChart(canvas, bars) {
        const {ctx, width, height} = prepareCanvas(canvas);
        const pad = 10;
        const max = Math.max(1, ...bars.map(b => b.value));
        const barWidth = (width - pad * 2) / bars.length;

        bars.forEach((bar, i) => {
            const barHeight = (bar.value / max) * (height - pad * 2 - 16);
            const x = pad + i * barWidth + barWidth * 0.15;
            const y = height - pad - barHeight;
            ctx.fillStyle = bar.color;
            ctx.fillRect(x, y, barWidth * 0.7, barHeight);
            ctx.fillStyle = '#8b95a5';
            ctx.font = '11px sans-serif';
            ctx.textAlign = 'center';
            ctx.fillText(bar.label, x + barWidth * 0.35, height - 2);
        });
    }

    connectWebSocket();
    refreshStatus({syncForm: true});
    setInterval(refreshStatus, 3000);
    refreshStats();
    setInterval(refreshStats, 1000);
})();
