// motel web UI — vanilla JS dashboard

// ---------------------------------------------------------------------------
// Tab switching
// ---------------------------------------------------------------------------
document.querySelectorAll('.tab').forEach(function(btn) {
    btn.addEventListener('click', function() {
        document.querySelectorAll('.tab').forEach(function(b) { b.classList.remove('active'); });
        document.querySelectorAll('.tab-content').forEach(function(c) { c.classList.remove('active'); });
        btn.classList.add('active');
        document.getElementById(btn.dataset.tab + '-tab').classList.add('active');
    });
});

// ---------------------------------------------------------------------------
// Service color palette (matches TUI One Dark theme)
// ---------------------------------------------------------------------------
var SERVICE_COLORS = [
    '#61afef', '#98c379', '#e5c07b', '#c678dd',
    '#56b6c2', '#e06c75', '#d19a66', '#bebebe'
];
var serviceColorMap = {};

function serviceColor(name) {
    if (serviceColorMap[name]) return serviceColorMap[name];
    var idx = Object.keys(serviceColorMap).length % SERVICE_COLORS.length;
    serviceColorMap[name] = SERVICE_COLORS[idx];
    return serviceColorMap[name];
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------
function formatTime(nanos) {
    if (!nanos || nanos === 0) return '-';
    var ms = nanos / 1e6;
    var d = new Date(ms);
    return d.toISOString().replace('T', ' ').replace('Z', '').slice(11, 23);
}

function formatDuration(ns) {
    if (ns >= 1e9) return (ns / 1e9).toFixed(3) + 's';
    if (ns >= 1e6) return (ns / 1e6).toFixed(2) + 'ms';
    if (ns >= 1e3) return (ns / 1e3).toFixed(1) + 'us';
    return ns + 'ns';
}

function escapeHtml(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function severityClass(sev) {
    if (!sev) return '';
    var s = sev.toLowerCase();
    if (s === 'trace') return 'severity-trace';
    if (s === 'debug') return 'severity-debug';
    if (s === 'info') return 'severity-info';
    if (s === 'warn' || s === 'warning') return 'severity-warn';
    if (s === 'error') return 'severity-error';
    if (s === 'fatal') return 'severity-fatal';
    return '';
}

function statusClass(code) {
    if (code === 1) return 'status-ok';
    if (code === 2) return 'status-error';
    return 'status-unset';
}

// ---------------------------------------------------------------------------
// SSE connection for real-time updates
// ---------------------------------------------------------------------------
var eventSource = null;

function connectSSE() {
    if (eventSource) eventSource.close();
    eventSource = new EventSource('/api/events');

    eventSource.addEventListener('traces', function() { fetchTraces(); fetchStatus(); });
    eventSource.addEventListener('logs', function() { fetchLogs(); fetchStatus(); });
    eventSource.addEventListener('metrics', function() { fetchMetrics(); fetchStatus(); });
    eventSource.addEventListener('traces_cleared', function() { fetchTraces(); fetchStatus(); });
    eventSource.addEventListener('logs_cleared', function() { fetchLogs(); fetchStatus(); });
    eventSource.addEventListener('metrics_cleared', function() { fetchMetrics(); fetchStatus(); });

    eventSource.addEventListener('open', function() {
        var el = document.getElementById('connection-status');
        el.textContent = 'connected';
        el.className = 'connected';
    });

    eventSource.addEventListener('error', function() {
        var el = document.getElementById('connection-status');
        el.textContent = 'disconnected';
        el.className = 'disconnected';
    });
}

// ---------------------------------------------------------------------------
// Fetch and render: Status
// ---------------------------------------------------------------------------
function fetchStatus() {
    fetch('/api/status').then(function(r) { return r.json(); }).then(function(data) {
        document.getElementById('trace-count').textContent = data.trace_count;
        document.getElementById('span-count').textContent = data.span_count;
        document.getElementById('log-count').textContent = data.log_count;
        document.getElementById('metric-count').textContent = data.metric_count;
    }).catch(function() {});
}

// ---------------------------------------------------------------------------
// Fetch and render: Logs
// ---------------------------------------------------------------------------
function fetchLogs() {
    fetch('/api/logs').then(function(r) { return r.json(); }).then(function(logs) {
        var tbody = document.querySelector('#logs-table tbody');
        var empty = document.getElementById('logs-empty');
        if (!logs || logs.length === 0) {
            tbody.innerHTML = '';
            empty.style.display = 'block';
            return;
        }
        empty.style.display = 'none';
        // Show newest first
        var rows = logs.slice().reverse();
        tbody.innerHTML = rows.map(function(log) {
            return '<tr>' +
                '<td>' + escapeHtml(log.time) + '</td>' +
                '<td>' + escapeHtml(log.service_name) + '</td>' +
                '<td class="' + severityClass(log.severity_text) + '">' + escapeHtml(log.severity_text) + '</td>' +
                '<td>' + escapeHtml(log.body) + '</td>' +
                '</tr>';
        }).join('');
    }).catch(function() {});
}

// ---------------------------------------------------------------------------
// Fetch and render: Traces
// ---------------------------------------------------------------------------
var traceGroups = [];

function fetchTraces() {
    fetch('/api/traces').then(function(r) { return r.json(); }).then(function(groups) {
        traceGroups = groups || [];
        renderTraceList();
    }).catch(function() {});
}

function renderTraceList() {
    var tbody = document.querySelector('#traces-table tbody');
    var empty = document.getElementById('traces-empty');
    if (!traceGroups || traceGroups.length === 0) {
        tbody.innerHTML = '';
        empty.style.display = 'block';
        return;
    }
    empty.style.display = 'none';
    // Show newest first
    var groups = traceGroups.slice().reverse();
    tbody.innerHTML = groups.map(function(g, i) {
        var realIdx = traceGroups.length - 1 - i;
        return '<tr class="trace-row" data-idx="' + realIdx + '">' +
            '<td style="font-family:monospace;font-size:11px">' + escapeHtml(g.trace_id.slice(0, 16)) + '...</td>' +
            '<td>' + escapeHtml(g.service_name) + '</td>' +
            '<td>' + escapeHtml(g.root_span_name) + '</td>' +
            '<td>' + g.span_count + '</td>' +
            '<td>' + escapeHtml(g.duration) + '</td>' +
            '<td>' + escapeHtml(g.start_time) + '</td>' +
            '</tr>';
    }).join('');

    // Click handlers
    tbody.querySelectorAll('.trace-row').forEach(function(row) {
        row.addEventListener('click', function() {
            var idx = parseInt(row.getAttribute('data-idx'));
            showTraceDetail(idx);
        });
    });
}

function showTraceDetail(idx) {
    var g = traceGroups[idx];
    if (!g) return;

    document.querySelector('.trace-list').style.display = 'none';
    var detail = document.getElementById('trace-detail');
    detail.classList.add('active');
    document.getElementById('trace-detail-title').textContent =
        g.root_span_name + ' (' + g.span_count + ' spans, ' + g.duration + ')';

    // Build waterfall
    var spans = g.spans || [];
    if (spans.length === 0) return;

    var minStart = Infinity, maxEnd = 0;
    spans.forEach(function(s) {
        if (s.start_ns < minStart) minStart = s.start_ns;
        var end = s.start_ns + s.duration_ns;
        if (end > maxEnd) maxEnd = end;
    });
    var totalRange = maxEnd - minStart;
    if (totalRange === 0) totalRange = 1;

    var tbody = document.querySelector('#waterfall-table tbody');
    tbody.innerHTML = spans.map(function(s) {
        var indent = '';
        for (var d = 0; d < s.depth; d++) {
            indent += d === s.depth - 1 ? '<span class="tree-indent">\u251c\u2500 </span>' : '<span class="tree-indent">\u2502  </span>';
        }
        var leftPct = ((s.start_ns - minStart) / totalRange * 100).toFixed(2);
        var widthPct = Math.max(0.5, (s.duration_ns / totalRange * 100)).toFixed(2);
        var color = serviceColor(s.service_name);
        var statusCls = statusClass(s.status_code);
        return '<tr>' +
            '<td class="span-name">' + indent + '<span class="' + statusCls + '">' + escapeHtml(s.span_name) + '</span></td>' +
            '<td class="span-bar-cell"><div class="span-bar-container">' +
            '<div class="span-bar" style="left:' + leftPct + '%;width:' + widthPct + '%;background:' + color + '" title="' + escapeHtml(s.service_name) + ': ' + escapeHtml(s.span_name) + '"></div>' +
            '</div></td>' +
            '<td class="span-duration">' + escapeHtml(s.duration) + '</td>' +
            '</tr>';
    }).join('');
}

document.getElementById('trace-back').addEventListener('click', function() {
    document.getElementById('trace-detail').classList.remove('active');
    document.querySelector('.trace-list').style.display = 'block';
});

// ---------------------------------------------------------------------------
// Fetch and render: Metrics
// ---------------------------------------------------------------------------
function fetchMetrics() {
    fetch('/api/metrics').then(function(r) { return r.json(); }).then(function(metrics) {
        var tbody = document.querySelector('#metrics-table tbody');
        var empty = document.getElementById('metrics-empty');
        if (!metrics || metrics.length === 0) {
            tbody.innerHTML = '';
            empty.style.display = 'block';
            return;
        }
        empty.style.display = 'none';
        tbody.innerHTML = metrics.map(function(m) {
            return '<tr>' +
                '<td>' + escapeHtml(m.metric_name) + '</td>' +
                '<td>' + escapeHtml(m.service_name) + '</td>' +
                '<td><span class="metric-type">' + escapeHtml(m.metric_type) + '</span></td>' +
                '<td class="metric-value">' + escapeHtml(m.display_value) + '</td>' +
                '<td>' + escapeHtml(m.unit) + '</td>' +
                '<td>' + m.data_point_count + '</td>' +
                '</tr>';
        }).join('');
    }).catch(function() {});
}

// ---------------------------------------------------------------------------
// SQL query execution
// ---------------------------------------------------------------------------
document.getElementById('sql-run').addEventListener('click', runSql);

document.getElementById('sql-input').addEventListener('keydown', function(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        runSql();
    }
});

function runSql() {
    var query = document.getElementById('sql-input').value.trim();
    if (!query) return;

    var btn = document.getElementById('sql-run');
    btn.disabled = true;
    btn.textContent = 'Running...';

    var errEl = document.getElementById('sql-error');
    errEl.style.display = 'none';

    fetch('/api/sql?q=' + encodeURIComponent(query))
        .then(function(r) {
            if (!r.ok) return r.text().then(function(t) { throw new Error(t); });
            return r.json();
        })
        .then(function(data) {
            var thead = document.querySelector('#sql-results thead');
            var tbody = document.querySelector('#sql-results tbody');
            thead.innerHTML = '<tr>' + data.columns.map(function(c) { return '<th>' + escapeHtml(c) + '</th>'; }).join('') + '</tr>';
            tbody.innerHTML = data.rows.map(function(row) {
                return '<tr>' + row.map(function(v) { return '<td>' + escapeHtml(v) + '</td>'; }).join('') + '</tr>';
            }).join('');
        })
        .catch(function(err) {
            errEl.textContent = err.message || 'SQL query failed';
            errEl.style.display = 'block';
        })
        .finally(function() {
            btn.disabled = false;
            btn.textContent = 'Run';
        });
}

// ---------------------------------------------------------------------------
// Initial load
// ---------------------------------------------------------------------------
fetchStatus();
fetchLogs();
fetchTraces();
fetchMetrics();
connectSSE();
