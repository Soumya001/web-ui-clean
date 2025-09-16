/* xp-hashrate-chart.js — XP Task Manager–style line chart with persistence
   - Reads size & grid density from CSS variables (no HTML edits needed):
       --xp-chart-max-width  (CSS handles width cap)
       --xp-chart-height     (canvas drawing height, px)
       --xp-grid             (target grid cell size, px; smaller => more cells)
   - Persists samples per wallet in localStorage
   - Redraws on resize
   - Improvements:
     * storageKey available at construction time (via opts.wallet or opts.storageKey)
     * parseRate accepts commas, spaces, unit suffixes
     * push() persists even if load() wasn't called first
     * load() accepts string key OR {wallet:'..'} / {storageKey:'..'}
     * public methods: seed(), _prune(), _save(), redraw()
*/

(function(global){
  const DPR = Math.max(1, Math.floor(window.devicePixelRatio || 1));

  // Read numeric CSS custom property from element or fallback
  function readCssNumber(el, name, fallback){
    const v = getComputedStyle(el).getPropertyValue(name).trim();
    const n = parseFloat(v);
    return isFinite(n) && n > 0 ? n : fallback;
  }

  // Format human-short units (used for axis ticks and 'now' label)
  function human(v){
    if(!isFinite(v) || v<=0) return '0';
    const u = [['T',1e12], ['G',1e9], ['M',1e6], ['K',1e3]];
    for(const [s,m] of u){ if(v>=m) { const x=v/m; return (x>=100?x.toFixed(0):x>=10?x.toFixed(1):x.toFixed(2))+s; } }
    return String(Math.round(v));
  }

  // Robust parse: accepts numbers, "1,234", "1.23G", " 500 K", etc.
  function parseRate(rate){
    if(rate==null) return 0;
    if(typeof rate==='number') return isFinite(rate) ? rate : 0;
    let s = String(rate).trim();
    if(!s) return 0;
    // remove commas and any whitespace
    s = s.replace(/[,\s]/g,'');
    const m = s.match(/^(-?[\d.]+)([kKmMgGtTpP]?)$/);
    if(!m) return 0;
    const num = parseFloat(m[1]);
    const unit = (m[2]||'').toUpperCase();
    const mult = { '':1, K:1e3, M:1e6, G:1e9, T:1e12, P:1e15 }[unit] || 1;
    return isFinite(num) ? (num * mult) : 0;
  }

  // Persistence helpers
  function loadSeries(key, windowSec){
    try{
      const raw = localStorage.getItem(key);
      if(!raw) return [];
      const j = JSON.parse(raw || '[]');
      const cutoff = Math.floor(Date.now()/1000) - windowSec;
      return Array.isArray(j) ? j.filter(p => p && p.t && p.t >= cutoff) : [];
    }catch(_){ return []; }
  }
  function saveSeries(key, arr){
    try{ localStorage.setItem(key, JSON.stringify(arr)); }catch(_){}
  }

  function XpChart(canvas, opts){
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.opts = Object.assign({
      windowSec: 6*3600,   // rolling 6 hours
      cadenceSec: 5,       // expected push cadence
      gridTargetPx: null,  // read from CSS or default
      heightPx: null       // read from CSS or default
    }, opts||{});

    // Allow early setting of storageKey via opts.wallet or opts.storageKey so push() can persist immediately
    if (this.opts.storageKey) {
      this.storageKey = this.opts.storageKey;
    } else if (this.opts.wallet) {
      this.storageKey = 'xpHashrate:' + String(this.opts.wallet);
    } else {
      this.storageKey = null;
    }

    this.series = []; // array of {t, v}
    this.maxY = 0;
    this._resize = this._resize.bind(this);
    this._raf = null;
    this._renderQueued = false;
    this._attach();
  }

  XpChart.prototype._attach = function(){
    // Read CSS-driven parameters (only if not provided via opts)
    const cssGrid = readCssNumber(this.canvas, '--xp-grid', 56);
    const cssH = readCssNumber(this.canvas, '--xp-chart-height', 220);
    this.opts.gridTargetPx = this.opts.gridTargetPx || cssGrid;
    this.opts.heightPx = this.opts.heightPx || cssH;

    this._resize();
    window.addEventListener('resize', this._resize);
  };

  XpChart.prototype._resize = function(){
    const cw = Math.max(200, Math.floor(this.canvas.clientWidth));
    const ch = Math.max(120, Math.floor(this.opts.heightPx));

    this.canvas.width  = cw * DPR;
    this.canvas.height = ch * DPR;
    this.canvas.style.height = ch + 'px';

    // Insets (room for axes)
    this.leftPad  = Math.round(46 * DPR);
    this.rightPad = Math.round(10 * DPR);
    this.topPad   = Math.round(10 * DPR);
    this.botPad   = Math.round(22 * DPR);

    this.plotW = this.canvas.width  - this.leftPad - this.rightPad;
    this.plotH = this.canvas.height - this.topPad  - this.botPad;

    this._queueRender();
  };

  XpChart.prototype._queueRender = function(){
    if(this._renderQueued) return;
    this._renderQueued = true;
    this._raf = requestAnimationFrame(() => {
      this._renderQueued = false;
      this._render();
    });
  };

  XpChart.prototype._render = function(){
    const ctx = this.ctx;
    const cw = this.canvas.width;
    const ch = this.canvas.height;

    ctx.save();
    // Background
    ctx.fillStyle = '#061a06';
    ctx.fillRect(0,0,cw,ch);

    // Plot rect
    const x0 = this.leftPad, y0 = this.topPad, w = this.plotW, h = this.plotH;

    // Grid — make cells square-ish using target px from CSS
    const tgt = Math.max(28, Math.min(140, this.opts.gridTargetPx * DPR)); // clamp
    const vLines = Math.max(4, Math.round(w / tgt));
    const hLines = Math.max(4, Math.round(h / tgt));

    // Outer border
    ctx.strokeStyle = '#00bf00';
    ctx.lineWidth = Math.max(1, Math.floor(1*DPR));
    ctx.strokeRect(x0, y0, w, h);

    // Grid lines
    ctx.strokeStyle = '#0b2e0b';
    ctx.lineWidth = Math.max(1, Math.floor(1*DPR));
    // vertical
    for(let i=1;i<vLines;i++){
      const x = x0 + Math.round((i * w) / vLines);
      ctx.beginPath(); ctx.moveTo(x, y0); ctx.lineTo(x, y0+h); ctx.stroke();
    }
    // horizontal
    for(let j=1;j<hLines;j++){
      const y = y0 + Math.round((j * h) / hLines);
      ctx.beginPath(); ctx.moveTo(x0, y); ctx.lineTo(x0+w, y); ctx.stroke();
    }

    // Y-axis labels (left)
    ctx.fillStyle = '#cfe9cf';
    ctx.font = `${Math.max(10,Math.floor(11*DPR))}px system-ui, -apple-system, Segoe UI, Arial`;
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'right';

    const maxY = this.maxY > 0 ? this.maxY : 1;
    for(let j=0;j<=hLines;j++){
      const y = y0 + Math.round((j * h) / hLines);
      const val = maxY * (1 - j / hLines);
      const label = j===hLines ? '0' : human(val);
      ctx.fillText(label, x0-8*DPR, y);
    }

    // Series line
    const now = Math.floor(Date.now()/1000);
    const t0 = now - this.opts.windowSec;

    // Transform helpers
    const xOfT = t => x0 + Math.round( ( (t - t0) / this.opts.windowSec ) * w );
    const yOfV = v => y0 + Math.round( h - (v / maxY) * h );

    // Clip to plot
    ctx.save();
    ctx.beginPath();
    ctx.rect(x0, y0, w, h);
    ctx.clip();

    // Path
    ctx.strokeStyle = '#2aff2a';
    ctx.lineWidth = Math.max(2, Math.floor(2*DPR));
    ctx.lineJoin = 'round';
    ctx.lineCap  = 'round';

    let started=false;
    ctx.beginPath();
    for(const p of this.series){
      if(p.t < t0) continue;
      const X = xOfT(p.t), Y = yOfV(p.v);
      if(!started){ ctx.moveTo(X,Y); started=true; }
      else ctx.lineTo(X,Y);
    }
    if(started) ctx.stroke();

    // “now:” label bottom-right
    ctx.restore();
    ctx.fillStyle = '#cfe9cf';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'alphabetic';
    const last = this.series.length ? this.series[this.series.length-1].v : 0;
    ctx.fillText('now: ' + human(last), x0+w-4*DPR, y0+h+14*DPR);

    ctx.restore();
  };

  // push a new sample (rateLike can be number or string)
  XpChart.prototype.push = function(rateLike){
    const v = parseRate(rateLike);
    const t = Math.floor(Date.now()/1000);

    const arr = this.series;
    // Append or replace last if same timestamp bucket
    if(arr.length && t === arr[arr.length-1].t){
      arr[arr.length-1].v = v;
    } else {
      arr.push({t,v});
    }

    // Keep within window
    const cutoff = t - this.opts.windowSec;
    while(arr.length && arr[0].t < cutoff) arr.shift();

    // Update maxY (smooth to recent peak, 10% headroom)
    let peak = 0;
    for(const p of arr){ if(p.v > peak) peak = p.v; }
    this.maxY = peak ? peak * 1.10 : this.maxY || 1;

    // Persist (ensure storageKey exists; try to derive from opts.wallet if available)
    try {
      if(!this.storageKey && this.opts && this.opts.wallet){
        this.storageKey = 'xpHashrate:' + String(this.opts.wallet);
      }
      if(this.storageKey) saveSeries(this.storageKey, arr);
    } catch(_) { /* ignore storage errors */ }

    // Redraw
    this._queueRender();
  };

  // load persisted series — keyOrOpts may be string key or object {wallet:'..'} or {storageKey:'..'}
  XpChart.prototype.load = function(keyOrOpts){
    if(typeof keyOrOpts === 'string'){
      this.storageKey = keyOrOpts;
    } else if(keyOrOpts && typeof keyOrOpts === 'object'){
      if(keyOrOpts.storageKey) this.storageKey = keyOrOpts.storageKey;
      else if(keyOrOpts.wallet) this.storageKey = 'xpHashrate:' + String(keyOrOpts.wallet);
    }
    if(this.storageKey) this.series = loadSeries(this.storageKey, this.opts.windowSec);
    else this.series = [];
    // Recalculate maxY on load
    let peak = 0; for(const p of this.series){ if(p.v > peak) peak = p.v; }
    this.maxY = peak ? peak * 1.10 : 0;
    this._queueRender();
  };

  // --- Public helpers added for template compatibility ---
  // Replace the chart series with a bulk seed (array of {t, v})
  XpChart.prototype.seed = function(points){
    if(!Array.isArray(points)) return;
    // Normalize points (numbers) and sort by time just in case
    const norm = points
      .map(p => {
        // accept {t,v} or [t,v] or [ts,val]
        if (Array.isArray(p)) return { t: Number(p[0]) || 0, v: parseRate(p[1] ?? 0) || 0 };
        return { t: Number(p.t || p[0]) || 0, v: parseRate(p.v ?? p[1] ?? p.value) || 0 };
      })
      .filter(p => p.t > 0)
      .sort((a,b)=>a.t - b.t);

    this.series = norm;

    // prune to window
    const now = Math.floor(Date.now()/1000);
    const cutoff = now - this.opts.windowSec;
    while(this.series.length && this.series[0].t < cutoff) this.series.shift();

    // recalc maxY and persist
    let peak = 0; for(const p of this.series){ if(p.v > peak) peak = p.v; }
    this.maxY = peak ? peak * 1.10 : 0;

    if(!this.storageKey && this.opts && this.opts.wallet){
      this.storageKey = 'xpHashrate:' + String(this.opts.wallet);
    }
    if(this.storageKey) saveSeries(this.storageKey, this.series);

    this._queueRender();
  };

  // Prune old samples (public alias)
  XpChart.prototype._prune = function(){
    const now = Math.floor(Date.now()/1000);
    const cutoff = now - this.opts.windowSec;
    while(this.series.length && this.series[0].t < cutoff) this.series.shift();
    // recalc maxY
    let peak = 0; for(const p of this.series){ if(p.v > peak) peak = p.v; }
    this.maxY = peak ? peak * 1.10 : 0;
  };

  // Save current series to storage (public alias)
  XpChart.prototype._save = function(){
    if(!this.storageKey && this.opts && this.opts.wallet){
      this.storageKey = 'xpHashrate:' + String(this.opts.wallet);
    }
    if(this.storageKey) saveSeries(this.storageKey, this.series);
  };

  // Redraw alias
  XpChart.prototype.redraw = function(){
    this._queueRender();
  };

  XpChart.prototype.destroy = function(){
    window.removeEventListener('resize', this._resize);
    cancelAnimationFrame(this._raf);
  };

  // Public init: selector, wallet (optional), options
  function init(selector, wallet, options){
    const canvas = document.querySelector(selector);
    if(!canvas) return null;

    // pass wallet down in opts so storageKey is available early
    const opts = Object.assign({}, options || {}, (wallet ? {wallet: wallet} : {}));
    const chart = new XpChart(canvas, opts);

    // call load with explicit key so persisted samples show immediately
    if (wallet) {
      const key = 'xpHashrate:' + wallet;
      chart.load(key);
    } else if (options && options.storageKey) {
      chart.load(options.storageKey);
    } else {
      // no key provided — nothing persisted, but chart still works
      chart.load(); // resets series to empty and queues render
    }

    return chart;
  }

  // Export
  global.XPHashrateChart = { init };

})(window);
