/* =========================
   XP Window (enhanced, mobile-friendly)
   ========================= */
(function () {
  const win = document.getElementById('xp-window');
  if (!win) return;

  const titleBar  = win.querySelector('.title-bar');
  const controls  = win.querySelectorAll('.title-bar-controls button');
  const btnMin    = controls[0] || null;
  const btnMax    = controls[1] || null;
  const btnClose  = controls[2] || null;

  // --- config ---
  // PATCH: responsive minimums instead of fixed numbers
  function MIN_W(){ return Math.max(280, Math.min(640, Math.floor(window.innerWidth * 0.90))); }
  function MIN_H(){ return Math.max(220, Math.min(520, Math.floor(window.innerHeight * 0.55))); }
  const DOCK_W  = 360;   // minimized width
  const MARGIN  = 12;    // dock margin
  const TITLE_H = 28;    // approx title bar height

  // --- z-index handling ---
  let zTop = parseInt(sessionStorage.getItem('xpwin:zTop') || '100', 10);
  function bringToFront() {
    zTop += 1;
    sessionStorage.setItem('xpwin:zTop', String(zTop));
    win.style.zIndex = String(zTop);
  }
  bringToFront();
  win.addEventListener('mousedown', bringToFront, { capture: true });

  // --- state ---
  let drag   = null;   // { sx, sy, left, top, pointerId? }
  let resize = null;   // { sx, sy, left, top, w, h, dir, pointerId? }
  let isMax  = false;
  let isMin  = false;

  // Keep a clean “normal” rectangle (never minimized/maximized)
  let normalRect = null; // { left, top, width, height }

  // helpers
  const px   = n => Math.round(n) + 'px';
  const rect = () => win.getBoundingClientRect();
  function readLeftTop() {
    const cs = getComputedStyle(win);
    return {
      left: parseFloat(cs.left || '0') || 0,
      top : parseFloat(cs.top  || '0') || 0
    };
  }
  function applyRect(r) {
    if (!r) return;
    Object.assign(win.style, {
      left  : px(r.left),
      top   : px(r.top),
      width : px(Math.max(MIN_W(), r.width)),   // PATCH: MIN_W()
      height: px(Math.max(MIN_H(), r.height))   // PATCH: MIN_H()
    });
  }
  function captureCurrentAsNormal() {
    // Only capture when not minimized/maximized
    if (isMax || isMin) return;
    const r  = rect();
    const lt = readLeftTop();
    normalRect = { left: lt.left, top: lt.top, width: r.width, height: r.height };
  }
  function persist() {
    if (normalRect) sessionStorage.setItem('xpwin:rect', JSON.stringify(normalRect));
    sessionStorage.setItem('xpwin:isMax', isMax ? '1' : '0');
    sessionStorage.setItem('xpwin:isMin', isMin ? '1' : '0');
  }

  // initial placement / restore
  (function restore() {
    const raw    = sessionStorage.getItem('xpwin:rect');
    const wasMax = sessionStorage.getItem('xpwin:isMax') === '1';
    const wasMin = sessionStorage.getItem('xpwin:isMin') === '1';

    if (raw) {
      try {
        normalRect = JSON.parse(raw);
        applyRect(normalRect);
      } catch (_) {
        centerInitial();
        captureCurrentAsNormal();
      }
    } else {
      centerInitial();
      captureCurrentAsNormal();
    }

    if (wasMax) doMaximize(true);
    if (wasMin) doMinimize(true);

    // reveal after positioned (anti-flicker)
    document.body.classList.add('xp-ready');
  })();

  function centerInitial() {
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const w = Math.max(MIN_W(), Math.min(0.8 * vw, 1000));  // PATCH: MIN_W()
    const h = Math.max(MIN_H(), Math.min(0.8 * vh, 700));   // PATCH: MIN_H()
    const left = Math.round((vw - w) / 2);
    const top  = Math.round(64);
    Object.assign(win.style, { left: px(left), top: px(top), width: px(w), height: px(h) });
  }

  // --- Pointer Event helpers (desktop + touch) ---
  function addWindowPointerListeners() {
    window.addEventListener('pointermove', onPointerMove, { passive:false }); // PATCH: passive:false
    window.addEventListener('pointerup',   onPointerUp,   { passive:true  });
    window.addEventListener('pointercancel', onPointerUp, { passive:true  });
  }
  function removeWindowPointerListeners() {
    window.removeEventListener('pointermove', onPointerMove);
    window.removeEventListener('pointerup',   onPointerUp);
    window.removeEventListener('pointercancel', onPointerUp);
  }

  // PATCH: while dragging/resizing, hard-disable page touch gestures
  let gestureBlockCount = 0;
  function blockPageGestures() {
    gestureBlockCount++;
    document.documentElement.style.touchAction = 'none';
    document.body.style.touchAction = 'none';
  }
  function unblockPageGestures() {
    gestureBlockCount = Math.max(0, gestureBlockCount - 1);
    if (gestureBlockCount === 0) {
      document.documentElement.style.removeProperty('touch-action');
      document.body.style.removeProperty('touch-action');
    }
  }
  // Extra guard for iOS Safari pinch
  ['gesturestart','gesturechange','gestureend'].forEach(evt=>{
    window.addEventListener(evt, e => { if (drag || resize) { e.preventDefault(); } }, { passive:false });
  });
  window.addEventListener('touchmove', e => {
    if ((drag || resize) && (e.touches && e.touches.length > 1 || (e.scale && e.scale !== 1))) {
      e.preventDefault();
    }
  }, { passive:false });

  // --- drag start (if maximized, restore then drag) ---
  if (titleBar) {
    titleBar.addEventListener('pointerdown', (e) => {
      // ignore clicks on buttons
      if (e.target.closest('.title-bar-controls')) return;

      bringToFront();
      e.preventDefault();
      titleBar.setPointerCapture?.(e.pointerId);

      // If maximized, restore to normal but keep cursor’s relative X/Y position
      if (isMax) {
        if (!normalRect) {
          // If somehow missing, synthesize
          normalRect = { left: 80, top: 64, width: Math.max(MIN_W(), 960), height: Math.max(MIN_H(), 600) }; // PATCH
        }
        const cursorFracX = e.clientX / window.innerWidth;
        const cursorFracY = Math.min(1, Math.max(0, e.clientY / window.innerHeight));
        const newLeft = Math.round(e.clientX - cursorFracX * normalRect.width);
        const newTop  = Math.round(e.clientY - cursorFracY * Math.max(TITLE_H + 8, normalRect.height * 0.1));
        doRestoreToNormal({ left: newLeft, top: newTop });
      }

      const lt = readLeftTop();
      drag = { sx: e.clientX, sy: e.clientY, left: lt.left, top: lt.top, pointerId: e.pointerId };
      document.body.classList.add('dragging');
      blockPageGestures();                 // PATCH
      addWindowPointerListeners();
    });
  }

  // --- resize handles ---
  ['n','s','e','w','nw','ne','sw','se'].forEach(dir => {
    const d = win.querySelector(`.resize-handle.${dir}`) || (() => {
      const el = document.createElement('div');
      el.className = 'resize-handle ' + dir;
      win.appendChild(el);
      return el;
    })();

    d.addEventListener('pointerdown', (e) => {
      bringToFront();
      if (isMax || isMin) return;
      e.preventDefault();
      d.setPointerCapture?.(e.pointerId);

      const r  = rect();
      const lt = readLeftTop();
      resize = { sx: e.clientX, sy: e.clientY, left: lt.left, top: lt.top, w: r.width, h: r.height, dir, pointerId: e.pointerId };
      document.body.classList.add('resizing');
      blockPageGestures();                 // PATCH
      addWindowPointerListeners();
    });
  });

  function onPointerMove(e) {
    // PATCH: preventDefault during drag/resize to block page pan/zoom
    if (drag || resize) { e.preventDefault(); }

    if (drag && e.pointerId === drag.pointerId) {
      const dx = e.clientX - drag.sx;
      const dy = e.clientY - drag.sy;
      Object.assign(win.style, { left: px(drag.left + dx), top: px(drag.top + dy) });
      return;
    }
    if (resize && e.pointerId === resize.pointerId) {
      const dx = e.clientX - resize.sx;
      const dy = e.clientY - resize.sy;
      let { left, top, w, h } = resize;
      const dir = resize.dir;

      if (dir.includes('e')) w = Math.max(MIN_W(), resize.w + dx);   // PATCH
      if (dir.includes('s')) h = Math.max(MIN_H(), resize.h + dy);   // PATCH
      if (dir.includes('w')) {
        const newW = Math.max(MIN_W(), resize.w - dx);               // PATCH
        left = left + (resize.w - newW);
        w = newW;
      }
      if (dir.includes('n')) {
        const newH = Math.max(MIN_H(), resize.h - dy);               // PATCH
        top = top + (resize.h - newH);
        h = newH;
      }

      Object.assign(win.style, { left: px(left), top: px(top), width: px(w), height: px(h) });
      return;
    }
  }

  function onPointerUp(e) {
    // drag end
    if (drag && e.pointerId === drag.pointerId) {
      drag = null;
      document.body.classList.remove('dragging');
      captureCurrentAsNormal();
      persist();
      removeWindowPointerListeners();
      unblockPageGestures();               // PATCH
    }
    // resize end
    if (resize && e.pointerId === resize.pointerId) {
      resize = null;
      document.body.classList.remove('resizing');
      captureCurrentAsNormal();
      persist();
      removeWindowPointerListeners();
      unblockPageGestures();               // PATCH
    }
  }

  // keep max/min layout on viewport changes
  window.addEventListener('resize', () => {
    if (isMax) {
      Object.assign(win.style, {
        left: px(0), top: px(0),
        width: px(window.innerWidth),
        height: px(window.innerHeight)
      });
    }
    if (isMin) {
      const top = window.innerHeight - TITLE_H - MARGIN;
      win.style.top  = px(top);
      win.style.left = px(MARGIN);
    }
  });

  // --- maximize / minimize / restore ---
  function doMaximize(fromRestore) {
    if (isMax) return;
    if (!fromRestore) captureCurrentAsNormal();
    isMax = true; isMin = false;
    Object.assign(win.style, {
      left: px(0), top: px(0),
      width: px(window.innerWidth),
      height: px(window.innerHeight)
    });
    win.classList.remove('is-minimized');
    persist();
  }

  function doMinimize(fromRestore) {
    if (isMin) return;
    if (!fromRestore) captureCurrentAsNormal();
    isMin = true; isMax = false;
    const top = window.innerHeight - TITLE_H - MARGIN;
    Object.assign(win.style, {
      left: px(MARGIN),
      top: px(top),
      width: px(DOCK_W),
      height: px(TITLE_H + 6)
    });
    win.classList.add('is-minimized');
    persist();
  }

  function doRestoreToNormal(overrideLT) {
    if (!normalRect) return;
    isMax = false; isMin = false;
    const base = { ...normalRect };
    if (overrideLT) {
      base.left = overrideLT.left ?? base.left;
      base.top  = overrideLT.top  ?? base.top;
    }
    applyRect(base);
    win.classList.remove('is-minimized');
    persist();
  }

  // buttons
  if (btnMin) btnMin.addEventListener('click', () => {
    // Minimize toggles between minimized and normal
    if (isMin) doRestoreToNormal();
    else       doMinimize(false);
  });

  if (btnMax) btnMax.addEventListener('click', () => {
    // Maximize toggles strictly between max and normal (never minimize)
    if (isMax) doRestoreToNormal();
    else       doMaximize(false);
  });

  if (btnClose) btnClose.addEventListener('click', () => {
    win.style.display = 'none';
  });

  // double-click title toggles max/restore
  if (titleBar) {
    titleBar.addEventListener('dblclick', () => {
      if (isMax) doRestoreToNormal(); else doMaximize(false);
    });
  }
})();

/* =========================
   Workers "+N more" Modal (unchanged)
   ========================= */
(function(){
  function closeModal(node){ if (node && node.parentNode) node.parentNode.removeChild(node); }

  async function loadWorkers(wallet, limit, offset){
    try{
      const res = await fetch(`/api/wallet/${encodeURIComponent(wallet)}/workers?limit=${limit}&offset=${offset}`);
      if(!res.ok) throw new Error('HTTP '+res.status);
      return await res.json();
    }catch(e){
      return { workers: [], total: 0 };
    }
  }

  function renderList(container, workers){
    if(!workers || !workers.length){ container.textContent = 'No active workers'; return; }
    const ul = document.createElement('ul');
    workers.forEach(w=>{
      const li = document.createElement('li');
      const when = w.last_seen ? new Date(w.last_seen*1000).toISOString().replace('T',' ').slice(0,19)+' UTC' : '';
      li.textContent = when ? `${w.name} (${when})` : w.name;
      ul.appendChild(li);
    });
    container.innerHTML = '';
    container.appendChild(ul);
  }

  document.addEventListener('click', async function(ev){
    const link = ev.target.closest('a.show-workers');
    if(!link) return;
    ev.preventDefault();
    const wallet = link.getAttribute('data-wallet') || '';

    const shell = document.createElement('div');
    shell.className = 'workers-modal';
    shell.innerHTML = `
      <div class="modal-card">
        <div class="modal-title">Active workers</div>
        <div class="modal-body" id="wm-list">Loading…</div>
        <div class="modal-actions">
          <button class="button" id="wm-prev">Prev</button>
          <span id="wm-page" style="opacity:.8"></span>
          <button class="button" id="wm-next">Next</button>
          <button class="button" id="wm-close">Close</button>
        </div>
      </div>
    `;
    document.body.appendChild(shell);

    const list    = shell.querySelector('#wm-list');
    const prev    = shell.querySelector('#wm-prev');
    const next    = shell.querySelector('#wm-next');
    const pageLbl = shell.querySelector('#wm-page');
    const closeBtn= shell.querySelector('#wm-close');

    let limit = 50, offset = 0, total = 0;

    async function refresh(){
      list.textContent = 'Loading…';
      const data = await loadWorkers(wallet, limit, offset);
      total = Number(data.total || 0);
      renderList(list, (data && data.workers) ? data.workers : []);
      const page  = Math.floor(offset/limit) + 1;
      const pages = Math.max(1, Math.ceil(total/limit));
      pageLbl.textContent = `Page ${page} / ${pages} • ${total} total`;
      prev.disabled = offset <= 0;
      next.disabled = offset + limit >= total;
    }

    prev.addEventListener('click', ()=> { if(offset >= limit){ offset -= limit; refresh(); } });
    next.addEventListener('click', ()=> { if(offset + limit < total){ offset += limit; refresh(); } });
    closeBtn.addEventListener('click', ()=> closeModal(shell));
    shell.addEventListener('click', (e)=> { if(e.target === shell) closeModal(shell); });

    refresh();
  });
})();

/* =========================
   API Polling (pool + node)
   ========================= */
window.startPolling = function () {
  if (typeof fetchPool === "function") {
    fetchPool();
    setInterval(fetchPool, 15000); // 15s
  }
  if (typeof fetchNode === "function") {
    fetchNode();
    setInterval(fetchNode, 60000); // 60s
  }
};
