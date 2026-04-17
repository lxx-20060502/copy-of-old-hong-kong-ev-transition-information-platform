const dropdown = document.getElementById('supportDropdown');
const trigger = dropdown?.querySelector('.dropdown-trigger');

function closeDropdown() {
  if (!dropdown || !trigger) return;
  dropdown.classList.remove('open');
  trigger.setAttribute('aria-expanded', 'false');
}
function openDropdown() {
  if (!dropdown || !trigger) return;
  dropdown.classList.add('open');
  trigger.setAttribute('aria-expanded', 'true');
}
trigger?.addEventListener('click', (event) => {
  event.stopPropagation();
  dropdown.classList.contains('open') ? closeDropdown() : openDropdown();
});
document.addEventListener('click', (event) => {
  if (!dropdown?.contains(event.target)) closeDropdown();
});
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    closeDropdown();
    closeDeveloperModal();
  }
});

const developerButton = document.getElementById('developerButton');
const developerModal = document.getElementById('developerModal');
const closeModalButton = document.getElementById('closeDeveloperModal');
function openDeveloperModal() {
  if (!developerModal) return;
  developerModal.classList.add('show');
  developerModal.setAttribute('aria-hidden', 'false');
  document.body.classList.add('modal-open');
}
function closeDeveloperModal() {
  if (!developerModal) return;
  developerModal.classList.remove('show');
  developerModal.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('modal-open');
}
developerButton?.addEventListener('click', openDeveloperModal);
closeModalButton?.addEventListener('click', closeDeveloperModal);
developerModal?.addEventListener('click', (event) => {
  if (event.target === developerModal) closeDeveloperModal();
});

const aboutHero = document.getElementById('aboutHero');
const aboutContent = document.getElementById('aboutContent');
const aboutBgTrack = document.querySelector('.about-bg-track');
const aboutBgImage = document.querySelector('.about-bg-image');
const aboutSections = [...document.querySelectorAll('.about-group[id]')];
const tocLinks = [...document.querySelectorAll('.toc-link')];

function syncAboutLayout() {
  if (!aboutHero || !aboutContent || !aboutBgTrack || !aboutBgImage) return;
  const imageHeight = aboutBgTrack.offsetHeight || aboutBgImage.getBoundingClientRect().height;
  const contentHeight = aboutContent.offsetHeight + 26;
  const minHeight = Math.max(imageHeight, contentHeight);
  aboutHero.style.minHeight = `${Math.ceil(minHeight)}px`;
  updateAboutParallax();
}
function updateAboutParallax() {
  if (!aboutHero || !aboutBgTrack) return;
  const rect = aboutHero.getBoundingClientRect();
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight;
  const sectionHeight = aboutHero.offsetHeight;
  const imageHeight = aboutBgTrack.offsetHeight;
  if (!sectionHeight || !imageHeight) return;
  const sectionTop = window.scrollY + rect.top;
  const scrollableSection = Math.max(1, sectionHeight - viewportHeight);
  const traversed = Math.min(Math.max(window.scrollY - sectionTop, 0), scrollableSection);
  const progress = traversed / scrollableSection;
  const maxTranslate = Math.max(0, sectionHeight - imageHeight);
  const translateY = maxTranslate * progress;
  aboutBgTrack.style.transform = `translate3d(0, ${translateY}px, 0)`;
}
function setActiveToc(targetId) {
  if (!tocLinks.length) return;
  tocLinks.forEach((link) => {
    const isActive = link.dataset.target === targetId;
    link.classList.toggle('active', isActive);
    link.setAttribute('aria-current', isActive ? 'true' : 'false');
  });
}
function updateActiveAboutSection() {
  if (!aboutSections.length) return;
  const midpoint = window.innerHeight * 0.34;
  let current = aboutSections[0];
  for (const section of aboutSections) {
    const rect = section.getBoundingClientRect();
    if (rect.top <= midpoint) current = section;
  }
  setActiveToc(current.id);
}
window.addEventListener('load', syncAboutLayout);
window.addEventListener('resize', syncAboutLayout);
window.addEventListener('scroll', () => {
  updateAboutParallax();
  updateActiveAboutSection();
}, { passive: true });
aboutBgImage?.addEventListener('load', syncAboutLayout);
updateActiveAboutSection();
tocLinks.forEach((link) => {
  link.addEventListener('click', () => setActiveToc(link.dataset.target));
});


const transactionConsole = document.getElementById('transactionConsole');
const workflowList = document.getElementById('workflowList');
const runTransactionButton = document.getElementById('runTransactionButton');
const progressCount = document.getElementById('progressCount');
const stepCaption = document.getElementById('stepCaption');
const explanationText = document.getElementById('explanationText');
const errorText = document.getElementById('errorText');
const responseStream = document.getElementById('responseStream');
const transactionForm = document.getElementById('transactionForm');
const connectionChip = document.getElementById('connectionChip');

const defaultTransactionConfig = {
  transactionName: 'Transaction A',
  apiBase: '/api/transaction-a',
  initialStepCaption: 'Ready to execute the full dataspace workflow for Asset 1.',
  successStepCaption: 'Transaction A finished successfully. Asset 1 has been pulled from the live environment.',
  idleResponseText: 'When you run Transaction A, each live request and response will appear here in a Postman format.',
  steps: [
    { key: 'discover', title: 'Discover Asset', note: 'Query the federated catalog for Asset 1.' },
    { key: 'select_offer', title: 'Select Offer', note: 'Match the manufacturer policy from the catalog.' },
    { key: 'negotiate', title: 'Initiate Negotiation', note: 'Send the contract request to the provider.' },
    { key: 'wait_negotiation', title: 'Await Agreement', note: 'Poll until the negotiation is finalized.' },
    { key: 'transfer', title: 'Initiate Transfer', note: 'Start the transfer process for the agreed asset.' },
    { key: 'wait_transfer', title: 'Await Data Plane', note: 'Poll until the transfer process is started.' },
    { key: 'get_edr', title: 'Get EDR', note: 'Retrieve the data address and authorization token.' },
    { key: 'download', title: 'Download Asset 1', note: 'Pull the vehicle market statistics payload.' }
  ]
};

const pageConfig = Object.assign({}, defaultTransactionConfig, window.transactionPageConfig || {});
const transactionSteps = pageConfig.steps || defaultTransactionConfig.steps;

let currentJobId = null;
let jobPollTimer = null;
let cachedLogsSignature = '';

function renderWorkflow(stepStates = {}) {
  if (!workflowList) return;
  workflowList.innerHTML = transactionSteps.map((step) => {
    const state = stepStates[step.key] || 'pending';
    return `
      <article class="workflow-step ${state}" data-step="${step.key}">
        <span class="step-dot"></span>
        <div class="step-meta">
          <div class="step-name">${step.title}</div>
          <div class="step-note">${step.note}</div>
        </div>
      </article>
    `;
  }).join('');
}

function setConnectionStatus(text, statusClass) {
  if (!connectionChip) return;
  connectionChip.textContent = text;
  connectionChip.classList.remove('online', 'offline');
  if (statusClass) connectionChip.classList.add(statusClass);
}


const DEFAULT_LOCAL_BACKEND_ORIGIN = 'http://127.0.0.1:8000';

function computeBackendOrigin() {
  const override = window.localStorage?.getItem('hkEvBackendOrigin')?.trim();
  if (override) return override.replace(/\/$/, '');

  const queryValue = new URLSearchParams(window.location.search).get('backend');
  if (queryValue) {
    const normalized = queryValue.trim().replace(/\/$/, '');
    try {
      window.localStorage?.setItem('hkEvBackendOrigin', normalized);
    } catch (error) {
      /* ignore storage failures */
    }
    return normalized;
  }

  const host = window.location.hostname;
  if (host === '127.0.0.1' || host === 'localhost') {
    return window.location.origin;
  }

  if (host.endsWith('github.io')) {
    return DEFAULT_LOCAL_BACKEND_ORIGIN;
  }

  return window.location.origin;
}

const BACKEND_ORIGIN = computeBackendOrigin();

function apiUrl(path) {
  return `${BACKEND_ORIGIN}${path}`;
}

async function checkBackendHealth() {
  if (!connectionChip) return;
  try {
    const response = await fetch(apiUrl('/api/health'));
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    setConnectionStatus(`Backend status: ${data.status}. Ready for live execution.`, 'online');
  } catch (error) {
    setConnectionStatus('Backend status: offline. Start the local Python server before running the transaction.', 'offline');
  }
}

function collectFormData() {
  const formData = new FormData(transactionForm);
  return Object.fromEntries(formData.entries());
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function formatJson(value) {
  if (value === undefined || value === null || value === '') return 'No payload.';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function renderLogs(logs = []) {
  if (!responseStream) return;
  const signature = JSON.stringify(logs.map((log) => [log.step, log.status, log.summary]));
  if (signature === cachedLogsSignature) return;
  cachedLogsSignature = signature;

  if (!logs.length) {
    responseStream.innerHTML = `
      <article class="response-card">
        <div class="response-head">
          <div class="response-step-title">Awaiting execution</div>
          <div class="response-status">idle</div>
        </div>
        <div class="response-explanation">${escapeHtml(pageConfig.idleResponseText || defaultTransactionConfig.idleResponseText)}</div>
      </article>
    `;
    return;
  }

  responseStream.innerHTML = logs.map((log) => `
    <article class="response-card">
      <div class="response-head">
        <div class="response-step-title">${escapeHtml(log.title || log.step)}</div>
        <div class="response-status ${escapeHtml(log.status || 'pending')}">${escapeHtml(log.status || 'pending')}</div>
      </div>
      <div class="response-explanation">${escapeHtml(log.summary || log.explanation || 'No summary available yet.')}</div>
      <div class="response-meta">
        ${log.request_method ? `<div class="meta-line"><strong>Request:</strong> ${escapeHtml(log.request_method)} ${escapeHtml(log.request_url || '')}</div>` : ''}
        ${log.response_status ? `<div class="meta-line"><strong>HTTP:</strong> ${escapeHtml(log.response_status)}</div>` : ''}
      </div>
      <pre class="code-block">${escapeHtml(formatJson(log.request_body))}</pre>
      <pre class="code-block">${escapeHtml(formatJson(log.response_body))}</pre>
    </article>
  `).join('');
}

function updateConsole(job) {
  const stepStates = {};
  transactionSteps.forEach((step) => { stepStates[step.key] = 'pending'; });
  (job.steps || []).forEach((step) => {
    stepStates[step.key] = step.status;
  });
  renderWorkflow(stepStates);

  const completedCount = (job.steps || []).filter((step) => step.status === 'completed').length;
  if (progressCount) progressCount.textContent = `${completedCount}/${transactionSteps.length}`;

  if (stepCaption) {
    stepCaption.textContent = job.current_message || (job.status === 'completed'
      ? (pageConfig.successStepCaption || defaultTransactionConfig.successStepCaption)
      : (pageConfig.initialStepCaption || defaultTransactionConfig.initialStepCaption));
  }

  if (explanationText) {
    explanationText.textContent = job.current_explanation || 'The page is waiting for you to start the workflow. Once the run begins, this panel explains what the system is doing in plain language at each stage.';
  }

  if (errorText) {
    errorText.textContent = job.error || 'No errors yet.';
  }

  renderLogs(job.logs || []);
}

async function pollJob(jobId) {
  if (!jobId) return;
  try {
    const response = await fetch(apiUrl(`${pageConfig.apiBase}/jobs/${jobId}`));
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const job = await response.json();
    updateConsole(job);
    if (job.status === 'completed' || job.status === 'failed') {
      clearInterval(jobPollTimer);
      jobPollTimer = null;
      runTransactionButton.disabled = false;
    }
  } catch (error) {
    if (errorText) errorText.textContent = `Polling failed: ${error.message}`;
    clearInterval(jobPollTimer);
    jobPollTimer = null;
    runTransactionButton.disabled = false;
  }
}

async function startTransactionRun() {
  if (!transactionForm || !runTransactionButton) return;
  runTransactionButton.disabled = true;
  errorText.textContent = 'No errors yet.';
  cachedLogsSignature = '';
  renderLogs([]);
  const payload = collectFormData();

  try {
    const response = await fetch(apiUrl(`${pageConfig.apiBase}/start`), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `HTTP ${response.status}`);
    }
    const data = await response.json();
    currentJobId = data.job_id;
    clearInterval(jobPollTimer);
    jobPollTimer = setInterval(() => pollJob(currentJobId), 1200);
    await pollJob(currentJobId);
  } catch (error) {
    if (errorText) errorText.textContent = `Start failed: ${error.message}`;
    runTransactionButton.disabled = false;
  }
}

if (transactionConsole) {
  renderWorkflow();
  renderLogs([]);
  checkBackendHealth();
  runTransactionButton?.addEventListener('click', startTransactionRun);
}
