// Shared default page size used by every paginated table workflow in the web UI.
const DEFAULT_PAGE_SIZE = 50;

// Shared selectable page sizes so users can tune table density consistently across pages.
const PAGE_SIZE_OPTIONS = [25, 50, 100, 200];

async function postJson(url, payload) {
  // Submit JSON payloads and gracefully surface non-JSON server errors.
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const rawBody = await response.text();
  let data;
  try {
    data = rawBody ? JSON.parse(rawBody) : {};
  } catch {
    throw new Error(rawBody || response.statusText || 'Unexpected server response');
  }
  if (!response.ok || !data.ok) {
    throw new Error(data.error || data.detail || response.statusText);
  }
  return data.data;
}

async function fetchJson(url, init = undefined) {
  // Load JSON payloads while handling plain-text backend errors without JSON parse crashes.
  const response = await fetch(url, init);
  const rawBody = await response.text();
  let data;
  try {
    data = rawBody ? JSON.parse(rawBody) : {};
  } catch {
    throw new Error(rawBody || response.statusText || 'Unexpected server response');
  }
  if (!response.ok || !data.ok) {
    throw new Error(data.error || data.detail || response.statusText);
  }
  return data.data;
}

async function fetchSetSkus(setCode) {
  const data = await fetchJson(`/api/sets/${encodeURIComponent(setCode)}`);
  return data.sku_list || [];
}

function clearElement(element) {
  while (element.firstChild) {
    element.removeChild(element.firstChild);
  }
}

function buildTable(container, headers, rows, emptyMessage) {
  const table = document.createElement('table');
  if (headers.length > 0) {
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    headers.forEach((header) => {
      const th = document.createElement('th');
      th.textContent = header;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
  }
  const tbody = document.createElement('tbody');
  if (rows.length === 0) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = headers.length || 1;
    cell.textContent = emptyMessage;
    row.appendChild(cell);
    tbody.appendChild(row);
  } else {
    rows.forEach((rowData) => {
      const row = document.createElement('tr');
      rowData.forEach((cellValue) => {
        const cell = document.createElement('td');
        if (cellValue instanceof HTMLElement) {
          cell.appendChild(cellValue);
        } else {
          cell.textContent = cellValue ?? '';
        }
        row.appendChild(cell);
      });
      tbody.appendChild(row);
    });
  }
  table.appendChild(tbody);
  clearElement(container);
  container.appendChild(table);
  decorateReusableTable(table, 0);
}


function formatPercent(value) {
  if (value === null || value === undefined || value === '') {
    return '';
  }
  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
    return value;
  }
  if (Number.isInteger(numeric)) {
    return `${numeric}%`;
  }
  return `${numeric.toFixed(2)}%`;
}

function parseList(value, separator = ',') {
  return value
    .split(separator)
    .map((item) => item.trim())
    .filter(Boolean);
}

// Format timestamps into HH:MM:SS  - DD/MM/YYYY so every table shows one global date pattern.
function formatTimestampForTable(rawValue) {
  if (!rawValue) return '';
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) {
    // Keep raw text visible when a value is not parseable as a JavaScript date.
    return String(rawValue);
  }
  const pad2 = (value) => String(value).padStart(2, '0');
  const hours = pad2(parsed.getHours());
  const minutes = pad2(parsed.getMinutes());
  const seconds = pad2(parsed.getSeconds());
  const day = pad2(parsed.getDate());
  const month = pad2(parsed.getMonth() + 1);
  const year = parsed.getFullYear();
  return `${hours}:${minutes}:${seconds}  - ${day}/${month}/${year}`;
}

// Normalize two-letter code input values (e.g. "ab" -> "AB") and validate strict A-Z format.
function normalizeTwoLetterCode(rawValue) {
  const normalized = (rawValue || '').toString().trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(normalized)) {
    throw new Error('Code must be exactly two letters (A-Z).');
  }
  return normalized;
}

// Build generic prev/next pagination wiring with page labels and disabled state handling.
function updatePagerControls({ prevButton, nextButton, label, page, total, pageSize }) {
  const totalPages = Math.max(1, Math.ceil((total || 0) / pageSize));
  if (label) {
    label.textContent = `Page ${page} of ${totalPages}`;
  }
  if (prevButton) {
    prevButton.disabled = page <= 1;
  }
  if (nextButton) {
    nextButton.disabled = page >= totalPages;
  }
  return totalPages;
}


// Wrap a table with the reusable scroll shell and apply sticky first-column behavior.
function decorateReusableTable(table, stickyColumnIndex = 0) {
  if (!table) return;
  table.classList.add('js-reusable-table');
  const parent = table.parentElement;
  if (parent && !parent.classList.contains('table-scroll-shell')) {
    const shell = document.createElement('div');
    shell.className = 'table-scroll-shell';
    parent.insertBefore(shell, table);
    shell.appendChild(table);
  }
  const headRows = Array.from(table.querySelectorAll('thead tr'));
  headRows.forEach((row) => {
    const cells = row.children;
    if (cells[stickyColumnIndex]) {
      cells[stickyColumnIndex].classList.add('sticky-first-col');
    }
  });
  const bodyRows = Array.from(table.querySelectorAll('tbody tr'));
  bodyRows.forEach((row) => {
    const cells = row.children;
    if (cells[stickyColumnIndex]) {
      cells[stickyColumnIndex].classList.add('sticky-first-col');
    }
  });
}

// Build a shared page-size selector and wire it to table reload callbacks for consistency.
function ensurePageSizeSelector(container, currentSize, onChange) {
  if (!container) return;
  let select = container.querySelector('.js-page-size-select');
  if (!select) {
    const label = document.createElement('label');
    label.textContent = 'Rows per page';
    select = document.createElement('select');
    select.className = 'js-page-size-select';
    PAGE_SIZE_OPTIONS.forEach((size) => {
      const option = document.createElement('option');
      option.value = String(size);
      option.textContent = String(size);
      select.appendChild(option);
    });
    label.appendChild(select);
    container.appendChild(label);
    container.classList.add('table-pagination-controls');
    select.addEventListener('change', () => {
      onChange(Number(select.value));
    });
  }
  select.value = String(currentSize);
}

function attachIngredientForm() {
  const form = document.getElementById('ingredient-form');
  if (!form) return;
  const status = document.getElementById('ingredient-form-status');
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const msdsFile = formData.get('msds_file');
    formData.delete('msds_file');
    const payload = Object.fromEntries(formData.entries());
    payload.category_code = Number(payload.category_code);
    payload.pack_size_value = Number(payload.pack_size_value);
    try {
      status.textContent = 'Creating SKU...';
      const created = await postJson('/api/ingredients', payload);
      const sku = created?.sku || created?.ingredient?.sku;
      if (msdsFile && msdsFile.size > 0 && sku) {
        status.textContent = 'Uploading MSDS...';
        const uploadData = new FormData();
        uploadData.append('file', msdsFile);
        uploadData.append('replace_confirmed', 'true');
        const uploadRes = await fetch(`/api/ingredients/${encodeURIComponent(sku)}/msds`, { method: 'POST', body: uploadData });
        const uploadJson = await uploadRes.json();
        if (!uploadRes.ok || !uploadJson.ok) {
          throw new Error(uploadJson.error || uploadJson.detail || 'Error uploading MSDS');
        }
      }
      status.textContent = 'Saved successfully.';
      window.location.reload();
    } catch (error) {
      status.textContent = '';
      alert(error.message);
    }
  });
}

function attachIngredientMsdsForm() {
  const form = document.getElementById('ingredient-msds-form');
  if (!form) return;
  const status = document.getElementById('ingredient-msds-status');
  const sku = form.dataset.sku;
  const hasMsds = form.dataset.hasMsds === '1';

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const file = formData.get('msds_file');
    const replaceChecked = formData.get('replace_confirmed') === 'on';

    if (!file || file.size === 0) {
      alert('Please select a PDF file.');
      return;
    }
    if (hasMsds && !replaceChecked) {
      alert('Please confirm replacement to overwrite existing MSDS.');
      return;
    }

    const uploadData = new FormData();
    uploadData.append('file', file);
    uploadData.append('replace_confirmed', replaceChecked ? 'true' : 'false');

    try {
      status.textContent = 'Uploading...';
      const response = await fetch(`/api/ingredients/${encodeURIComponent(sku)}/msds`, { method: 'POST', body: uploadData });
      const data = await response.json();
      if (!response.ok || !data.ok) {
        throw new Error(data.error || data.detail || 'Upload failed');
      }
      status.textContent = 'Upload successful.';
      window.location.href = '/ingredients';
    } catch (error) {
      status.textContent = '';
      alert(error.message);
    }
  });
}

function attachIngredientImportForm() {
  const form = document.getElementById('ingredient-import-form');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const msdsFile = formData.get('msds_file');
    formData.delete('msds_file');
    const payload = Object.fromEntries(formData.entries());
    payload.category_code = Number(payload.category_code);
    payload.pack_size_value = Number(payload.pack_size_value);
    try {
      const created = await postJson('/api/ingredients/import', payload);
      const sku = created?.sku || payload.sku;
      if (msdsFile && msdsFile.size > 0 && sku) {
        const uploadData = new FormData();
        uploadData.append('file', msdsFile);
        uploadData.append('replace_confirmed', 'true');
        const uploadRes = await fetch(`/api/ingredients/${encodeURIComponent(sku)}/msds`, { method: 'POST', body: uploadData });
        const uploadJson = await uploadRes.json();
        if (!uploadRes.ok || !uploadJson.ok) {
          throw new Error(uploadJson.error || uploadJson.detail || 'Error uploading MSDS');
        }
      }
      window.location.href = '/ingredients';
    } catch (error) {
      alert(error.message);
    }
  });
}

function attachBatchForm() {
  const form = document.getElementById('batch-form');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const coaFile = formData.get('coa_file');
    formData.delete('coa_file');
    const payload = Object.fromEntries(formData.entries());
    if (payload.quantity_value === '') {
      delete payload.quantity_value;
    } else if (payload.quantity_value !== undefined) {
      payload.quantity_value = Number(payload.quantity_value);
    }
    if (payload.quantity_unit === '') {
      delete payload.quantity_unit;
    }
    try {
      await postJson('/api/ingredient_batches', payload);
      if (coaFile && coaFile.size > 0) {
        const uploadData = new FormData();
        uploadData.append('file', coaFile);
        uploadData.append('replace_confirmed', 'true');
        const uploadRes = await fetch(
          `/api/ingredient_batches/${encodeURIComponent(payload.sku)}/${encodeURIComponent(payload.ingredient_batch_code)}/coa`,
          { method: 'POST', body: uploadData }
        );
        const uploadJson = await uploadRes.json();
        if (!uploadRes.ok || !uploadJson.ok) {
          throw new Error(uploadJson.error || uploadJson.detail || 'Error uploading CoA');
        }
      }
      form.reset();
      alert('Batch created');
    } catch (error) {
      alert(error.message);
    }
  });
}

// Wire the batch lookup form to load batches and render each batch code as a clickable detail link.

function attachBatchLookupForm() {
  const form = document.getElementById('batch-lookup-form');
  if (!form) return;
  const output = document.getElementById('batch-results');
  const prevButton = document.getElementById('batches-prev-page');
  const nextButton = document.getElementById('batches-next-page');
  const pageLabel = document.getElementById('batches-page-label');
  const paginationContainer = document.getElementById('batches-pagination');
  let pageSize = DEFAULT_PAGE_SIZE;
  let page = 1;
  let lastTotal = 0;

  // Load paginated batches for current filters and render rows into the table body.
  async function loadBatches(targetPage) {
    const formData = new FormData(form);
    const params = new URLSearchParams();
    const sku = (formData.get('sku') || '').toString().trim();
    const batchCode = (formData.get('batch_code') || '').toString().trim();
    if (sku) {
      params.set('sku', sku);
    }
    if (batchCode) {
      params.set('batch_code', batchCode);
    }
    params.set('page', String(targetPage));
    params.set('page_size', String(pageSize));

    const response = await fetch(`/api/ingredient_batches?${params.toString()}`);
    const data = await response.json();
    if (!data.ok) {
      throw new Error(data.error || 'Error loading batches');
    }

    const items = data.data.items || [];
    page = Number(data.data.page || targetPage);
    lastTotal = Number(data.data.total || 0);
    clearElement(output);

    if (items.length === 0) {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 7;
      cell.textContent = 'No batches found.';
      row.appendChild(cell);
      output.appendChild(row);
    } else {
      items.forEach((item) => {
        const row = document.createElement('tr');
        const skuCell = document.createElement('td');
        skuCell.textContent = item.sku;
        const codeCell = document.createElement('td');
        const batchLink = document.createElement('a');
        batchLink.href = `/batches/${encodeURIComponent(item.sku)}/${encodeURIComponent(item.ingredient_batch_code)}`;
        batchLink.textContent = item.ingredient_batch_code;
        codeCell.appendChild(batchLink);
        const receivedCell = document.createElement('td');
        receivedCell.textContent = formatTimestampForTable(item.received_at);
        const notesCell = document.createElement('td');
        notesCell.textContent = item.notes || '';
        const quantityCell = document.createElement('td');
        quantityCell.textContent = item.quantity_value !== null && item.quantity_value !== undefined && item.quantity_value !== '' ? `${item.quantity_value} ${item.quantity_unit || ''}`.trim() : '';
        const ownerCell = document.createElement('td');
        ownerCell.textContent = item.created_by || 'Unknown';
        const coaCell = document.createElement('td');
        if (item.spec_object_path) {
          const link = document.createElement('a');
          link.href = '#';
          link.textContent = 'CoA';
          link.addEventListener('click', async (e) => {
            e.preventDefault();
            const response = await fetch(`/api/ingredient_batches/${encodeURIComponent(item.sku)}/${encodeURIComponent(item.ingredient_batch_code)}/spec/download_url`);
            const specData = await response.json();
            if (!response.ok || !specData.ok) {
              alert(specData.error || specData.detail || 'Unable to load CoA');
              return;
            }
            window.open(specData.data.download_url, '_blank', 'noopener');
          });
          coaCell.appendChild(link);
        } else {
          coaCell.textContent = 'None';
        }
        row.appendChild(skuCell);
        row.appendChild(codeCell);
        row.appendChild(receivedCell);
        row.appendChild(notesCell);
        row.appendChild(quantityCell);
        row.appendChild(ownerCell);
        row.appendChild(coaCell);
        output.appendChild(row);
      });
    }

    updatePagerControls({
      prevButton,
      nextButton,
      label: pageLabel,
      page,
      total: lastTotal,
      pageSize,
    });
    const table = output.closest('table');
    decorateReusableTable(table, 0);
  }

  // Start from page 1 whenever the user submits new filter values.
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await loadBatches(1);
    } catch (error) {
      alert(error.message);
    }
  });

  // Wire previous-page navigation while preserving active filter values from the form.
  if (prevButton) {
    prevButton.addEventListener('click', () => {
      if (page <= 1) return;
      loadBatches(page - 1).catch((error) => alert(error.message));
    });
  }

  // Wire next-page navigation while preserving active filter values from the form.
  if (nextButton) {
    nextButton.addEventListener('click', () => {
      const totalPages = Math.max(1, Math.ceil(lastTotal / pageSize));
      if (page >= totalPages) return;
      loadBatches(page + 1).catch((error) => alert(error.message));
    });
  }

  // Add shared page-size control for consistent pagination UX.
  ensurePageSizeSelector(paginationContainer, pageSize, (nextSize) => {
    pageSize = nextSize;
    loadBatches(1).catch((error) => alert(error.message));
  });

  // Load the initial all-batches page on first render for immediate visibility.
  loadBatches(1).catch((error) => {
    alert(error.message);
  });
}

function attachSetForm() {
  const form = document.getElementById('set-form');
  if (!form) return;
  const addButton = document.getElementById('add-sku-select');
  const selectsContainer = document.getElementById('sku-selects');
  const template = document.getElementById('sku-select-template');
  const filterForm = document.getElementById('sets-filter-form');
  const output = document.getElementById('sets-results');
  const prevButton = document.getElementById('sets-prev-page');
  const nextButton = document.getElementById('sets-next-page');
  const pageLabel = document.getElementById('sets-page-label');
  const paginationContainer = document.getElementById('sets-pagination');
  let pageSize = DEFAULT_PAGE_SIZE;
  let page = 1;
  let lastTotal = 0;

  // Keep dynamic SKU field growth so users can include more than the six default selectors.
  if (addButton && selectsContainer && template) {
    addButton.addEventListener('click', () => {
      const fragment = template.content.cloneNode(true);
      selectsContainer.appendChild(fragment);
    });
  }

  // Submit set creation payload after collecting all non-empty selected SKU values.
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const selects = Array.from(form.querySelectorAll('select[name="skus"]'));
    const skus = selects.map((select) => select.value.trim()).filter(Boolean);
    if (skus.length === 0) {
      alert('Select at least one SKU.');
      return;
    }
    try {
      await postJson('/api/sets', { skus });
      await loadSets(1);
    } catch (error) {
      alert(error.message);
    }
  });

  // Render the paged set list with owner and created timestamp columns.
  async function loadSets(targetPage) {
    const params = new URLSearchParams();
    const q = filterForm ? (new FormData(filterForm).get('q') || '').toString().trim() : '';
    if (q) {
      params.set('q', q);
    }
    params.set('page', String(targetPage));
    params.set('page_size', String(pageSize));

    const response = await fetch(`/api/sets?${params.toString()}`);
    const data = await response.json();
    if (!response.ok || !data.ok) {
      throw new Error(data.error || data.detail || 'Error loading sets');
    }

    const items = data.data.items || [];
    page = Number(data.data.page || targetPage);
    lastTotal = Number(data.data.total || 0);
    clearElement(output);

    if (items.length === 0) {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 4;
      cell.textContent = 'No sets found.';
      row.appendChild(cell);
      output.appendChild(row);
    } else {
      items.forEach((item) => {
        const row = document.createElement('tr');
        const setCodeCell = document.createElement('td');
        setCodeCell.textContent = item.set_code || '';
        const ownerCell = document.createElement('td');
        ownerCell.textContent = item.created_by || 'Unknown';
        const createdCell = document.createElement('td');
        createdCell.textContent = formatTimestampForTable(item.created_at);
        const skusCell = document.createElement('td');
        skusCell.textContent = Array.isArray(item.sku_list) ? item.sku_list.join(', ') : '';
        row.appendChild(setCodeCell);
        row.appendChild(ownerCell);
        row.appendChild(createdCell);
        row.appendChild(skusCell);
        output.appendChild(row);
      });
    }

    updatePagerControls({ prevButton, nextButton, label: pageLabel, page, total: lastTotal, pageSize });
    const table = output.closest('table');
    decorateReusableTable(table, 0);
  }

  // Restart pagination when search filters are changed and submitted.
  if (filterForm) {
    filterForm.addEventListener('submit', (event) => {
      event.preventDefault();
      loadSets(1).catch((error) => alert(error.message));
    });
  }

  // Step backward through set result pages while keeping search query state.
  if (prevButton) {
    prevButton.addEventListener('click', () => {
      if (page <= 1) return;
      loadSets(page - 1).catch((error) => alert(error.message));
    });
  }

  // Step forward through set result pages while keeping search query state.
  if (nextButton) {
    nextButton.addEventListener('click', () => {
      const totalPages = Math.max(1, Math.ceil(lastTotal / pageSize));
      if (page >= totalPages) return;
      loadSets(page + 1).catch((error) => alert(error.message));
    });
  }

  // Add shared page-size control for consistent pagination UX.
  ensurePageSizeSelector(paginationContainer, pageSize, (nextSize) => {
    pageSize = nextSize;
    loadSets(1).catch((error) => alert(error.message));
  });

  // Load the first page immediately so existing sets are always visible by default.
  loadSets(1).catch((error) => {
    alert(error.message);
  });
}
function attachDryWeightForm() {
  const form = document.getElementById('dry-weight-form');
  if (!form) return;
  const loadButton = document.getElementById('load-dry-weight-set');
  const entryContainer = document.getElementById('dry-weight-entry');
  const totalOutput = document.getElementById('dry-weight-total');

  function renderSetTable(skus) {
    if (skus.length === 0) {
      buildTable(entryContainer, ['SKU'], [], 'No SKUs found for this set.');
      totalOutput.textContent = '';
      return;
    }
    const inputRow = skus.map((sku) => {
      const input = document.createElement('input');
      input.type = 'number';
      input.min = '0';
      input.max = '100';
      input.step = '0.01';
      input.required = true;
      input.dataset.sku = sku;
      input.className = 'dry-weight-input';
      input.addEventListener('input', updateTotal);
      return input;
    });
    buildTable(entryContainer, skus, [inputRow], 'No SKUs found for this set.');
    updateTotal();
  }

  function updateTotal() {
    const inputs = Array.from(entryContainer.querySelectorAll('input.dry-weight-input'));
    const total = inputs.reduce((sum, input) => sum + Number(input.value || 0), 0);
    totalOutput.textContent = `Total: ${total.toFixed(2)}%`;
    totalOutput.className = Math.abs(total - 100) < 0.00001 ? 'total-ok' : 'total-error';
  }

  async function loadSet() {
    let setCode;
    try {
      // Keep set-code entry strict and case-insensitive by normalizing to uppercase before lookup.
      setCode = normalizeTwoLetterCode(new FormData(form).get('set_code'));
    } catch (error) {
      alert(error.message);
      return;
    }
    const skus = await fetchSetSkus(setCode);
    renderSetTable(skus);
  }

  if (loadButton) {
    loadButton.addEventListener('click', () => {
      loadSet().catch((error) => {
        alert(error.message || 'Error loading set');
      });
    });
  }

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    let setCode;
    try {
      // Normalize set code before create so lowercase input (e.g. ab) posts correctly as AB.
      setCode = normalizeTwoLetterCode(new FormData(form).get('set_code'));
    } catch (error) {
      alert(error.message);
      return;
    }
    const inputs = Array.from(entryContainer.querySelectorAll('input.dry-weight-input'));
    if (!setCode || inputs.length === 0) {
      alert('Load a set before creating a variant.');
      return;
    }
    const items = [];
    for (const input of inputs) {
      if (input.value === '') {
        alert('Enter a dry weight for each SKU.');
        return;
      }
      const rounded = Number(Number(input.value).toFixed(2));
      items.push({ sku: input.dataset.sku, wt_percent: rounded });
    }
    const total = items.reduce((sum, item) => sum + item.wt_percent, 0);
    if (Math.abs(total - 100) > 0.00001) {
      alert('Dry weights must add up to exactly 100.00%.');
      return;
    }
    try {
      await postJson('/api/dry_weights', { set_code: setCode, items });
      form.reset();
      buildTable(entryContainer, ['SKU'], [], 'Load a set to enter dry weights.');
      totalOutput.textContent = '';
      alert('Dry weight variant created');
    } catch (error) {
      alert(error.message);
    }
  });
}

function attachDryWeightLookupForm() {
  const form = document.getElementById('dry-weight-lookup-form');
  if (!form) return;
  const output = document.getElementById('dry-weight-results');
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    let setCode;
    try {
      // Normalize lookup code to make AB and ab searches behave identically in dry-weight lookup.
      setCode = normalizeTwoLetterCode(new FormData(form).get('set_code'));
    } catch (error) {
      alert(error.message);
      return;
    }
    const response = await fetch(`/api/dry_weights?set_code=${encodeURIComponent(setCode)}`);
    const data = await response.json();
    if (!data.ok) {
      alert(data.error || 'Error loading weights');
      return;
    }
    const variants = data.data.items || [];
    const skuSet = new Set();
    variants.forEach((variant) => {
      (variant.items || []).forEach((item) => skuSet.add(item.sku));
    });
    const skuList = Array.from(skuSet);
    const headers = ['Set code', 'Weight code', ...skuList];
    const rows = variants.map((variant) => {
      const skuMap = new Map();
      (variant.items || []).forEach((item) => {
        skuMap.set(item.sku, item.wt_percent);
      });
      return [
        variant.set_code || '',
        variant.weight_code || '',
        ...skuList.map((sku) => formatPercent(skuMap.get(sku))),
      ];
    });
    buildTable(output, headers, rows, 'No variants found.');
  });
}

function attachBatchVariantForm() {
  const form = document.getElementById('batch-variant-form');
  if (!form) return;
  const loadButton = document.getElementById('load-batch-items');
  const itemsContainer = document.getElementById('batch-variant-items');

  async function loadBatchItems() {
    const formData = new FormData(form);
    let setCode;
    let weightCode;
    try {
      // Normalize lowercase entry to uppercase and enforce strict two-letter code format.
      setCode = normalizeTwoLetterCode(formData.get('set_code'));
      weightCode = normalizeTwoLetterCode(formData.get('weight_code'));
    } catch (error) {
      alert(error.message);
      return;
    }
    if (!setCode || !weightCode) {
      alert('Enter a set code and weight code to load a set.');
      return;
    }
    const weightsResponse = await fetch(`/api/dry_weights?set_code=${encodeURIComponent(setCode)}`);
    const weightsData = await weightsResponse.json();
    if (!weightsData.ok) {
      alert(weightsData.error || 'Error loading weight variants');
      return;
    }
    const variants = weightsData.data.items || [];
    const variant = variants.find((row) => row.weight_code === weightCode);
    if (!variant) {
      buildTable(itemsContainer, ['SKU', 'Batch'], [], 'No matching weight variant found.');
      return;
    }
    const skuList = (variant.items || []).map((item) => item.sku);
    const batchLists = await Promise.all(
      skuList.map(async (sku) => {
        const response = await fetch(`/api/ingredient_batches?sku=${encodeURIComponent(sku)}`);
        const data = await response.json();
        if (!data.ok) {
          return { sku, batches: [], error: data.error };
        }
        return { sku, batches: data.data.items || [], error: null };
      })
    );
    const rows = batchLists.map(({ sku, batches, error }) => {
      const select = document.createElement('select');
      select.dataset.sku = sku;
      const placeholder = document.createElement('option');
      placeholder.value = '';
      placeholder.textContent = error ? 'Error loading batches' : 'Select batch';
      select.appendChild(placeholder);
      batches.forEach((batch) => {
        const option = document.createElement('option');
        option.value = batch.ingredient_batch_code;
        option.textContent = batch.ingredient_batch_code;
        select.appendChild(option);
      });
      return [sku, select];
    });
    buildTable(itemsContainer, ['SKU', 'Batch'], rows, 'No SKUs found for this weight variant.');
  }

  if (loadButton) {
    loadButton.addEventListener('click', () => {
      loadBatchItems().catch((error) => {
        alert(error.message || 'Error loading batch items');
      });
    });
  }
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    let setCode;
    let weightCode;
    try {
      // Normalize lowercase entry to uppercase and enforce strict two-letter code format.
      setCode = normalizeTwoLetterCode(formData.get('set_code'));
      weightCode = normalizeTwoLetterCode(formData.get('weight_code'));
    } catch (error) {
      alert(error.message);
      return;
    }
    const selects = Array.from(itemsContainer.querySelectorAll('select[data-sku]'));
    if (selects.length === 0) {
      alert('Load set items before creating a variant.');
      return;
    }
    const items = [];
    for (const select of selects) {
      const batchCode = select.value;
      if (!batchCode) {
        alert('Select a batch for each SKU.');
        return;
      }
      items.push({ sku: select.dataset.sku, ingredient_batch_code: batchCode });
    }
    try {
      await postJson('/api/batch_variants', { set_code: setCode, weight_code: weightCode, items });
      form.reset();
      buildTable(itemsContainer, ['SKU', 'Batch'], [], 'No SKUs loaded.');
      alert('Batch variant created');
    } catch (error) {
      alert(error.message);
    }
  });
}

function attachBatchVariantLookupForm() {
  const form = document.getElementById('batch-variant-lookup-form');
  if (!form) return;
  const output = document.getElementById('batch-variant-results');
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    let setCode;
    let weightCode;
    try {
      // Normalize lowercase entry to uppercase and enforce strict two-letter code format.
      setCode = normalizeTwoLetterCode(formData.get('set_code'));
      weightCode = normalizeTwoLetterCode(formData.get('weight_code'));
    } catch (error) {
      alert(error.message);
      return;
    }
    const response = await fetch(
      `/api/batch_variants?set_code=${encodeURIComponent(setCode)}&weight_code=${encodeURIComponent(weightCode)}`
    );
    const data = await response.json();
    if (!data.ok) {
      alert(data.error || 'Error loading batch variants');
      return;
    }
    const variants = data.data.items || [];
    const skuSet = new Set();
    variants.forEach((variant) => {
      (variant.items || []).forEach((item) => skuSet.add(item.sku));
    });
    const skuList = Array.from(skuSet);
    const headers = ['Set code', 'Weight code', 'Batch variant code', ...skuList];
    const rows = variants.map((variant) => {
      const skuMap = new Map();
      (variant.items || []).forEach((item) => {
        skuMap.set(item.sku, item.ingredient_batch_code);
      });
      return [
        variant.set_code || '',
        variant.weight_code || '',
        variant.batch_variant_code || '',
        ...skuList.map((sku) => skuMap.get(sku) || ''),
      ];
    });
    buildTable(output, headers, rows, 'No batch variants found.');
  });
}

// Build per-SKU map so formulation arrays can be aligned by sku key for rowspans.
function mapItemsBySku(items, valueKey, skuList = []) {
  const map = new Map();
  // Handle array payloads from BigQuery views where each entry is an object (or struct-like value).
  if (Array.isArray(items)) {
    items.forEach((item, index) => {
      const fallbackSku = skuList[index];
      const sku = (item?.sku || fallbackSku || '').toString().trim();
      if (!sku) return;
      const value = item && typeof item === 'object' && valueKey in item ? item[valueKey] : item;
      map.set(sku, value);
    });
    return map;
  }
  // Handle object payloads keyed by SKU in case an environment serializes maps as JSON objects.
  if (items && typeof items === 'object') {
    Object.entries(items).forEach(([rawSku, rawValue]) => {
      const sku = (rawSku || '').toString().trim();
      if (!sku) return;
      const value = rawValue && typeof rawValue === 'object' && valueKey in rawValue ? rawValue[valueKey] : rawValue;
      map.set(sku, value);
    });
  }
  return map;
}

// Create reusable hover-only copy control used by formulation and location code values.
function createCopyTextBlock(copyValue, className = '') {
  const wrapper = document.createElement('div');
  wrapper.className = `copy-text-block ${className}`.trim();
  const value = document.createElement('div');
  value.className = 'copy-text-value';
  value.textContent = copyValue || '';
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'copy-text-button';
  button.textContent = 'Copy text';
  button.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(copyValue || '');
      button.textContent = 'Copied';
      window.setTimeout(() => {
        button.textContent = 'Copy text';
      }, 1200);
    } catch (error) {
      button.textContent = 'Copy failed';
      window.setTimeout(() => {
        button.textContent = 'Copy text';
      }, 1200);
    }
  });
  wrapper.appendChild(value);
  wrapper.appendChild(button);
  return wrapper;
}

// Build table cell content that is both human-readable and directly clickable for external links.
function createUrlCellContent(urlValue) {
  const wrapper = document.createElement('div');
  const value = (urlValue || '').toString().trim();
  if (!value) {
    wrapper.textContent = '';
    return { wrapper, input: null };
  }
  const anchor = document.createElement('a');
  anchor.href = value;
  anchor.target = '_blank';
  anchor.rel = 'noopener noreferrer';
  anchor.textContent = value;
  wrapper.appendChild(anchor);
  return { wrapper, input: null };
}

// Render formulation rows with HTML rowspans so sku/weight/batch lines align like the reference.
function renderFormulationsTable(output, items) {
  clearElement(output);
  const table = document.createElement('table');
  // Mark this table so CSS can target formulation hover behavior without affecting other tables.
  table.classList.add('formulations-table');
  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  ['Formulation', 'Created', 'Owner', 'SKU Count', 'SKUs', 'Dry Weights (%)', 'Batches'].forEach((label) => {
    const th = document.createElement('th');
    th.textContent = label;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);
  const tbody = document.createElement('tbody');

  if (!items.length) {
    const row = document.createElement('tr');
    const cell = document.createElement('td');
    cell.colSpan = 7;
    cell.textContent = 'No formulations found.';
    row.appendChild(cell);
    tbody.appendChild(row);
    table.appendChild(tbody);
    output.appendChild(table);
    decorateReusableTable(table, 0);
    return;
  }

  items.forEach((item) => {
    const skuList = Array.isArray(item.sku_list) ? item.sku_list : parseList(item.sku_list || '');
    // Align dry weights by SKU with a fallback to sku_list ordering to avoid index/key mismatches.
    const weightMap = mapItemsBySku(item.dry_weight_items, 'wt_percent', skuList);
    // Align batch codes by SKU in the same way so each SKU row shows the matching batch.
    const batchMap = mapItemsBySku(item.batch_items, 'ingredient_batch_code', skuList);
    const displaySkus = skuList.length ? skuList : ['—'];
    const lineCount = displaySkus.length;
    const formulationCode = [item.set_code || '', item.weight_code || '', item.batch_variant_code || ''].join(' ').trim();

    displaySkus.forEach((sku, index) => {
      const row = document.createElement('tr');

      if (index === 0) {
        const formulationCell = document.createElement('td');
        formulationCell.rowSpan = lineCount;
        formulationCell.appendChild(createCopyTextBlock(formulationCode, 'formulation-copy'));

        const createdCell = document.createElement('td');
        createdCell.rowSpan = lineCount;
        createdCell.textContent = formatTimestampForTable(item.created_at);

        const ownerCell = document.createElement('td');
        ownerCell.rowSpan = lineCount;
        ownerCell.textContent = item.created_by || 'Unknown';

        const countCell = document.createElement('td');
        countCell.rowSpan = lineCount;
        countCell.textContent = item.sku_count ?? skuList.length;

        row.appendChild(formulationCell);
        row.appendChild(createdCell);
        row.appendChild(ownerCell);
        row.appendChild(countCell);
      }

      const skuCell = document.createElement('td');
      skuCell.classList.add('formulation-detail-cell');
      skuCell.textContent = sku;
      const weightCell = document.createElement('td');
      weightCell.classList.add('formulation-detail-cell');
      weightCell.textContent = sku === '—' ? '—' : formatPercent(weightMap.get(sku));
      const batchCell = document.createElement('td');
      batchCell.classList.add('formulation-detail-cell');
      batchCell.textContent = sku === '—' ? '—' : (batchMap.get(sku) || '');
      row.appendChild(skuCell);
      row.appendChild(weightCell);
      row.appendChild(batchCell);
      tbody.appendChild(row);
    });
  });

  table.appendChild(tbody);
  output.appendChild(table);
}

// Attach formulation filtering with support for filtering by SKU membership.

function attachFormulationsFilterForm() {
  const form = document.getElementById('formulations-filter-form');
  if (!form) return;
  const output = document.getElementById('formulations-results');
  const prevButton = document.getElementById('formulations-prev-page');
  const nextButton = document.getElementById('formulations-next-page');
  const pageLabel = document.getElementById('formulations-page-label');
  const paginationContainer = document.getElementById('formulations-pagination');
  let pageSize = DEFAULT_PAGE_SIZE;
  let page = 1;
  let lastTotal = 0;

  // Fetch formulations with current filters and requested page, then render a formatted summary table.
  async function loadFormulations(targetPage) {
    const params = new URLSearchParams();
    new FormData(form).forEach((value, key) => {
      const trimmed = value.toString().trim();
      if (trimmed) {
        // Normalize all two-letter formulation filters so AB and ab return identical results.
        if (['set_code', 'weight_code', 'batch_variant_code'].includes(key)) {
          try {
            params.append(key, normalizeTwoLetterCode(trimmed));
          } catch (error) {
            throw error;
          }
        } else {
          params.append(key, trimmed);
        }
      }
    });
    params.set('page', String(targetPage));
    params.set('page_size', String(pageSize));

    const response = await fetch(`/api/formulations?${params.toString()}`);
    const data = await response.json();
    if (!data.ok) {
      throw new Error(data.error || 'Error loading formulations');
    }

    const items = data.data.items || [];
    page = Number(data.data.page || targetPage);
    lastTotal = Number(data.data.total || 0);
    renderFormulationsTable(output, items);
    updatePagerControls({ prevButton, nextButton, label: pageLabel, page, total: lastTotal, pageSize });
    const table = output.querySelector('table');
    decorateReusableTable(table, 0);
  }

  // Re-run query from page one whenever filter criteria change via explicit search submit.
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await loadFormulations(1);
    } catch (error) {
      alert(error.message);
    }
  });

  // Navigate backwards while preserving active filter values from the form.
  if (prevButton) {
    prevButton.addEventListener('click', () => {
      if (page <= 1) return;
      loadFormulations(page - 1).catch((error) => alert(error.message));
    });
  }

  // Navigate forwards while preserving active filter values from the form.
  if (nextButton) {
    nextButton.addEventListener('click', () => {
      const totalPages = Math.max(1, Math.ceil(lastTotal / pageSize));
      if (page >= totalPages) return;
      loadFormulations(page + 1).catch((error) => alert(error.message));
    });
  }

  // Add shared page-size control for consistent pagination UX.
  ensurePageSizeSelector(paginationContainer, pageSize, (nextSize) => {
    pageSize = nextSize;
    loadFormulations(1).catch((error) => alert(error.message));
  });

  // Load all formulations (newest-to-oldest from API) on page open with default page size.
  loadFormulations(1).catch((error) => {
    alert(error.message);
  });
}


function formatYyMmDdToDateLabel(codeValue) {
  // Convert YYMMDD values into a readable YYYY-MM-DD label for the date-of-production column.
  const value = (codeValue || '').toString();
  if (!/^\d{6}$/.test(value)) return value;
  const year = `20${value.slice(0, 2)}`;
  const month = value.slice(2, 4);
  const day = value.slice(4, 6);
  return `${year}-${month}-${day}`;
}

function formatDateToYyMmDd(dateValue) {
  // Convert YYYY-MM-DD input values into the requested reversed YYMMDD code format.
  if (!dateValue) return '';
  const [year, month, day] = dateValue.split('-');
  if (!year || !month || !day) return '';
  return `${year.slice(-2)}${month}${day}`;
}

function attachLocationCodePage() {
  const locationForm = document.getElementById('location-code-form');
  if (!locationForm) return;
  const formulationSelect = document.getElementById('location-formulation-select');
  const formulationManual = document.getElementById('location-formulation-manual');
  const partnerSelect = document.getElementById('location-partner-select');
  const dateInput = document.getElementById('location-production-date');
  const dateCodeInput = document.getElementById('location-production-code');
  const output = document.getElementById('location-code-output');
  const tableBody = document.getElementById('location-codes-table-body');
  const prevButton = document.getElementById('location-codes-prev-page');
  const nextButton = document.getElementById('location-codes-next-page');
  const pageLabel = document.getElementById('location-codes-page-label');
  const paginationContainer = document.getElementById('location-codes-pagination');
  const filterForm = document.getElementById('location-codes-filter-form');
  let pageSize = DEFAULT_PAGE_SIZE;
  let page = 1;
  let lastTotal = 0;

  // Parse the formulation code from dropdown/manual input and enforce AB AB AC formatting.
  function parseFormulationCode(rawCode) {
    const parts = (rawCode || '').toString().trim().toUpperCase().split(/\s+/).filter(Boolean);
    if (!parts.length) return null;
    if (parts.length !== 3 || !parts.every((part) => /^[A-Z]{2}$/.test(part))) {
      throw new Error('Formulation code must be exactly three two-letter parts, e.g. AB AB AC.');
    }
    return { set_code: parts[0], weight_code: parts[1], batch_variant_code: parts[2] };
  }

  // Populate formulation dropdown options from active formulation combinations.
  async function loadFormulations() {
    const data = await fetchJson('/api/location_codes/formulations');
    const items = Array.isArray(data.items) ? data.items : [];
    if (!formulationSelect) return;
    clearElement(formulationSelect);
    const first = document.createElement('option');
    first.value = '';
    first.textContent = 'Select formulation';
    formulationSelect.appendChild(first);
    items.forEach((item) => {
      // Support both snake_case and camelCase key names from different backend serializers.
      const code = [item.set_code || item.setCode, item.weight_code || item.weightCode, item.batch_variant_code || item.batchVariantCode].filter(Boolean).join(' ');
      const option = document.createElement('option');
      option.value = code;
      option.textContent = code;
      formulationSelect.appendChild(option);
    });
  }

  // Fetch and render partner-name-first dropdown options with code metadata for location generation.
  async function loadPartners() {
    const data = await fetchJson('/api/location_codes/partners');
    const partners = data.items || [];
    clearElement(partnerSelect);
    const first = document.createElement('option');
    first.value = '';
    first.textContent = 'Select partner';
    partnerSelect.appendChild(first);
    partners.forEach((partner) => {
      const option = document.createElement('option');
      option.value = partner.partner_code;
      // Render machine specification in the option label so similarly named partners remain distinguishable.
      const machineSpecification = (partner.machine_specification || '').toString().trim();
      option.textContent = machineSpecification
        ? `${partner.partner_name} (${machineSpecification})`
        : `${partner.partner_name}`;
      option.dataset.partnerCode = partner.partner_code;
      partnerSelect.appendChild(option);
    });
  }

  // Render paginated location code table rows with owner, partner label, and creation date metadata.
  async function loadLocationCodes(targetPage) {
    const params = new URLSearchParams();
    params.set('page', String(targetPage));
    params.set('page_size', String(pageSize));
    if (filterForm) {
      const query = ((new FormData(filterForm).get('q') || '').toString().trim());
      if (query) params.set('q', query);
    }
    const data = await fetchJson(`/api/location_codes?${params.toString()}`);
    const items = Array.isArray(data.items) ? data.items : [];
    page = Number(data.page || targetPage);
    lastTotal = Number(data.total || 0);
    clearElement(tableBody);

    if (!items.length) {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 4;
      cell.textContent = 'No location codes found.';
      row.appendChild(cell);
      tableBody.appendChild(row);
    } else {
      items.forEach((item) => {
        const row = document.createElement('tr');
        const locationCell = document.createElement('td');
        locationCell.appendChild(createCopyTextBlock(item.location_id || '', 'location-copy'));
        const ownerCell = document.createElement('td');
        ownerCell.textContent = item.created_by || 'Unknown';
        const partnerCell = document.createElement('td');
        const machineSpecification = (item.machine_specification || '').toString().trim();
        partnerCell.textContent = machineSpecification ? `${item.partner_name || 'Unknown'} (${machineSpecification})` : (item.partner_name || 'Unknown');
        const createdCell = document.createElement('td');
        createdCell.textContent = formatTimestampForTable(item.created_at);
        row.append(locationCell, ownerCell, partnerCell, createdCell);
        tableBody.appendChild(row);
      });
    }

    updatePagerControls({ prevButton, nextButton, label: pageLabel, page, total: lastTotal, pageSize });
    const table = tableBody.closest('table');
    decorateReusableTable(table, 0);
  }

  // Update generated YYMMDD preview whenever the production date picker changes.
  dateInput.addEventListener('input', () => {
    dateCodeInput.value = formatDateToYyMmDd(dateInput.value);
  });

  if (formulationSelect) {
    formulationSelect.addEventListener('change', () => {
      // Mirror dropdown selection into the manual field so one visible source of truth is submitted.
      if (formulationManual) {
        formulationManual.value = formulationSelect.value || '';
      }
    });
  }

  if (formulationManual) {
    formulationManual.addEventListener('change', () => {
      if (!(formulationManual.value || '').trim()) return;
      try {
        // Validate manual value eagerly to surface formatting errors before form submit.
        parseFormulationCode(formulationManual.value);
      } catch (error) {
        alert(error.message);
      }
    });
  }

  // Submit location ID creation payload and refresh the listing from the first page.
  locationForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(locationForm);
    let payload;
    try {
      // Validate all AB-style code fields and normalize to uppercase for consistent location IDs.
      const preferredFormulationCode = (formData.get('formulation_manual') || '').toString().trim()
        || (formData.get('formulation_select') || '').toString().trim()
        || (formulationManual?.value || '').toString().trim()
        || (formulationSelect?.value || '').toString().trim();
      const parsedFormulation = parseFormulationCode(preferredFormulationCode);
      if (!parsedFormulation) {
        throw new Error('Select a formulation or enter a formulation code manually.');
      }
      payload = {
        ...parsedFormulation,
        partner_code: normalizeTwoLetterCode(formData.get('partner_code')),
        production_date: formatDateToYyMmDd((formData.get('production_date') || '').toString()),
      };
    } catch (error) {
      alert(error.message);
      return;
    }
    if (!payload.production_date) {
      alert('Please select a production date.');
      return;
    }
    try {
      const created = await postJson('/api/location_codes', payload);
      clearElement(output);
      output.appendChild(createCopyTextBlock(created.location_id || '', 'location-copy-output'));
      await loadLocationCodes(1);
    } catch (error) {
      alert(error.message);
    }
  });

  // Trigger filter-based search while resetting pagination to the first page.
  if (filterForm) {
    filterForm.addEventListener('submit', (event) => {
      event.preventDefault();
      loadLocationCodes(1).catch((error) => alert(error.message));
    });
  }

  if (prevButton) {
    prevButton.addEventListener('click', () => {
      if (page <= 1) return;
      loadLocationCodes(page - 1).catch((error) => alert(error.message));
    });
  }

  if (nextButton) {
    nextButton.addEventListener('click', () => {
      const totalPages = Math.max(1, Math.ceil(lastTotal / pageSize));
      if (page >= totalPages) return;
      loadLocationCodes(page + 1).catch((error) => alert(error.message));
    });
  }

  Promise.all([loadFormulations(), loadPartners()]).catch((error) => alert(error.message));
  loadLocationCodes(1).catch((error) => alert(error.message));
}

function attachLocationPartnerUtilityForm() {
  // Wire the utilities page partner creation workflow and keep the partner code table in sync.
  const partnerForm = document.getElementById('location-partner-form');
  if (!partnerForm) return;
  const partnerResults = document.getElementById('location-partner-results');

  async function loadPartnerTable() {
    const data = await fetchJson('/api/location_codes/partners');
    const partners = data.items || [];
    const rows = partners.map((partner) => [
      partner.partner_name || '',
      partner.partner_code || '',
      partner.machine_specification || '',
      partner.created_by || '',
    ]);
    buildTable(partnerResults, ['Partner', 'Identification Code', 'Machine specification', 'Owner'], rows, 'No partners found.');
  }

  partnerForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(partnerForm);
    const payload = {
      // Always create a new AB code from the counter, even if partner/machine text matches existing rows.
      partner_name: (formData.get('partner_name') || '').toString().trim(),
      machine_specification: (formData.get('machine_specification') || '').toString().trim(),
    };
    try {
      const created = await postJson('/api/location_codes/partners', payload);
      alert(`Partner code ${created.partner_code} created.`);
      partnerForm.reset();
      await loadPartnerTable();
    } catch (error) {
      alert(error.message);
    }
  });

  loadPartnerTable().catch((error) => alert(error.message));
}

function attachCompoundingHowPage() {
  // Wire form generation, table rendering, and inline edit interactions for compounding-how records.
  const form = document.getElementById('compounding-how-form');
  if (!form) return;
  const locationSelect = document.getElementById('compounding-location-code');
  const suffixInput = document.getElementById('compounding-process-suffix');
  const previewInput = document.getElementById('compounding-processing-preview');
  const allocateButton = document.getElementById('compounding-allocate');
  const failureModeSelect = document.getElementById('compounding-failure-mode');
  const output = document.getElementById('compounding-how-results');

  // Keep processing code preview synchronized with selected location code and generated suffix.
  function updatePreview() {
    const locationCode = (locationSelect.value || '').trim();
    const suffix = (suffixInput.value || '').trim();
    previewInput.value = locationCode && suffix ? `${locationCode} ${suffix}` : '';
  }

  // Populate dropdown metadata and prime a generated process suffix for first submit.
  async function loadMeta() {
    const data = await fetchJson('/api/compounding_how/meta');
    clearElement(locationSelect);
    const firstLocation = document.createElement('option');
    firstLocation.value = '';
    firstLocation.textContent = 'Select location code';
    locationSelect.appendChild(firstLocation);
    (data.location_codes || []).forEach((locationCode) => {
      const option = document.createElement('option');
      option.value = locationCode;
      option.textContent = locationCode;
      locationSelect.appendChild(option);
    });

    clearElement(failureModeSelect);
    const firstMode = document.createElement('option');
    firstMode.value = '';
    firstMode.textContent = 'Select failure mode';
    failureModeSelect.appendChild(firstMode);
    (data.failure_modes || []).forEach((mode) => {
      const option = document.createElement('option');
      option.value = mode;
      option.textContent = mode;
      failureModeSelect.appendChild(option);
    });

    const allocated = await postJson('/api/compounding_how/allocate', {});
    suffixInput.value = allocated.process_code_suffix || '';
    updatePreview();
  }

  // Render compounding-how records and expose inline edit controls for permitted mutable fields.
  async function loadItems() {
    const data = await fetchJson('/api/compounding_how');
    const items = data.items || [];
    clearElement(output);

    if (!items.length) {
      const p = document.createElement('p');
      p.textContent = 'No compounding how entries found.';
      output.appendChild(p);
      return;
    }

    const table = document.createElement('table');
    table.innerHTML = '<thead><tr><th>Processing Code</th><th>Date created</th><th>Owner</th><th>Failure Mode</th><th>Machine Setup File</th><th>Processed Data File</th><th>Actions</th></tr></thead>';
    const tbody = document.createElement('tbody');

    items.forEach((item) => {
      const row = document.createElement('tr');
      const processingCodeCell = document.createElement('td');
      processingCodeCell.appendChild(createCopyTextBlock(item.processing_code || '', 'processing-copy'));

      const createdCell = document.createElement('td');
      createdCell.textContent = formatTimestampForTable(item.created_at);

      const ownerCell = document.createElement('td');
      ownerCell.textContent = item.created_by || 'Unknown';

      const failureCell = document.createElement('td');
      const failureInput = document.createElement('select');
      ([''].concat(Array.from(new Set(Array.from(failureModeSelect.options).map((option) => option.value).filter(Boolean))))).forEach((mode) => {
        const option = document.createElement('option');
        option.value = mode;
        option.textContent = mode || 'Select failure mode';
        failureInput.appendChild(option);
      });
      failureInput.value = item.failure_mode || '';
      failureInput.disabled = true;
      failureCell.appendChild(failureInput);

      const machineCell = document.createElement('td');
      const machineInput = document.createElement('input');
      machineInput.type = 'url';
      machineInput.value = item.machine_setup_url || '';
      machineInput.disabled = true;
      machineCell.appendChild(machineInput);
      // Provide an explicit clickable anchor so operators can open links directly from the table.
      if ((item.machine_setup_url || '').trim()) {
        const openMachineLink = document.createElement('a');
        openMachineLink.href = item.machine_setup_url;
        openMachineLink.target = '_blank';
        openMachineLink.rel = 'noopener noreferrer';
        openMachineLink.textContent = 'Open';
        machineCell.appendChild(openMachineLink);
      }

      const processedCell = document.createElement('td');
      const processedInput = document.createElement('input');
      processedInput.type = 'url';
      processedInput.value = item.processed_data_url || '';
      processedInput.disabled = true;
      processedCell.appendChild(processedInput);
      // Provide an explicit clickable anchor so operators can open links directly from the table.
      if ((item.processed_data_url || '').trim()) {
        const openProcessedLink = document.createElement('a');
        openProcessedLink.href = item.processed_data_url;
        openProcessedLink.target = '_blank';
        openProcessedLink.rel = 'noopener noreferrer';
        openProcessedLink.textContent = 'Open';
        processedCell.appendChild(openProcessedLink);
      }

      const actionCell = document.createElement('td');
      const editButton = document.createElement('button');
      editButton.type = 'button';
      editButton.textContent = 'Edit';
      actionCell.appendChild(editButton);

      // Toggle into edit mode and persist allowed fields with API update call.
      editButton.addEventListener('click', async () => {
        if (editButton.dataset.mode !== 'editing') {
          editButton.dataset.mode = 'editing';
          editButton.textContent = 'Save';
          failureInput.disabled = false;
          machineInput.disabled = false;
          processedInput.disabled = false;
          return;
        }

        try {
          await fetchJson(`/api/compounding_how/${encodeURIComponent(item.processing_code || '')}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              failure_mode: failureInput.value,
              machine_setup_url: machineInput.value,
              processed_data_url: processedInput.value,
            }),
          });
          editButton.dataset.mode = '';
          editButton.textContent = 'Edit';
          failureInput.disabled = true;
          machineInput.disabled = true;
          processedInput.disabled = true;
        } catch (error) {
          alert(error.message);
        }
      });

      row.append(processingCodeCell, createdCell, ownerCell, failureCell, machineCell, processedCell, actionCell);
      tbody.appendChild(row);
    });

    table.appendChild(tbody);
    output.appendChild(table);
    decorateReusableTable(table, 0);
  }

  locationSelect.addEventListener('change', updatePreview);

  // Allocate a fresh AB-style suffix and update preview so users can regenerate before save.
  allocateButton.addEventListener('click', async () => {
    try {
      const allocated = await postJson('/api/compounding_how/allocate', {});
      suffixInput.value = allocated.process_code_suffix || '';
      updatePreview();
    } catch (error) {
      alert(error.message);
    }
  });

  // Submit compounding-how record and then refresh table while allocating next suffix for convenience.
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    try {
      await postJson('/api/compounding_how', {
        location_code: (formData.get('location_code') || '').toString().trim(),
        process_code_suffix: (formData.get('process_code_suffix') || '').toString().trim(),
        failure_mode: (formData.get('failure_mode') || '').toString().trim(),
        machine_setup_url: (formData.get('machine_setup_url') || '').toString().trim(),
        processed_data_url: (formData.get('processed_data_url') || '').toString().trim(),
      });
      form.reset();
      // After save, fetch the next submitted-based suffix and restore preview helpers.
      const allocated = await postJson('/api/compounding_how/allocate', {});
      suffixInput.value = allocated.process_code_suffix || '';
      updatePreview();
      await loadItems();
    } catch (error) {
      alert(error.message);
    }
  });

  loadMeta().then(loadItems).catch((error) => alert(error.message));
}

// Render formulations on the batch detail page by loading them from the batch detail API endpoint.
function attachBatchDetailFormulations() {
  const container = document.getElementById('batch-detail-formulations');
  if (!container) return;
  const sku = container.dataset.sku;
  const batchCode = container.dataset.batchCode;
  fetch(`/api/ingredient_batches/${encodeURIComponent(sku)}/${encodeURIComponent(batchCode)}`)
    .then((response) => response.json())
    .then((data) => {
      if (!data.ok) {
        throw new Error(data.error || 'Error loading formulations for batch');
      }
      renderFormulationsTable(container, data.data.formulations || []);
    })
    .catch((error) => {
      buildTable(container, ['Error'], [[error.message]], '');
    });
}

function appendSelectOptions(select, options) {
  // Fill select options with a blank first option to keep all optional fields nullable.
  clearElement(select);
  const blank = document.createElement('option');
  blank.value = '';
  blank.textContent = 'Select';
  select.appendChild(blank);
  options.forEach((value) => {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = value;
    select.appendChild(option);
  });
}

function attachPelletBagsPage() {
  const form = document.getElementById('pellet-bag-create-form');
  if (!form) return;

  const status = document.getElementById('pellet-create-status');
  const createdCodesContainer = document.getElementById('pellet-created-codes');
  const output = document.getElementById('pellet-bags-results');

  const productType = document.getElementById('pellet-product-type');
  const purpose = document.getElementById('pellet-purpose');
  const referenceSample = document.getElementById('pellet-reference-sample');
  const qcStatus = document.getElementById('pellet-qc-status');
  const longStatus = document.getElementById('pellet-long-status');
  const densityStatus = document.getElementById('pellet-density-status');
  const injectionStatus = document.getElementById('pellet-injection-status');
  const filmStatus = document.getElementById('pellet-film-status');
  const injectionAssignee = document.getElementById('pellet-injection-assignee');
  const filmAssignee = document.getElementById('pellet-film-assignee');

  // Cache dropdown option arrays so inline edit cells can mirror create-form selects exactly.
  const pelletMetaOptions = {
    purpose: [],
    reference_sample_taken: [],
    qc_status: [],
    long_moisture_status: [],
    density_status: [],
    injection_moulding_status: [],
    film_forming_status: [],
    injection_moulding_assignee_email: [],
    film_forming_assignee_email: [],
  };

  function numericOrNull(value) {
    // Convert numeric inputs to Number and keep empty values as null for API compatibility.
    if (value === null || value === undefined || value === '') return null;
    return Number(value);
  }

  function renderCreatedCodes(items) {
    // Show newly minted codes with one-click copy buttons immediately after creation.
    clearElement(createdCodesContainer);
    if (!items || items.length === 0) return;
    const list = document.createElement('ul');
    items.forEach((item) => {
      const li = document.createElement('li');
      const text = document.createElement('a');
      text.href = `/pellet-bags/${encodeURIComponent(item.pellet_bag_code || '')}`;
      text.textContent = item.pellet_bag_code || '';
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Copy';
      button.addEventListener('click', async () => {
        await navigator.clipboard.writeText(item.pellet_bag_code || '');
      });
      li.append(text, document.createTextNode(' '), button);
      list.appendChild(li);
    });
    createdCodesContainer.appendChild(list);
  }

  async function loadMeta() {
    // Load dropdown metadata including dynamic assignee email options.
    const meta = await fetchJson('/api/pellet_bags/meta');
    appendSelectOptions(productType, meta.product_types || []);
    appendSelectOptions(purpose, meta.purpose_options || []);
    appendSelectOptions(referenceSample, meta.reference_sample_options || []);
    appendSelectOptions(qcStatus, meta.qc_status_options || []);
    appendSelectOptions(longStatus, meta.status_options || []);
    appendSelectOptions(densityStatus, meta.status_options || []);
    appendSelectOptions(injectionStatus, meta.injection_film_status_options || []);
    appendSelectOptions(filmStatus, meta.injection_film_status_options || []);
    appendSelectOptions(injectionAssignee, meta.assignee_emails || []);
    appendSelectOptions(filmAssignee, meta.assignee_emails || []);

    // Persist metadata locally so table edit mode can reuse the same constrained option lists as the create form.
    pelletMetaOptions.purpose = meta.purpose_options || [];
    pelletMetaOptions.reference_sample_taken = meta.reference_sample_options || [];
    pelletMetaOptions.qc_status = meta.qc_status_options || [];
    pelletMetaOptions.long_moisture_status = meta.status_options || [];
    pelletMetaOptions.density_status = meta.status_options || [];
    pelletMetaOptions.injection_moulding_status = meta.injection_film_status_options || [];
    pelletMetaOptions.film_forming_status = meta.injection_film_status_options || [];
    pelletMetaOptions.injection_moulding_assignee_email = meta.assignee_emails || [];
    pelletMetaOptions.film_forming_assignee_email = meta.assignee_emails || [];
  }

  function registerEditableControl(row, control) {
    // Register each editable control so the row-level Edit/Save action can toggle disabled state and collect payload values.
    row._editable = row._editable || [];
    row._editable.push(control);
  }

  function editableInputCell(row, value, name, type = 'text') {
    // Render editable text/number cells for free-text fields that are not dropdown constrained.
    const td = document.createElement('td');
    const input = document.createElement('input');
    input.type = type;
    input.value = value ?? '';
    input.name = name;
    input.disabled = true;
    td.appendChild(input);
    registerEditableControl(row, input);
    return td;
  }

  function editableSelectCell(row, value, name, options) {
    // Render editable select cells so edit mode uses the same dropdown workflow as creation optional fields.
    const td = document.createElement('td');
    const select = document.createElement('select');
    appendSelectOptions(select, options || []);
    select.name = name;
    select.value = value ?? '';
    select.disabled = true;
    td.appendChild(select);
    registerEditableControl(row, select);
    return td;
  }

  async function loadItems() {
    // Render the created bag table and enable in-row editing for optional fields only.
    const data = await fetchJson('/api/pellet_bags');
    const items = data.items || [];
    clearElement(output);
    const table = document.createElement('table');
    const headers = [
      'Pellet bag code','Product type','Bag mass kg','Remaining mass kg','Short moisture percent','Purpose','Reference sample taken','QC status','Long moisture status','Density status','Injection moulding status','Injection moulding assignee','Film forming status','Film forming assignee','Customer','Notes','Created at','Created by','Actions'
    ];
    const thead = document.createElement('thead');
    const headRow = document.createElement('tr');
    headers.forEach((header) => {
      const th = document.createElement('th');
      th.textContent = header;
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    items.forEach((item) => {
      const row = document.createElement('tr');
      const pelletCodeCell = document.createElement('td');
      // Keep full pellet bag codes on one line and wide enough so token groups remain legible.
      const pelletCodeLink = document.createElement('a');
      pelletCodeLink.href = `/pellet-bags/${encodeURIComponent(item.pellet_bag_code ?? '')}`;
      pelletCodeLink.textContent = item.pellet_bag_code ?? '';
      pelletCodeCell.appendChild(pelletCodeLink);
      pelletCodeCell.classList.add('pellet-code-cell');
      row.appendChild(pelletCodeCell);

      const readonlyValues = [item.product_type, item.bag_mass_kg];
      readonlyValues.forEach((value) => {
        const td = document.createElement('td');
        td.textContent = value ?? '';
        row.appendChild(td);
      });

      row.appendChild(editableInputCell(row, item.remaining_mass_kg, 'remaining_mass_kg', 'number'));
      row.appendChild(editableInputCell(row, item.short_moisture_percent, 'short_moisture_percent', 'number'));
      row.appendChild(editableSelectCell(row, item.purpose, 'purpose', pelletMetaOptions.purpose));
      row.appendChild(editableSelectCell(row, item.reference_sample_taken, 'reference_sample_taken', pelletMetaOptions.reference_sample_taken));
      row.appendChild(editableSelectCell(row, item.qc_status, 'qc_status', pelletMetaOptions.qc_status));
      row.appendChild(editableSelectCell(row, item.long_moisture_status, 'long_moisture_status', pelletMetaOptions.long_moisture_status));
      row.appendChild(editableSelectCell(row, item.density_status, 'density_status', pelletMetaOptions.density_status));
      row.appendChild(editableSelectCell(row, item.injection_moulding_status, 'injection_moulding_status', pelletMetaOptions.injection_moulding_status));
      row.appendChild(editableSelectCell(row, item.injection_moulding_assignee_email, 'injection_moulding_assignee_email', pelletMetaOptions.injection_moulding_assignee_email));
      row.appendChild(editableSelectCell(row, item.film_forming_status, 'film_forming_status', pelletMetaOptions.film_forming_status));
      row.appendChild(editableSelectCell(row, item.film_forming_assignee_email, 'film_forming_assignee_email', pelletMetaOptions.film_forming_assignee_email));
      row.appendChild(editableInputCell(row, item.customer, 'customer'));
      row.appendChild(editableInputCell(row, item.notes, 'notes'));

      const createdAt = document.createElement('td');
      createdAt.textContent = formatTimestampForTable(item.created_at);
      const createdBy = document.createElement('td');
      createdBy.textContent = item.created_by || '';
      row.append(createdAt, createdBy);

      const actionTd = document.createElement('td');
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Edit';
      button.addEventListener('click', async () => {
        if (button.dataset.mode !== 'editing') {
          button.dataset.mode = 'editing';
          button.textContent = 'Save';
          (row._editable || []).forEach((input) => { input.disabled = false; });
          return;
        }

        const payload = {};
        (row._editable || []).forEach((input) => {
          payload[input.name] = input.type === 'number' ? numericOrNull(input.value) : (input.value || null);
        });

        await fetchJson(`/api/pellet_bags/${encodeURIComponent(item.pellet_bag_id || '')}`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        button.dataset.mode = '';
        button.textContent = 'Edit';
        (row._editable || []).forEach((input) => { input.disabled = true; });
      });
      actionTd.appendChild(button);
      row.appendChild(actionTd);

      tbody.appendChild(row);
    });
    table.appendChild(tbody);
    output.appendChild(table);
    decorateReusableTable(table, 0);
  }

  form.addEventListener('submit', async (event) => {
    // Submit create payload, supporting bulk minting and optional creation fields.
    event.preventDefault();
    const formData = new FormData(form);
    const payload = {
      compounding_how_code: (formData.get('compounding_how_code') || '').toString().trim(),
      product_type: (formData.get('product_type') || '').toString().trim(),
      bag_mass_kg: Number(formData.get('bag_mass_kg')),
      number_of_bags: Number(formData.get('number_of_bags') || 1),
      short_moisture_percent: numericOrNull(formData.get('short_moisture_percent')),
      purpose: (formData.get('purpose') || '').toString().trim() || null,
      reference_sample_taken: (formData.get('reference_sample_taken') || '').toString().trim() || null,
      qc_status: (formData.get('qc_status') || '').toString().trim() || null,
      long_moisture_status: (formData.get('long_moisture_status') || '').toString().trim() || null,
      density_status: (formData.get('density_status') || '').toString().trim() || null,
      injection_moulding_status: (formData.get('injection_moulding_status') || '').toString().trim() || null,
      film_forming_status: (formData.get('film_forming_status') || '').toString().trim() || null,
      injection_moulding_assignee_email: (formData.get('injection_moulding_assignee_email') || '').toString().trim() || null,
      film_forming_assignee_email: (formData.get('film_forming_assignee_email') || '').toString().trim() || null,
      remaining_mass_kg: numericOrNull(formData.get('remaining_mass_kg')),
      notes: (formData.get('notes') || '').toString(),
      customer: (formData.get('customer') || '').toString(),
    };

    status.textContent = 'Creating...';
    const created = await postJson('/api/pellet_bags', payload);
    status.textContent = `Created ${created.items?.length || 0} bag(s).`;
    renderCreatedCodes(created.items || []);
    await loadItems();
  });

  loadMeta().then(loadItems).catch((error) => alert(error.message));
}





function attachConversion1ProductsPage() {
  // Bind only on Conversion 1 products route where the create form is present.
  const form = document.getElementById('conversion1-products-create-form');
  if (!form) return;

  const errorsBox = document.getElementById('conversion1-products-errors');
  const status = document.getElementById('conversion1-products-create-status');
  const createdCodesContainer = document.getElementById('conversion1-products-created-codes');
  const output = document.getElementById('conversion1-products-results');
  const howSelect = document.getElementById('conversion1-products-how-select');
  const howManual = document.getElementById('conversion1-products-how-manual');
  const numberInput = document.getElementById('conversion1-products-number-of-records');
  const filterInput = document.getElementById('conversion1-products-filter');
  const filterButton = document.getElementById('conversion1-products-filter-button');

  // Reuse API-provided option lists so create and edit controls stay in sync with backend validation.
  const metaOptions = {
    storage_location_options: [],
    other_status_options: [],
    tensile_status_options: [],
  };

  // Track pagination state locally to support filter reloads and table page-size controls.
  const state = { page: 1, pageSize: DEFAULT_PAGE_SIZE, total: 0, search: '' };

  function normalizeCodeValue() {
    // Prefer manual pasted value when present, otherwise use dropdown selection.
    const manualValue = (howManual.value || '').trim().replace(/\s+/g, ' ');
    const selectValue = (howSelect.value || '').trim();
    return manualValue || selectValue;
  }

  function showErrors(errors) {
    // Render validation errors above the create panel to match existing form UX pattern.
    if (!errors || errors.length === 0) {
      errorsBox.style.display = 'none';
      errorsBox.innerHTML = '';
      return;
    }
    errorsBox.style.display = 'block';
    errorsBox.innerHTML = errors.map((item) => `<p>${item}</p>`).join('');
  }

  function clearCreatedCodes() {
    // Clear created-codes helper area before each fresh create attempt.
    clearElement(createdCodesContainer);
  }

  function renderCreatedCodes(items) {
    // Show just-created product codes for fast copy access after generation.
    clearCreatedCodes();
    if (!items || items.length === 0) return;
    const list = document.createElement('ul');
    items.forEach((item) => {
      const li = document.createElement('li');
      li.textContent = item.product_code || '';
      list.appendChild(li);
    });
    createdCodesContainer.appendChild(list);
  }

  function registerEditableControl(row, control) {
    // Register each cell control for row-level edit/save toggling and payload extraction.
    row._editable = row._editable || [];
    row._editable.push(control);
  }

  function editableInputCell(row, value, name, type = 'text') {
    // Build a disabled input cell that unlocks only in row edit mode.
    const td = document.createElement('td');
    const input = document.createElement('input');
    input.type = type;
    input.value = value ?? '';
    input.name = name;
    input.disabled = true;
    td.appendChild(input);
    registerEditableControl(row, input);
    return td;
  }

  function editableSelectCell(row, value, name, options) {
    // Build dropdown edit cells using API metadata option lists.
    const td = document.createElement('td');
    const select = document.createElement('select');
    const blank = document.createElement('option');
    blank.value = '';
    blank.textContent = 'Select';
    select.appendChild(blank);
    options.forEach((optionValue) => {
      const option = document.createElement('option');
      option.value = optionValue;
      option.textContent = optionValue;
      select.appendChild(option);
    });
    select.value = value ?? '';
    select.name = name;
    select.disabled = true;
    td.appendChild(select);
    registerEditableControl(row, select);
    return td;
  }

  function editableBooleanCell(row, value, name) {
    // Represent yes/no boolean with select options while keeping backend bool payload semantics.
    const td = document.createElement('td');
    const select = document.createElement('select');
    [
      { value: '', label: 'Select' },
      { value: 'true', label: 'Yes' },
      { value: 'false', label: 'No' },
    ].forEach((entry) => {
      const option = document.createElement('option');
      option.value = entry.value;
      option.textContent = entry.label;
      select.appendChild(option);
    });
    if (value === true) select.value = 'true';
    else if (value === false) select.value = 'false';
    else select.value = '';
    select.name = name;
    select.disabled = true;
    td.appendChild(select);
    registerEditableControl(row, select);
    return td;
  }

  function parseCellValue(input) {
    // Convert editable control values into nullable typed payload values for PATCH requests.
    if (input.tagName === 'SELECT' && input.name === 'numbered_in_order') {
      if (input.value === '') return null;
      return input.value === 'true';
    }
    if (input.type === 'number') {
      if (input.value === '') return null;
      return Number(input.value);
    }
    if (input.tagName === 'SELECT') {
      return input.value || null;
    }
    return (input.value || '').trim() || null;
  }

  async function loadMeta() {
    // Load all dropdown metadata and active how-code options needed by create/edit controls.
    const [meta, howCodes] = await Promise.all([
      fetchJson('/api/conversion1_products/meta'),
      fetchJson('/api/conversion1_products/how_codes'),
    ]);
    metaOptions.storage_location_options = meta.storage_location_options || [];
    metaOptions.other_status_options = meta.other_status_options || [];
    metaOptions.tensile_status_options = meta.tensile_status_options || [];

    clearElement(howSelect);
    const defaultOption = document.createElement('option');
    defaultOption.value = '';
    defaultOption.textContent = 'Select Conversion 1 How code';
    howSelect.appendChild(defaultOption);
    (howCodes.items || []).forEach((code) => {
      const option = document.createElement('option');
      option.value = code;
      option.textContent = code;
      howSelect.appendChild(option);
    });
  }

  async function loadItems() {
    // Fetch product rows for current filter/pagination settings and render inline-edit table.
    const query = new URLSearchParams({
      page: String(state.page),
      page_size: String(state.pageSize),
    });
    if (state.search) query.set('search', state.search);
    const data = await fetchJson(`/api/conversion1_products?${query.toString()}`);
    state.total = Number(data.total || 0);

    clearElement(output);
    const table = document.createElement('table');
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    [
      'Product code', 'Created at', 'Created by', 'Storage location', 'Notes', 'Number units produced', 'Numbered in order',
      'Tensile rigid status', 'Tensile films status', 'Seal strength status', 'Shelf stability status', 'Solubility status',
      'Defect analysis status', 'Blocking status', 'Film EMC status', 'Friction status', 'Width mm', 'Length m',
      'Avg film thickness um', 'SD film thickness', 'Film thickness variation %', 'Action',
    ].forEach((label) => {
      const th = document.createElement('th');
      th.textContent = label;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    (data.items || []).forEach((item) => {
      const row = document.createElement('tr');
      const productCodeCell = document.createElement('td');
      productCodeCell.textContent = item.product_code || '';
      row.appendChild(productCodeCell);
      const createdAtCell = document.createElement('td');
      createdAtCell.textContent = formatTimestampForTable(item.created_at);
      row.appendChild(createdAtCell);
      const createdByCell = document.createElement('td');
      createdByCell.textContent = item.created_by || '';
      row.appendChild(createdByCell);

      row.appendChild(editableSelectCell(row, item.storage_location, 'storage_location', metaOptions.storage_location_options));
      row.appendChild(editableInputCell(row, item.notes, 'notes'));
      row.appendChild(editableInputCell(row, item.number_units_produced, 'number_units_produced', 'number'));
      row.appendChild(editableBooleanCell(row, item.numbered_in_order, 'numbered_in_order'));
      row.appendChild(editableSelectCell(row, item.tensile_rigid_status, 'tensile_rigid_status', metaOptions.tensile_status_options));
      row.appendChild(editableSelectCell(row, item.tensile_films_status, 'tensile_films_status', metaOptions.tensile_status_options));
      row.appendChild(editableSelectCell(row, item.seal_strength_status, 'seal_strength_status', metaOptions.other_status_options));
      row.appendChild(editableSelectCell(row, item.shelf_stability_status, 'shelf_stability_status', metaOptions.other_status_options));
      row.appendChild(editableSelectCell(row, item.solubility_status, 'solubility_status', metaOptions.other_status_options));
      row.appendChild(editableSelectCell(row, item.defect_analysis_status, 'defect_analysis_status', metaOptions.other_status_options));
      row.appendChild(editableSelectCell(row, item.blocking_status, 'blocking_status', metaOptions.other_status_options));
      row.appendChild(editableSelectCell(row, item.film_emc_status, 'film_emc_status', metaOptions.other_status_options));
      row.appendChild(editableSelectCell(row, item.friction_status, 'friction_status', metaOptions.other_status_options));
      row.appendChild(editableInputCell(row, item.width_mm, 'width_mm', 'number'));
      row.appendChild(editableInputCell(row, item.length_m, 'length_m', 'number'));
      row.appendChild(editableInputCell(row, item.avg_film_thickness_um, 'avg_film_thickness_um', 'number'));
      row.appendChild(editableInputCell(row, item.sd_film_thickness, 'sd_film_thickness', 'number'));
      row.appendChild(editableInputCell(row, item.film_thickness_variation_percent, 'film_thickness_variation_percent', 'number'));

      const actionCell = document.createElement('td');
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = 'Edit';
      button.addEventListener('click', async () => {
        if (button.dataset.mode !== 'editing') {
          button.dataset.mode = 'editing';
          button.textContent = 'Save';
          (row._editable || []).forEach((input) => { input.disabled = false; });
          return;
        }
        const payload = {};
        (row._editable || []).forEach((input) => {
          payload[input.name] = parseCellValue(input);
        });
        await fetchJson(`/api/conversion1_products/${encodeURIComponent(item.product_code || '')}`, {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        button.dataset.mode = '';
        button.textContent = 'Edit';
        (row._editable || []).forEach((input) => { input.disabled = true; });
      });
      actionCell.appendChild(button);
      row.appendChild(actionCell);
      tbody.appendChild(row);
    });

    if ((data.items || []).length === 0) {
      const row = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 22;
      td.textContent = 'No conversion 1 product entries loaded.';
      row.appendChild(td);
      tbody.appendChild(row);
    }

    table.appendChild(tbody);
    output.appendChild(table);
    decorateReusableTable(table, 0);

    const controls = document.createElement('div');
    renderPageSizeControl({
      container: controls,
      currentSize: state.pageSize,
      onChange: async (newSize) => {
        state.pageSize = newSize;
        state.page = 1;
        await loadItems();
      },
    });
    const prev = document.createElement('button');
    prev.type = 'button';
    prev.textContent = 'Previous';
    const next = document.createElement('button');
    next.type = 'button';
    next.textContent = 'Next';
    const label = document.createElement('span');
    prev.addEventListener('click', async () => { state.page = Math.max(1, state.page - 1); await loadItems(); });
    next.addEventListener('click', async () => { state.page += 1; await loadItems(); });
    controls.append(prev, label, next);
    output.appendChild(controls);
    updatePagerControls({ prevButton: prev, nextButton: next, label, page: state.page, total: state.total, pageSize: state.pageSize });
  }

  form.addEventListener('submit', async (event) => {
    // Validate create form inputs client-side before requesting server-side creation.
    event.preventDefault();
    showErrors([]);
    clearCreatedCodes();

    const howCode = normalizeCodeValue();
    const numberOfRecords = Number(numberInput.value || 1);
    const errors = [];
    if (!howCode) errors.push('Conversion 1 How code is required.');
    if (!Number.isInteger(numberOfRecords) || numberOfRecords < 1 || numberOfRecords > 200) {
      errors.push('Number of records must be an integer between 1 and 200.');
    }
    if (errors.length > 0) {
      showErrors(errors);
      return;
    }

    status.textContent = 'Creating...';
    try {
      const created = await postJson('/api/conversion1_products', {
        conversion1_how_code: howCode,
        number_of_records: numberOfRecords,
      });
      status.textContent = `Created ${created.items?.length || 0} product(s).`;
      renderCreatedCodes(created.items || []);
      state.page = 1;
      await loadItems();
    } catch (error) {
      status.textContent = '';
      showErrors([error.message]);
    }
  });

  filterButton.addEventListener('click', async () => {
    // Apply product-code substring filter and reload the first page of results.
    state.search = (filterInput.value || '').trim();
    state.page = 1;
    await loadItems();
  });

  // Bootstrap dropdown metadata and initial table data once on page load.
  loadMeta().then(loadItems).catch((error) => showErrors([error.message]));
}

// Render formulations on the pellet bag detail page from server-provided JSON payload.
function attachPelletBagDetailFormulations() {
  // Locate the pellet detail formulation container and exit on non-detail routes.
  const container = document.getElementById('pellet-detail-formulations');
  if (!container) return;

  // Read embedded JSON payload prepared by the server route to avoid an extra API round-trip.
  const dataNode = document.getElementById('pellet-detail-formulations-data');
  if (!dataNode) return;

  try {
    // Parse formulations and reuse the shared formulation table renderer for matching styles.
    const formulations = JSON.parse(dataNode.textContent || '[]');
    renderFormulationsTable(container, Array.isArray(formulations) ? formulations : []);
    // Apply reusable sticky first-column behaviour used by the formulation page table.
    const table = container.querySelector('table');
    decorateReusableTable(table, 0);
  } catch (error) {
    // Fall back to an inline error message if payload parsing fails for any reason.
    container.textContent = `Unable to render formulations: ${error.message}`;
  }
}

// Keep sidebar group expansion state stable across route changes and browser refreshes.
function attachSidebarNavigation() {
  // Query all accordion groups rendered by the shared sidebar template.
  const groups = Array.from(document.querySelectorAll('[data-nav-group]'));
  // Exit early on pages that do not render the app sidebar.
  if (groups.length === 0) return;

  // Persisted storage key is versioned so future sidebar changes can migrate safely.
  const storageKey = 'formulation-tracker.sidebar-groups.v1';
  // Capture current route path for active-link matching.
  const currentPath = window.location.pathname;

  // Parse persisted open/closed states with a defensive fallback for malformed JSON.
  let storedState = {};
  try {
    storedState = JSON.parse(window.localStorage.getItem(storageKey) || '{}') || {};
  } catch {
    storedState = {};
  }

  // Track latest state in memory and write back to localStorage after each toggle.
  const nextState = { ...storedState };

  // Resolve active states per group and hydrate open/closed behaviour.
  groups.forEach((group) => {
    // Extract configured stable group id used for persistence.
    const groupId = group.getAttribute('data-group-id') || '';
    // Collect only clickable nav items so placeholder rows are ignored.
    const navLinks = Array.from(group.querySelectorAll('[data-nav-item]'));

    // Detect whether this group contains the current route item.
    const activeLink = navLinks.find((link) => {
      const href = link.getAttribute('href') || '';
      return href === currentPath;
    });

    // Mark each link's visual active state so only one current route stands out.
    navLinks.forEach((link) => {
      const href = link.getAttribute('href') || '';
      link.classList.toggle('is-active', href === currentPath);
      if (href === currentPath) {
        link.setAttribute('aria-current', 'page');
      } else {
        link.removeAttribute('aria-current');
      }
    });

    // Highlight the parent row subtly whenever a child route is active.
    group.classList.toggle('is-parent-active', Boolean(activeLink));

    // Parent groups auto-expand when a child route is active.
    if (activeLink) {
      group.open = true;
    } else if (groupId in nextState) {
      // Use persisted explicit state for non-active groups.
      group.open = Boolean(nextState[groupId]);
    }

    // Ensure stored state mirrors hydrated state so first render also gets persisted.
    if (groupId) {
      nextState[groupId] = Boolean(group.open);
    }

    // Persist user toggles while preserving group openness across navigation.
    group.addEventListener('toggle', () => {
      if (!groupId) return;
      nextState[groupId] = Boolean(group.open);
      window.localStorage.setItem(storageKey, JSON.stringify(nextState));
    });
  });

  // Persist hydrated defaults once so subsequent route changes are stable.
  window.localStorage.setItem(storageKey, JSON.stringify(nextState));
}

// Apply reusable sticky/scroll behavior to server-rendered tables that exist on initial page load.
Array.from(document.querySelectorAll('table')).forEach((table) => {
  decorateReusableTable(table, 0);
});

attachIngredientForm();
attachIngredientImportForm();
attachIngredientMsdsForm();
attachBatchForm();
attachBatchLookupForm();
attachSetForm();
attachDryWeightForm();
attachDryWeightLookupForm();
attachBatchVariantForm();
attachBatchVariantLookupForm();
attachFormulationsFilterForm();
attachLocationCodePage();
attachLocationPartnerUtilityForm();
attachCompoundingHowPage();
attachPelletBagsPage();
attachConversion1ProductsPage();
attachBatchDetailFormulations();
attachPelletBagDetailFormulations();
attachSidebarNavigation();
