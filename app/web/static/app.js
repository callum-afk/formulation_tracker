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
  const pageSize = 10;
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
        receivedCell.textContent = item.received_at ? new Date(item.received_at).toLocaleString() : '';
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
  const pageSize = 10;
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
        createdCell.textContent = item.created_at ? new Date(item.created_at).toLocaleString() : '';
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
function mapItemsBySku(items, valueKey) {
  const map = new Map();
  if (!Array.isArray(items)) {
    return map;
  }
  items.forEach((item) => {
    const sku = item?.sku;
    if (!sku) return;
    map.set(sku, item[valueKey]);
  });
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

// Render formulation rows with HTML rowspans so sku/weight/batch lines align like the reference.
function renderFormulationsTable(output, items) {
  clearElement(output);
  const table = document.createElement('table');
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
    return;
  }

  items.forEach((item) => {
    const skuList = Array.isArray(item.sku_list) ? item.sku_list : parseList(item.sku_list || '');
    const weightMap = mapItemsBySku(item.dry_weight_items, 'wt_percent');
    const batchMap = mapItemsBySku(item.batch_items, 'ingredient_batch_code');
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
        createdCell.textContent = item.created_at ? new Date(item.created_at).toLocaleString() : '';

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
      skuCell.textContent = sku;
      const weightCell = document.createElement('td');
      weightCell.textContent = sku === '—' ? '—' : formatPercent(weightMap.get(sku));
      const batchCell = document.createElement('td');
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
  const pageSize = 10;
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
  const partnerSelect = document.getElementById('location-partner-select');
  const dateInput = document.getElementById('location-production-date');
  const dateCodeInput = document.getElementById('location-production-code');
  const output = document.getElementById('location-code-output');
  const tableBody = document.getElementById('location-codes-table-body');
  const prevButton = document.getElementById('location-codes-prev-page');
  const nextButton = document.getElementById('location-codes-next-page');
  const pageLabel = document.getElementById('location-codes-page-label');
  const filterForm = document.getElementById('location-codes-filter-form');
  const pageSize = 10;
  let page = 1;
  let lastTotal = 0;

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

  // Render paginated location code table rows with inline copy controls and optional server-side filtering.
  async function loadLocationCodes(targetPage) {
    const params = new URLSearchParams();
    params.set('page', String(targetPage));
    params.set('page_size', String(pageSize));
    if (filterForm) {
      const query = ((new FormData(filterForm).get('q') || '').toString().trim());
      if (query) params.set('q', query);
    }
    const data = await fetchJson(`/api/location_codes?${params.toString()}`);
    const items = data.items || [];
    page = Number(data.page || targetPage);
    lastTotal = Number(data.total || 0);
    clearElement(tableBody);

    if (!items.length) {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 1;
      cell.textContent = 'No location codes found.';
      row.appendChild(cell);
      tableBody.appendChild(row);
    } else {
      items.forEach((item) => {
        const row = document.createElement('tr');
        const locationCell = document.createElement('td');
        locationCell.appendChild(createCopyTextBlock(item.location_id || '', 'location-copy'));
        row.append(locationCell);
        tableBody.appendChild(row);
      });
    }

    updatePagerControls({ prevButton, nextButton, label: pageLabel, page, total: lastTotal, pageSize });
  }

  // Update generated YYMMDD preview whenever the production date picker changes.
  dateInput.addEventListener('input', () => {
    dateCodeInput.value = formatDateToYyMmDd(dateInput.value);
  });

  // Submit location ID creation payload and refresh the listing from the first page.
  locationForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(locationForm);
    let payload;
    try {
      // Validate all AB-style code fields and normalize to uppercase for consistent location IDs.
      payload = {
        set_code: normalizeTwoLetterCode(formData.get('set_code')),
        weight_code: normalizeTwoLetterCode(formData.get('weight_code')),
        batch_variant_code: normalizeTwoLetterCode(formData.get('batch_variant_code')),
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

  loadPartners().catch((error) => alert(error.message));
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
      createdCell.textContent = item.created_at ? new Date(item.created_at).toLocaleString() : '';

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

      const processedCell = document.createElement('td');
      const processedInput = document.createElement('input');
      processedInput.type = 'url';
      processedInput.value = item.processed_data_url || '';
      processedInput.disabled = true;
      processedCell.appendChild(processedInput);

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
      const allocated = await postJson('/api/compounding_how/allocate', {});
      suffixInput.value = allocated.process_code_suffix || '';
      updatePreview();
      form.reset();
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
attachBatchDetailFormulations();
