async function postJson(url, payload) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok || !data.ok) {
    throw new Error(data.error || data.detail || response.statusText);
  }
  return data.data;
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

function attachBatchLookupForm() {
  const form = document.getElementById('batch-lookup-form');
  if (!form) return;
  const output = document.getElementById('batch-results');
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const sku = new FormData(form).get('sku');
    const response = await fetch(`/api/ingredient_batches?sku=${encodeURIComponent(sku)}`);
    const data = await response.json();
    if (!data.ok) {
      alert(data.error || 'Error loading batches');
      return;
    }
    const items = data.data.items || [];
    clearElement(output);
    if (items.length === 0) {
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = 7;
      cell.textContent = 'No batches found.';
      row.appendChild(cell);
      output.appendChild(row);
      return;
    }
    items.forEach((item) => {
      const row = document.createElement('tr');
      const skuCell = document.createElement('td');
      skuCell.textContent = item.sku;
      const codeCell = document.createElement('td');
      codeCell.textContent = item.ingredient_batch_code;
      const receivedCell = document.createElement('td');
      receivedCell.textContent = item.received_at ? new Date(item.received_at).toLocaleString() : '';
      const notesCell = document.createElement('td');
      notesCell.textContent = item.notes || '';
      const quantityCell = document.createElement('td');
      quantityCell.textContent = item.quantity_value !== null && item.quantity_value !== undefined && item.quantity_value !== '' ? `${item.quantity_value} ${item.quantity_unit || ''}`.trim() : '';
      const ownerCell = document.createElement('td');
      ownerCell.textContent = item.created_by || '';
      const coaCell = document.createElement('td');
      if (item.spec_object_path) {
        const link = document.createElement('a');
        link.href = '#';
        link.textContent = 'CoA';
        link.addEventListener('click', async (e) => {
          e.preventDefault();
          const response = await fetch(`/api/ingredient_batches/${encodeURIComponent(item.sku)}/${encodeURIComponent(item.ingredient_batch_code)}/spec/download_url`);
          const data = await response.json();
          if (!response.ok || !data.ok) {
            alert(data.error || data.detail || 'Unable to load CoA');
            return;
          }
          window.open(data.data.download_url, '_blank', 'noopener');
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
  });
}

function attachSetForm() {
  const form = document.getElementById('set-form');
  if (!form) return;
  const addButton = document.getElementById('add-sku-select');
  const selectsContainer = document.getElementById('sku-selects');
  const template = document.getElementById('sku-select-template');
  if (addButton && selectsContainer && template) {
    addButton.addEventListener('click', () => {
      const fragment = template.content.cloneNode(true);
      selectsContainer.appendChild(fragment);
    });
  }
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
      window.location.reload();
    } catch (error) {
      alert(error.message);
    }
  });
}

function attachDryWeightForm() {
  const form = document.getElementById('dry-weight-form');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const setCode = formData.get('set_code');
    const itemsRaw = parseList(formData.get('items'));
    const items = itemsRaw.map((entry) => {
      const [sku, percent] = entry.split(':').map((item) => item.trim());
      return { sku, wt_percent: Number(percent) };
    });
    try {
      await postJson('/api/dry_weights', { set_code: setCode, items });
      form.reset();
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
    const setCode = new FormData(form).get('set_code');
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
    const setCode = formData.get('set_code');
    const weightCode = formData.get('weight_code');
    if (!setCode || !weightCode) {
      alert('Enter a set code and weight code to load SKUs.');
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
    const setCode = formData.get('set_code');
    const weightCode = formData.get('weight_code');
    const selects = Array.from(itemsContainer.querySelectorAll('select[data-sku]'));
    if (selects.length === 0) {
      alert('Load SKUs before creating a variant.');
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
    const setCode = formData.get('set_code');
    const weightCode = formData.get('weight_code');
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

function attachFormulationsFilterForm() {
  const form = document.getElementById('formulations-filter-form');
  if (!form) return;
  const output = document.getElementById('formulations-results');
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const params = new URLSearchParams(new FormData(form));
    const response = await fetch(`/api/formulations?${params.toString()}`);
    const data = await response.json();
    if (!data.ok) {
      alert(data.error || 'Error loading formulations');
      return;
    }
    const items = data.data.items || [];
    if (items.length === 0) {
      buildTable(output, [], [], 'No formulations found.');
      return;
    }
    const headers = Object.keys(items[0]);
    const rows = items.map((item) => headers.map((header) => item[header] ?? ''));
    buildTable(output, headers, rows, 'No formulations found.');
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
