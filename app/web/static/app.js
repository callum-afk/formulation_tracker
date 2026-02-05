async function postJson(url, payload) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok || !data.ok) {
    throw new Error(data.error || response.statusText);
  }
  return data.data;
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
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const payload = Object.fromEntries(formData.entries());
    payload.category_code = Number(payload.category_code);
    payload.pack_size_value = Number(payload.pack_size_value);
    try {
      await postJson('/api/ingredients', payload);
      window.location.reload();
    } catch (error) {
      alert(error.message);
    }
  });
}

function attachIngredientImportForm() {
  const form = document.getElementById('ingredient-import-form');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const payload = Object.fromEntries(new FormData(form).entries());
    payload.category_code = Number(payload.category_code);
    payload.pack_size_value = Number(payload.pack_size_value);
    try {
      await postJson('/api/ingredients/import', payload);
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
    const payload = Object.fromEntries(new FormData(form).entries());
    try {
      await postJson('/api/ingredient_batches', payload);
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
    output.textContent = JSON.stringify(data.data.items, null, 2);
  });
}

function attachSetForm() {
  const form = document.getElementById('set-form');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const skus = parseList(new FormData(form).get('skus'));
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
    output.textContent = JSON.stringify(data.data.items, null, 2);
  });
}

function attachBatchVariantForm() {
  const form = document.getElementById('batch-variant-form');
  if (!form) return;
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const setCode = formData.get('set_code');
    const weightCode = formData.get('weight_code');
    const itemsRaw = parseList(formData.get('items'));
    const items = itemsRaw.map((entry) => {
      const [sku, batch] = entry.split(':').map((item) => item.trim());
      return { sku, ingredient_batch_code: batch };
    });
    try {
      await postJson('/api/batch_variants', { set_code: setCode, weight_code: weightCode, items });
      form.reset();
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
    output.textContent = JSON.stringify(data.data.items, null, 2);
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
    output.textContent = JSON.stringify(data.data.items, null, 2);
  });
}

attachIngredientForm();
attachIngredientImportForm();
attachBatchForm();
attachBatchLookupForm();
attachSetForm();
attachDryWeightForm();
attachDryWeightLookupForm();
attachBatchVariantForm();
attachBatchVariantLookupForm();
attachFormulationsFilterForm();
