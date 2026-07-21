(() => {
  "use strict";

  const body = document.body;
  const saveStatus = document.querySelector("[data-save-status]");
  const navigation = document.querySelector(".app-sidebar");
  const navigationButton = document.querySelector("[data-open-navigation]");
  const closeNavigationButtons = document.querySelectorAll("[data-close-navigation]");
  let lastNavigationFocus = null;
  let mutationQueue = Promise.resolve();
  const autosaveTimers = new WeakMap();

  const escapeHtml = (value) => String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

  const setSaveStatus = (message, state = "", actions = "") => {
    if (!saveStatus) return;
    saveStatus.className = `save-status ${state}`.trim();
    saveStatus.innerHTML = `${escapeHtml(message)}${actions}`;
  };

  const openNavigation = () => {
    lastNavigationFocus = document.activeElement;
    body.classList.add("navigation-open");
    navigationButton?.setAttribute("aria-expanded", "true");
    navigation?.querySelector("a")?.focus();
  };

  const closeNavigation = () => {
    body.classList.remove("navigation-open");
    navigationButton?.setAttribute("aria-expanded", "false");
    if (lastNavigationFocus instanceof HTMLElement) lastNavigationFocus.focus();
  };

  navigationButton?.addEventListener("click", openNavigation);
  closeNavigationButtons.forEach((button) => button.addEventListener("click", closeNavigation));
  navigation?.querySelectorAll("a").forEach((link) => link.addEventListener("click", () => {
    if (window.matchMedia("(max-width: 820px)").matches) closeNavigation();
  }));
  document.addEventListener("keydown", (event) => {
    if (!body.classList.contains("navigation-open")) return;
    if (event.key === "Escape") {
      closeNavigation();
      return;
    }
    if (event.key === "Tab" && navigation) {
      const focusable = [...navigation.querySelectorAll('a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])')]
        .filter((element) => element instanceof HTMLElement && element.offsetParent !== null);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }
  });

  const legacyDestinations = {
    streamers: "/streamers",
    config: "/settings",
    powerchat: "/powerchat",
    jobs: "/activity?view=jobs",
    logs: "/activity?view=logs",
    about: "/about",
  };
  if (body.dataset.page === "overview") {
    const legacyHash = window.location.hash.replace(/^#/, "").toLowerCase();
    if (legacyDestinations[legacyHash]) window.location.replace(legacyDestinations[legacyHash]);
  }

  const formValues = (form) => {
    const values = {};
    form.querySelectorAll("[name]").forEach((control) => {
      if (!(control instanceof HTMLInputElement || control instanceof HTMLSelectElement || control instanceof HTMLTextAreaElement)) return;
      if (control.disabled || control.type === "submit" || control.type === "button" || control.type === "file") return;
      if (control.dataset.formFallback === "true") return;
      if (["_revision", "action", "form_kind", "streamer_name", "return_to"].includes(control.name)) return;
      if (control instanceof HTMLInputElement && control.type === "checkbox") {
        values[control.name] = control.checked;
        return;
      }
      if (control instanceof HTMLInputElement && control.type === "radio") {
        if (control.checked) values[control.name] = control.value;
        return;
      }
      values[control.name] = control.value;
    });
    return values;
  };

  const clearFieldErrors = (form) => {
    form.querySelectorAll(".form-field.invalid").forEach((field) => field.classList.remove("invalid"));
    form.querySelectorAll("[data-field-error]").forEach((error) => { error.textContent = ""; });
  };

  const showFieldErrors = (form, errors = {}) => {
    clearFieldErrors(form);
    Object.entries(errors).forEach(([name, message]) => {
      const control = form.querySelector(`[name="${CSS.escape(name)}"]`);
      const field = control?.closest(".form-field, .setting-card");
      field?.classList.add("invalid");
      const output = field?.querySelector("[data-field-error]");
      if (output) output.textContent = String(message || "Invalid value");
    });
  };

  const conflictActions = (form) => {
    const formId = form.id || `autosave-${Math.random().toString(36).slice(2)}`;
    form.id = formId;
    return ` <button class="button small secondary" type="button" data-reload-page>Reload</button><button class="button small secondary" type="button" data-reapply-form="${escapeHtml(formId)}">Reapply</button>`;
  };

  const saveAutosaveForm = async (form, { force = false } = {}) => {
    if (!(form instanceof HTMLFormElement)) return;
    if (!force && form.dataset.dirty !== "true") return;
    clearTimeout(autosaveTimers.get(form));
    form.dataset.saving = "true";
    setSaveStatus("Saving…", "saving");
    clearFieldErrors(form);
    const payload = {
      values: formValues(form),
      revision: body.dataset.configRevision || "",
      form_kind: form.dataset.autosave,
    };
    if (form.dataset.streamerName) payload.streamer_name = form.dataset.streamerName;

    let response;
    try {
      const action = form.getAttribute("action") || window.location.pathname;
      response = await fetch(action, {
        method: "POST",
        headers: { "Accept": "application/json", "Content-Type": "application/json" },
        cache: "no-store",
        body: JSON.stringify(payload),
      });
    } catch (error) {
      form.dataset.saving = "false";
      form.dataset.dirty = "true";
      setSaveStatus("Save failed — check connection", "error", ` <button class="button small secondary" type="button" data-retry-form="${escapeHtml(form.id)}">Retry</button>`);
      return;
    }

    let result;
    try {
      result = await response.json();
    } catch (error) {
      form.dataset.saving = "false";
      form.dataset.dirty = "true";
      const message = response.ok ? "Save failed — invalid server response" : `Save failed — server returned ${response.status}`;
      setSaveStatus(message, "error", ` <button class="button small secondary" type="button" data-retry-form="${escapeHtml(form.id)}">Retry</button>`);
      return;
    }

    form.dataset.saving = "false";
    if (response.status === 409) {
      form.dataset.dirty = "true";
      form.dataset.conflictRevision = result.revision || "";
      setSaveStatus("Config changed elsewhere", "warning", conflictActions(form));
      return;
    }
    if (!response.ok || !result.ok) {
      form.dataset.dirty = "true";
      showFieldErrors(form, result.field_errors || {});
      setSaveStatus(result.message || "Could not save", "error", ` <button class="button small secondary" type="button" data-retry-form="${escapeHtml(form.id)}">Retry</button>`);
      return;
    }

    form.dataset.dirty = "false";
    if (result.revision) body.dataset.configRevision = result.revision;
    const restart = Array.isArray(result.restart_required) && result.restart_required.length;
    setSaveStatus(restart ? "Saved · restart required" : "Saved", restart ? "warning" : "saved");
    window.setTimeout(() => {
      if (saveStatus?.classList.contains("saved")) setSaveStatus("");
    }, 3500);
  };

  const enqueueAutosave = (form, options) => {
    mutationQueue = mutationQueue.then(() => saveAutosaveForm(form, options));
    return mutationQueue;
  };

  const markAutosaveDirty = (form, immediate = false) => {
    form.dataset.dirty = "true";
    setSaveStatus("Unsaved changes", "");
    clearTimeout(autosaveTimers.get(form));
    const timer = window.setTimeout(() => enqueueAutosave(form), immediate ? 0 : 600);
    autosaveTimers.set(form, timer);
  };

  const updatePostStreamStatuses = (form) => {
    const effective = {};
    form.querySelectorAll("[data-post-stream-select]").forEach((select) => {
      if (!(select instanceof HTMLSelectElement)) return;
      effective[select.name] = select.value === "inherit"
        ? select.dataset.appDefault === "true"
        : select.value === "enabled";
    });
    if (Object.hasOwn(effective, "voice_match_enabled")) {
      effective.voice_match_enabled = Boolean(effective.voice_match_enabled && effective.transcribe_subtitles);
    }
    Object.entries(effective).forEach(([name, enabled]) => {
      const status = form.querySelector(`[data-post-stream-status="${CSS.escape(name)}"]`);
      if (!status) return;
      status.textContent = enabled ? "Currently on" : "Currently off";
      status.classList.toggle("good", enabled);
    });
  };

  document.querySelectorAll("form[data-autosave]").forEach((form, index) => {
    if (!form.id) form.id = `autosave-form-${index + 1}`;
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      markAutosaveDirty(form, true);
    });
    form.addEventListener("input", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement)) return;
      if (["checkbox", "radio"].includes(target.type)) return;
      markAutosaveDirty(form, false);
    });
    form.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement || target instanceof HTMLSelectElement || target instanceof HTMLTextAreaElement)) return;
      if (target instanceof HTMLInputElement && target.type === "checkbox") {
        const stateLabel = target.closest(".switch-field")?.querySelector(":scope > span");
        if (stateLabel) stateLabel.textContent = target.checked ? "Enabled" : "Disabled";
        if (target.name === "powerchat_enabled") {
          const status = form.closest(".powerchat-listener-card")?.querySelector("[data-powerchat-listener-status]");
          if (status) {
            status.textContent = target.checked ? "Listening" : "Not enabled";
            status.classList.toggle("good", target.checked);
            status.classList.toggle("warning", !target.checked);
          }
        }
      }
      if (target.matches("[data-post-stream-select]")) updatePostStreamStatuses(form);
      markAutosaveDirty(form, ["checkbox", "radio"].includes(target.type) || target instanceof HTMLSelectElement);
    });
    form.addEventListener("focusout", (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.hasAttribute("name") && form.dataset.dirty === "true") markAutosaveDirty(form, true);
    });
  });

  document.addEventListener("click", (event) => {
    const reloadButton = event.target.closest("[data-reload-page]");
    if (reloadButton) {
      window.location.reload();
      return;
    }
    const retryButton = event.target.closest("[data-retry-form]");
    if (retryButton) {
      const form = document.getElementById(retryButton.dataset.retryForm);
      if (form) enqueueAutosave(form, { force: true });
      return;
    }
    const reapplyButton = event.target.closest("[data-reapply-form]");
    if (reapplyButton) {
      const form = document.getElementById(reapplyButton.dataset.reapplyForm);
      if (form) {
        body.dataset.configRevision = form.dataset.conflictRevision || body.dataset.configRevision;
        enqueueAutosave(form, { force: true });
      }
    }
  });

  document.querySelectorAll("[data-settings-search]").forEach((input) => {
    input.addEventListener("input", () => {
      const query = input.value.trim().toLowerCase();
      const root = input.closest("[data-settings-root]") || document;
      root.querySelectorAll("[data-setting-card]").forEach((card) => {
        card.hidden = Boolean(query) && !String(card.dataset.searchText || "").toLowerCase().includes(query);
      });
      root.querySelectorAll("[data-settings-group]").forEach((group) => {
        const visible = [...group.querySelectorAll("[data-setting-card]")].some((card) => !card.hidden);
        group.hidden = !visible;
      });
    });
  });

  document.querySelectorAll("[data-streamer-search]").forEach((input) => {
    input.addEventListener("input", () => {
      const query = input.value.trim().toLowerCase();
      document.querySelectorAll("[data-streamer-summary]").forEach((card) => {
        card.hidden = Boolean(query) && !String(card.dataset.searchText || "").toLowerCase().includes(query);
      });
    });
  });

  const applyActivityFilters = () => {
    const filters = document.querySelector("[data-activity-filters]");
    if (!filters) return;
    const query = String(filters.querySelector("[data-activity-search]")?.value || "").trim().toLowerCase();
    const state = String(filters.querySelector("[data-activity-state]")?.value || "").toLowerCase();
    document.querySelectorAll("[data-activity-record]").forEach((record) => {
      const matchesQuery = !query || String(record.dataset.searchText || "").includes(query);
      const matchesState = !state || String(record.dataset.state || "").toLowerCase() === state;
      record.hidden = !(matchesQuery && matchesState);
    });
  };
  document.querySelector("[data-activity-filters]")?.addEventListener("input", applyActivityFilters);
  document.querySelector("[data-activity-filters]")?.addEventListener("change", applyActivityFilters);

  const platformFromValue = (value, selected = "auto") => {
    selected = String(selected || "auto").toLowerCase();
    if (selected !== "auto") return selected;
    value = String(value || "").trim();
    const prefix = value.match(/^(youtube|twitch|kick|rumble):/i);
    if (prefix) return prefix[1].toLowerCase();
    try {
      const host = new URL(value).hostname.toLowerCase().replace(/^www\./, "");
      if (host.includes("youtube.com") || host === "youtu.be") return "youtube";
      if (host.endsWith("twitch.tv")) return "twitch";
      if (host.endsWith("kick.com")) return "kick";
      if (host.endsWith("rumble.com")) return "rumble";
    } catch (_) {}
    return "youtube";
  };

  const normalizeSource = (value, selected = "auto") => {
    value = String(value || "").trim();
    if (!value) return "";
    const platform = platformFromValue(value, selected);
    if (/^https?:\/\//i.test(value)) {
      try {
        const url = new URL(value);
        const parts = url.pathname.split("/").filter(Boolean);
        if (platform === "youtube" && parts[0]?.startsWith("@")) return parts[0];
        if (["twitch", "kick"].includes(platform) && parts[0]) return `${platform}:${parts[0]}`;
        if (platform === "rumble" && parts.length) return `rumble:${parts.join("/")}`;
      } catch (_) {}
      return value;
    }
    if (/^(youtube|twitch|kick|rumble):/i.test(value)) return value;
    const clean = value.replace(/^@+/, "").replace(/^\/+|\/+$/g, "");
    return platform === "youtube" ? `@${clean}` : `${platform}:${clean}`;
  };

  const readSources = (manager) => {
    const values = manager.querySelector("[data-source-values]")?.value || "";
    return String(values).split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
  };

  const sourceLabel = (source) => {
    const platform = platformFromValue(source);
    const value = source.replace(/^(youtube|twitch|kick|rumble):/i, "").replace(/^@/, "");
    return { platform, value: value.split("/").filter(Boolean).pop() || value };
  };

  const renderSources = (manager, sources) => {
    const list = manager.querySelector("[data-source-manager-list]");
    const values = manager.querySelector("[data-source-values]");
    const unique = [...new Set(sources)];
    if (values) values.value = unique.join("\n");
    if (!list) return;
    if (!unique.length) {
      list.innerHTML = '<div class="notice muted">No sources added yet.</div>';
      return;
    }
    list.innerHTML = unique.map((source) => {
      const meta = sourceLabel(source);
      return `<div class="source-row"><span class="source-platform-icon">${escapeHtml(meta.platform.slice(0, 1).toUpperCase())}</span><div><strong>${escapeHtml(meta.value)}</strong><small>${escapeHtml(source)}</small></div><button class="button small ghost" type="button" data-remove-source="${escapeHtml(source)}" aria-label="Remove ${escapeHtml(source)}">Remove</button></div>`;
    }).join("");
  };

  document.querySelectorAll("[data-source-manager]").forEach((manager) => {
    renderSources(manager, readSources(manager));
    manager.addEventListener("click", (event) => {
      const addButton = event.target.closest("[data-add-source]");
      const removeButton = event.target.closest("[data-remove-source]");
      if (addButton) {
        const input = manager.querySelector("[data-source-input]");
        const platform = manager.querySelector("[data-source-platform]");
        const source = normalizeSource(input?.value, platform?.value);
        if (!source) return;
        renderSources(manager, [...readSources(manager), source]);
        if (input) input.value = "";
        const form = manager.closest("form[data-autosave]");
        if (form) markAutosaveDirty(form, true);
        input?.focus();
      }
      if (removeButton) {
        renderSources(manager, readSources(manager).filter((source) => source !== removeButton.dataset.removeSource));
        const form = manager.closest("form[data-autosave]");
        if (form) markAutosaveDirty(form, true);
      }
    });
    manager.querySelector("[data-source-input]")?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        manager.querySelector("[data-add-source]")?.click();
      }
    });
  });

  document.addEventListener("click", (event) => {
    const openButton = event.target.closest("[data-open-dialog]");
    if (openButton) {
      const dialog = document.getElementById(openButton.dataset.openDialog);
      if (dialog instanceof HTMLDialogElement && !dialog.open) dialog.showModal();
    }
    const closeButton = event.target.closest("[data-close-dialog]");
    if (closeButton) closeButton.closest("dialog")?.close();
  });

  const confirmDialog = document.querySelector("[data-confirm-dialog]");
  let pendingConfirmation = null;
  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement) || form.dataset.confirmed === "true") return;
    const message = event.submitter?.dataset.confirm || form.dataset.confirm;
    if (!message) return;
    if (!confirmDialog?.showModal) {
      if (!window.confirm(message)) event.preventDefault();
      return;
    }
    event.preventDefault();
    pendingConfirmation = { form, submitter: event.submitter || null };
    confirmDialog.querySelector("[data-confirm-message]").textContent = message;
    confirmDialog.querySelector("[data-confirm-submit]").textContent = event.submitter?.dataset.confirmLabel || form.dataset.confirmLabel || "Confirm";
    confirmDialog.showModal();
  });
  confirmDialog?.addEventListener("close", () => {
    if (confirmDialog.returnValue === "confirm" && pendingConfirmation) {
      pendingConfirmation.form.dataset.confirmed = "true";
      pendingConfirmation.form.requestSubmit(pendingConfirmation.submitter || undefined);
    }
    pendingConfirmation = null;
  });

  let powerchatPage = 1;
  const powerchatStats = () => {
    const payload = document.getElementById("powerchat-stats-json");
    if (!payload) return { events: [], stream_totals: [] };
    try { return JSON.parse(payload.textContent || "{}"); } catch (_) { return { events: [], stream_totals: [] }; }
  };
  const powerchatNumber = (value, decimals = 0) => {
    const number = Number(value || 0);
    if (!decimals && Number.isInteger(number)) return String(number);
    return decimals ? number.toFixed(decimals) : String(number);
  };
  const powerchatSummary = (money = [], units = []) => [
    ...money.map((row) => row.currency && row.amount != null ? `${String(row.currency).toUpperCase()} ${powerchatNumber(row.amount, 2)}` : ""),
    ...units.map((row) => row.unit && row.amount != null ? `${row.platform ? `${row.platform}: ` : ""}${powerchatNumber(row.amount)} ${row.unit}` : ""),
  ].filter(Boolean).join(", ");
  const powerchatRates = (rows = []) => rows.map((row) => row.currency && row.amount_per_hour != null ? `${String(row.currency).toUpperCase()} ${powerchatNumber(row.amount_per_hour, 2)}/hr` : "").filter(Boolean).join(", ");
  const powerchatDuration = (value) => {
    let seconds = Math.max(0, Math.trunc(Number(value) || 0));
    const hours = Math.trunc(seconds / 3600);
    const minutes = Math.trunc((seconds % 3600) / 60);
    seconds %= 60;
    return hours ? `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}` : `${minutes}:${String(seconds).padStart(2, "0")}`;
  };
  const powerchatDateTime = (value) => {
    if (!value) return "-";
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  };
  const newPowerchatBucket = () => ({ event_count: 0, money: {}, money_counts: {}, units: {} });
  const addPowerchatEvent = (bucket, event) => {
    bucket.event_count += 1;
    if (event.kind === "money" && event.money_amount != null && event.money_currency) {
      const currency = String(event.money_currency).toUpperCase();
      bucket.money[currency] = Number(bucket.money[currency] || 0) + Number(event.money_amount || 0);
      bucket.money_counts[currency] = Number(bucket.money_counts[currency] || 0) + 1;
    } else if (event.kind === "unit" && event.unit_amount != null && event.unit) {
      const key = `${event.platform || ""}::${event.unit}`;
      bucket.units[key] = Number(bucket.units[key] || 0) + Number(event.unit_amount || 0);
    }
  };
  const powerchatMoney = (bucket) => Object.entries(bucket.money).sort(([a], [b]) => a.localeCompare(b)).map(([currency, amount]) => ({ currency, amount: Math.round(Number(amount) * 100) / 100 }));
  const powerchatUnits = (bucket) => Object.entries(bucket.units).sort(([a], [b]) => a.localeCompare(b)).map(([key, amount]) => {
    const [platform, unit] = key.split("::");
    return { platform, unit, amount: Math.round(Number(amount) * 100) / 100 };
  });
  const powerchatAverages = (bucket) => powerchatMoney(bucket).map((row) => ({ currency: row.currency, amount: Math.round((row.amount / Number(bucket.money_counts[row.currency] || 1)) * 100) / 100 }));
  const powerchatRateRows = (bucket, duration) => duration > 0 ? powerchatMoney(bucket).map((row) => ({ ...row, amount_per_hour: Math.round((row.amount / (duration / 3600)) * 100) / 100 })) : [];
  const powerchatSortAmount = (bucket) => Object.values(bucket.money).reduce((total, amount) => total + Number(amount || 0), 0);
  const finishPowerchatDonors = (donors) => [...donors.values()].map((row) => ({
    donor: row.donor,
    event_count: row.bucket.event_count,
    latest_received_at: row.latest,
    money_totals: powerchatMoney(row.bucket),
    unit_totals: powerchatUnits(row.bucket),
    sort_amount: powerchatSortAmount(row.bucket),
  })).sort((a, b) => b.sort_amount - a.sort_amount || b.event_count - a.event_count || a.donor.localeCompare(b.donor));
  const finishPowerchatHours = (hours) => [...hours.values()].map((row) => ({
    hour_index: row.hour_index,
    hour_label: row.hour_label,
    event_count: row.bucket.event_count,
    money_totals: powerchatMoney(row.bucket),
    unit_totals: powerchatUnits(row.bucket),
    average_money: powerchatAverages(row.bucket),
  })).sort((a, b) => a.hour_index - b.hour_index);
  const addPowerchatDonor = (donors, event) => {
    const name = event.donor || "Unknown donor";
    if (!donors.has(name)) donors.set(name, { donor: name, latest: "", bucket: newPowerchatBucket() });
    const donor = donors.get(name);
    addPowerchatEvent(donor.bucket, event);
    if (event.received_at && event.received_at > donor.latest) donor.latest = event.received_at;
  };
  const addPowerchatHour = (hours, event) => {
    if (event.hour_index == null) return false;
    const index = Number(event.hour_index || 0);
    if (!hours.has(index)) hours.set(index, { hour_index: index, hour_label: event.hour_label || `${index}:00-${index}:59`, bucket: newPowerchatBucket() });
    addPowerchatEvent(hours.get(index).bucket, event);
    return true;
  };
  const aggregatePowerchat = (events, sourceStats) => {
    const durations = new Map((sourceStats.stream_totals || []).map((row) => [row.video_id, Number(row.duration_seconds || 0)]));
    const total = newPowerchatBucket();
    const donors = new Map();
    const hours = new Map();
    const streams = new Map();
    const streamers = new Map();
    let withoutOffset = 0;
    events.forEach((event) => {
      addPowerchatEvent(total, event);
      addPowerchatDonor(donors, event);
      if (!addPowerchatHour(hours, event)) withoutOffset += 1;
      const streamKey = event.video_id || event.stream_title || "unknown";
      if (!streams.has(streamKey)) streams.set(streamKey, { streamer: event.streamer || "", video_id: event.video_id || "", title: event.stream_title || "-", duration_seconds: durations.get(event.video_id) || 0, bucket: newPowerchatBucket() });
      addPowerchatEvent(streams.get(streamKey).bucket, event);
      const streamerName = event.streamer || "Unknown streamer";
      if (!streamers.has(streamerName)) streamers.set(streamerName, { streamer: streamerName, bucket: newPowerchatBucket(), donors: new Map(), hours: new Map(), streams: new Map(), without_offset: 0 });
      const streamer = streamers.get(streamerName);
      addPowerchatEvent(streamer.bucket, event);
      addPowerchatDonor(streamer.donors, event);
      if (!addPowerchatHour(streamer.hours, event)) streamer.without_offset += 1;
      if (!streamer.streams.has(streamKey)) streamer.streams.set(streamKey, streams.get(streamKey));
    });
    const finishStreams = (rows) => [...rows.values()].map((row) => ({
      streamer: row.streamer,
      video_id: row.video_id,
      title: row.title,
      event_count: row.bucket.event_count,
      duration_seconds: row.duration_seconds,
      money_totals: powerchatMoney(row.bucket),
      unit_totals: powerchatUnits(row.bucket),
      money_rates: powerchatRateRows(row.bucket, row.duration_seconds),
      sort_amount: powerchatSortAmount(row.bucket),
    })).sort((a, b) => b.sort_amount - a.sort_amount || b.event_count - a.event_count);
    const streamRows = finishStreams(streams);
    const duration = streamRows.reduce((totalDuration, row) => totalDuration + row.duration_seconds, 0);
    const streamerRows = [...streamers.values()].map((row) => {
      const childStreams = finishStreams(row.streams);
      const childDuration = childStreams.reduce((totalDuration, stream) => totalDuration + stream.duration_seconds, 0);
      return {
        streamer: row.streamer,
        event_count: row.bucket.event_count,
        stream_count: childStreams.length,
        duration_seconds: childDuration,
        events_without_offset: row.without_offset,
        money_totals: powerchatMoney(row.bucket),
        unit_totals: powerchatUnits(row.bucket),
        money_rates: powerchatRateRows(row.bucket, childDuration),
        top_donors: finishPowerchatDonors(row.donors).slice(0, 10),
        hourly_totals: finishPowerchatHours(row.hours),
        stream_totals: childStreams,
        sort_amount: powerchatSortAmount(row.bucket),
      };
    }).sort((a, b) => b.sort_amount - a.sort_amount || b.event_count - a.event_count || a.streamer.localeCompare(b.streamer));
    return {
      event_count: events.length,
      streams_with_powerchat: streamRows.length,
      events_without_offset: withoutOffset,
      money_totals: powerchatMoney(total),
      unit_totals: powerchatUnits(total),
      money_rates: powerchatRateRows(total, duration),
      top_donors: finishPowerchatDonors(donors).slice(0, 25),
      hourly_totals: finishPowerchatHours(hours),
      stream_totals: streamRows,
      streamer_dashboards: streamerRows,
    };
  };
  const readPowerchatFilters = () => {
    const value = (selector, fallback = "") => document.querySelector(selector)?.value || fallback;
    const pageSize = Number(value("[data-powerchat-page-size]", "50"));
    return {
      streamer: value("[data-powerchat-filter-streamer]", "all"),
      platform: value("[data-powerchat-filter-platform]", "all"),
      kind: value("[data-powerchat-filter-kind]", "all"),
      from: value("[data-powerchat-filter-from]"),
      to: value("[data-powerchat-filter-to]"),
      search: value("[data-powerchat-filter-search]"),
      page_size: [25, 50, 100].includes(pageSize) ? pageSize : 50,
    };
  };
  const powerchatEventMatches = (event, filters) => {
    if (filters.streamer !== "all" && event.streamer !== filters.streamer) return false;
    if (filters.platform !== "all" && event.platform !== filters.platform) return false;
    if (filters.kind !== "all" && event.kind !== filters.kind) return false;
    const date = String(event.received_at || "").slice(0, 10);
    if (filters.from && date && date < filters.from) return false;
    if (filters.to && date && date > filters.to) return false;
    const query = filters.search.trim().toLowerCase();
    return !query || [event.donor, event.message, event.stream_title, event.streamer, event.video_id, event.platform].join(" ").toLowerCase().includes(query);
  };
  const powerchatExportUrl = (format, filters = {}) => {
    const params = new URLSearchParams({ format });
    ["streamer", "video_id", "platform", "kind", "from", "to", "search"].forEach((key) => {
      const value = String(filters[key] || "").trim();
      if (value && value !== "all") params.set(key, value);
    });
    return `/powerchat-events?${params}`;
  };
  const renderPowerchatSummaryCards = (stats, streamer = false) => {
    const donor = stats.top_donors?.[0]?.donor || "-";
    const cards = [
      ["Total", powerchatSummary(stats.money_totals, stats.unit_totals) || "-"],
      ["Per hour", powerchatRates(stats.money_rates) || "-"],
      ["Events", String(stats.event_count || 0)],
      [streamer ? "Streams" : "Top donor", streamer ? String(stats.stream_count || 0) : donor],
      [streamer ? "Top donor" : "Streams", streamer ? donor : String(stats.streams_with_powerchat || 0)],
      ["No offset", String(stats.events_without_offset || 0)],
    ];
    return cards.map(([label, value]) => `<div class="powerchat-summary-card"><strong>${escapeHtml(value)}</strong><span class="muted">${escapeHtml(label)}</span></div>`).join("");
  };
  const renderPowerchatHours = (rows = []) => rows.length ? rows.map((row) => `<tr><td>${escapeHtml(row.hour_label || "-")}</td><td>${row.event_count || 0}</td><td>${escapeHtml(powerchatSummary(row.money_totals, row.unit_totals) || "-")}</td><td>${escapeHtml(powerchatSummary(row.average_money, []) || "-")}</td></tr>`).join("") : '<tr><td colspan="4" class="file-meta">No hourly Powerchat events captured yet</td></tr>';
  const renderPowerchatStreams = (rows = []) => rows.length ? rows.slice(0, 50).map((row) => `<tr><td>${escapeHtml(row.streamer || "-")}</td><td class="file-name">${escapeHtml(row.title || row.video_id || "-")}</td><td>${row.event_count || 0}</td><td>${escapeHtml(powerchatSummary(row.money_totals, row.unit_totals) || "-")}</td><td>${escapeHtml(powerchatDuration(row.duration_seconds))}</td><td>${escapeHtml(powerchatRates(row.money_rates) || "-")}</td></tr>`).join("") : '<tr><td colspan="6" class="file-meta">No streams with Powerchat events yet</td></tr>';
  const renderPowerchatDonors = (rows = []) => rows.length ? rows.slice(0, 25).map((row) => `<tr><td>${escapeHtml(row.donor || "Unknown donor")}</td><td>${row.event_count || 0}</td><td>${escapeHtml(powerchatSummary(row.money_totals, row.unit_totals) || "-")}</td><td>${escapeHtml(powerchatDateTime(row.latest_received_at))}</td></tr>`).join("") : '<tr><td colspan="4" class="file-meta">No Powerchat donors yet</td></tr>';
  const powerchatAmount = (event) => event.kind === "money" && event.money_currency ? `${String(event.money_currency).toUpperCase()} ${powerchatNumber(event.money_amount, 2)}` : event.kind === "unit" && event.unit ? `${event.platform ? `${event.platform}: ` : ""}${powerchatNumber(event.unit_amount)} ${event.unit}` : "-";
  const renderPowerchatLedger = (events = []) => events.length ? events.map((event) => `<tr><td>${escapeHtml(event.offset_seconds == null ? powerchatDateTime(event.received_at) : powerchatDuration(event.offset_seconds))}</td><td>${escapeHtml(event.streamer || "-")}</td><td class="file-name">${escapeHtml(event.stream_title || event.video_id || "-")}</td><td>${escapeHtml(event.donor || "Unknown donor")}</td><td>${escapeHtml(powerchatAmount(event))}</td><td>${escapeHtml(event.platform || "Powerchat")}</td><td class="log-message">${escapeHtml(event.message || "-")}</td></tr>`).join("") : '<tr><td colspan="7" class="file-meta">No Powerchat events captured yet</td></tr>';
  const renderPowerchatStreamers = (rows = []) => rows.length ? rows.map((row, index) => `<details class="powerchat-streamer-card"${index === 0 ? " open" : ""}><summary><strong>${escapeHtml(row.streamer)}</strong><span>Total: ${escapeHtml(powerchatSummary(row.money_totals, row.unit_totals) || "-")}</span><span>Rate: ${escapeHtml(powerchatRates(row.money_rates) || "-")}</span><span>${row.stream_count || 0} streams</span><span>${row.event_count || 0} events</span></summary><div class="powerchat-streamer-card-body"><div class="powerchat-export-actions"><a class="download action-button" href="${escapeHtml(powerchatExportUrl("json", { streamer: row.streamer }))}">Download JSON</a><a class="download action-button" href="${escapeHtml(powerchatExportUrl("csv", { streamer: row.streamer }))}">Download CSV</a></div><div class="powerchat-summary-grid">${renderPowerchatSummaryCards(row, true)}</div><div class="powerchat-dashboard-section"><h4>Donations Per Hour</h4><div class="table-wrap"><table><thead><tr><th>Stream Hour</th><th>Events</th><th>Total</th><th>Average</th></tr></thead><tbody>${renderPowerchatHours(row.hourly_totals)}</tbody></table></div></div><div class="powerchat-dashboard-section"><h4>Streams</h4><div class="table-wrap"><table><thead><tr><th>Streamer</th><th>Stream</th><th>Events</th><th>Total</th><th>Duration</th><th>Per hour</th></tr></thead><tbody>${renderPowerchatStreams(row.stream_totals)}</tbody></table></div></div><div class="powerchat-dashboard-section"><h4>Top Donors</h4><div class="table-wrap"><table><thead><tr><th>Donor</th><th>Events</th><th>Total</th><th>Latest</th></tr></thead><tbody>${renderPowerchatDonors(row.top_donors)}</tbody></table></div></div></div></details>`).join("") : '<div class="file-meta">No streamers with Powerchat events yet.</div>';
  const renderPowerchatDashboard = () => {
    if (!document.getElementById("powerchat-dashboard")) return;
    const sourceStats = powerchatStats();
    const filters = readPowerchatFilters();
    const events = (sourceStats.events || []).filter((event) => powerchatEventMatches(event, filters));
    const stats = aggregatePowerchat(events, sourceStats);
    document.querySelectorAll("[data-powerchat-export]").forEach((link) => { link.href = powerchatExportUrl(link.dataset.powerchatExport || "json", filters); });
    const maxPage = Math.max(1, Math.ceil(events.length / filters.page_size));
    powerchatPage = Math.min(Math.max(1, powerchatPage), maxPage);
    const start = (powerchatPage - 1) * filters.page_size;
    const pageEvents = events.slice(start, start + filters.page_size);
    const setHtml = (id, html) => { const element = document.getElementById(id); if (element) element.innerHTML = html; };
    setHtml("powerchat-summary-cards", renderPowerchatSummaryCards(stats));
    setHtml("powerchat-streamer-rows", renderPowerchatStreamers(stats.streamer_dashboards));
    setHtml("powerchat-hourly-rows", renderPowerchatHours(stats.hourly_totals));
    setHtml("powerchat-stream-rows", renderPowerchatStreams(stats.stream_totals));
    setHtml("powerchat-donor-rows", renderPowerchatDonors(stats.top_donors));
    setHtml("powerchat-ledger-rows", renderPowerchatLedger(pageEvents));
    const state = document.getElementById("powerchat-ledger-state");
    if (state) state.textContent = events.length ? `Showing ${start + 1}–${Math.min(start + pageEvents.length, events.length)} of ${events.length} events` : "Showing 0 events";
    const previous = document.querySelector("[data-powerchat-page-prev]");
    const next = document.querySelector("[data-powerchat-page-next]");
    if (previous) previous.disabled = powerchatPage <= 1;
    if (next) next.disabled = powerchatPage >= maxPage;
  };
  document.addEventListener("input", (event) => {
    if (!event.target.closest("[data-powerchat-filter-control]")) return;
    powerchatPage = 1;
    renderPowerchatDashboard();
  });
  document.addEventListener("change", (event) => {
    if (!event.target.closest("[data-powerchat-filter-control]")) return;
    powerchatPage = 1;
    renderPowerchatDashboard();
  });
  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-powerchat-page-prev], [data-powerchat-page-next]");
    if (!button) return;
    event.preventDefault();
    powerchatPage += button.hasAttribute("data-powerchat-page-next") ? 1 : -1;
    renderPowerchatDashboard();
  });

  const loadVoiceDetails = async (button) => {
    const root = button.closest(".voice-settings");
    const panel = button.closest("[data-voice-details]");
    const streamer = panel?.dataset.streamerName || "";
    const state = panel?.querySelector("[data-voice-details-state]");
    if (!root || !panel || !streamer) return;
    button.disabled = true;
    if (state) state.textContent = "Loading voice details…";
    try {
      const response = await fetch(`/streamer-voice-details?streamer=${encodeURIComponent(streamer)}`, { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const review = root.querySelector("[data-voice-review]");
      if (review) review.innerHTML = payload.review || '<div class="file-meta">No voice matches found.</div>';
    } catch (error) {
      button.disabled = false;
      if (state) state.textContent = `Unable to load voice details: ${error.message || error}`;
    }
  };

  const loadStreamSpeakers = async (button) => {
    const panel = button.closest("[data-stream-speakers]");
    const streamer = panel?.dataset.streamerName || "";
    const videoId = panel?.dataset.videoId || "";
    const state = panel?.querySelector("[data-stream-speakers-state]");
    if (!panel || !streamer || !videoId) return;
    button.disabled = true;
    if (state) state.textContent = "Loading detected speakers…";
    try {
      const query = new URLSearchParams({ streamer, video_id: videoId });
      const response = await fetch(`/stream-voice-speakers?${query}`, { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      panel.innerHTML = payload.speakers || '<div class="file-meta">No diarized transcript speakers found yet.</div>';
    } catch (error) {
      button.disabled = false;
      if (state) state.textContent = `Unable to load detected speakers: ${error.message || error}`;
    }
  };

  document.addEventListener("click", (event) => {
    const voiceButton = event.target.closest("[data-load-voice-details]");
    if (voiceButton) {
      event.preventDefault();
      loadVoiceDetails(voiceButton);
      return;
    }
    const speakerButton = event.target.closest("[data-load-stream-speakers]");
    if (speakerButton) {
      event.preventDefault();
      loadStreamSpeakers(speakerButton);
    }
  });

  const captureDetailsState = (region) => new Map(
    [...region.querySelectorAll("details[data-details-key]")].map((details) => [
      details.dataset.detailsKey,
      details.open,
    ]),
  );

  const restoreDetailsState = (region, state) => {
    region.querySelectorAll("details[data-details-key]").forEach((details) => {
      if (state.has(details.dataset.detailsKey)) {
        details.open = state.get(details.dataset.detailsKey);
      }
    });
  };

  const refreshFragment = async (region) => {
    if (document.hidden || region.matches(":focus-within") || region.querySelector('[data-dirty="true"]')) return;
    try {
      const response = await fetch(region.dataset.fragmentUrl, { headers: { "X-Dashboard-Fragment": "1" }, cache: "no-store" });
      if (!response.ok) return;
      const revision = response.headers.get("X-Fragment-Revision") || "";
      if (revision && revision === region.dataset.fragmentRevision) return;
      const html = await response.text();
      if (region.matches(":focus-within") || region.querySelector('[data-dirty="true"]')) return;
      const detailsState = captureDetailsState(region);
      region.innerHTML = html;
      restoreDetailsState(region, detailsState);
      if (region.querySelector("#powerchat-dashboard")) powerchatPage = 1;
      if (revision) region.dataset.fragmentRevision = revision;
      applyActivityFilters();
      const stamp = document.querySelector("[data-last-refreshed]");
      if (stamp) stamp.textContent = `Updated ${new Date().toLocaleTimeString()}`;
    } catch (_) {}
  };
  document.querySelectorAll("[data-fragment-url]").forEach((region) => {
    window.setInterval(() => refreshFragment(region), 15000);
  });
})();
