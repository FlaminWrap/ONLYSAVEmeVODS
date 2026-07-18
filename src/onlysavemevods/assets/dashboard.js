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

  const refreshFragment = async (region) => {
    if (document.hidden || region.matches(":focus-within") || region.querySelector('[data-dirty="true"]')) return;
    try {
      const response = await fetch(region.dataset.fragmentUrl, { headers: { "X-Dashboard-Fragment": "1" }, cache: "no-store" });
      if (!response.ok) return;
      const revision = response.headers.get("X-Fragment-Revision") || "";
      if (revision && revision === region.dataset.fragmentRevision) return;
      const html = await response.text();
      if (region.matches(":focus-within") || region.querySelector('[data-dirty="true"]')) return;
      region.innerHTML = html;
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
