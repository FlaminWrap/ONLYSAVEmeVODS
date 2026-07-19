from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from html import escape
from pathlib import Path


@dataclass(frozen=True, slots=True)
class NavigationItem:
    key: str
    label: str
    href: str
    icon: str


NAVIGATION_ITEMS: tuple[NavigationItem, ...] = (
    NavigationItem("overview", "Overview", "/", "home"),
    NavigationItem("streamers", "Streamers", "/streamers", "person"),
    NavigationItem("settings", "Settings", "/settings", "sliders"),
    NavigationItem("powerchat", "Powerchat", "/powerchat", "currency"),
    NavigationItem("activity", "Activity", "/activity", "activity"),
    NavigationItem("tools", "Tools", "/tools", "hammer"),
    NavigationItem("about", "About", "/about", "info"),
)

NAVIGATION_ICONS: dict[str, str] = {
    "home": '<svg viewBox="0 0 24 24" focusable="false"><path d="M3.5 10.5 12 3.5l8.5 7"/><path d="M5.5 9v11h13V9"/><path d="M9.5 20v-6h5v6"/></svg>',
    "person": '<svg viewBox="0 0 24 24" focusable="false"><circle cx="12" cy="8" r="3.25"/><path d="M5.5 20c.5-4 2.7-6 6.5-6s6 2 6.5 6"/></svg>',
    "sliders": '<svg viewBox="0 0 24 24" focusable="false"><path d="M4 7h7m4 0h5M4 17h5m4 0h7"/><circle cx="13" cy="7" r="2"/><circle cx="11" cy="17" r="2"/></svg>',
    "currency": '<svg viewBox="0 0 24 24" focusable="false"><path d="M15.5 8.5c-.8-.7-1.9-1-3.2-1-1.8 0-3.1.9-3.1 2.3 0 3.4 6.4 1.7 6.4 5.2 0 1.5-1.4 2.5-3.5 2.5-1.5 0-2.8-.5-3.7-1.4M12 5.5v14"/></svg>',
    "activity": '<svg viewBox="0 0 24 24" focusable="false"><path d="M3 12h4l2.2-5 4.2 10 2.2-5H21"/></svg>',
    "hammer": '<svg viewBox="0 0 24 24" focusable="false"><path d="M6.5 21a2.1 2.1 0 0 1-3-3L13 8.5l3 3Z"/><path d="m11 5 3-3 8 8-3 3-3-3-2 2-5-5Z"/></svg>',
    "info": '<svg viewBox="0 0 24 24" focusable="false"><circle cx="12" cy="12" r="8.5"/><path d="M12 11v5"/><path d="M12 8h.01"/></svg>',
}

MENU_ICON = '<svg viewBox="0 0 24 24" focusable="false"><path d="M4 7h16M4 12h16M4 17h16"/></svg>'


def dashboard_asset_revision() -> str:
    """Fingerprint packaged UI assets so HTML and CSS/JS cannot drift apart."""

    digest = sha256()
    asset_dir = Path(__file__).resolve().parent / "assets"
    try:
        for name in ("dashboard.css", "dashboard.js"):
            digest.update((asset_dir / name).read_bytes())
    except OSError:
        return ""
    return digest.hexdigest()[:16]


def render_dashboard_shell(
    *,
    active: str,
    title: str,
    content: str,
    app_version: str,
    subtitle: str = "",
    config_revision: str = "",
    page_actions: str = "",
    body_attributes: str = "",
) -> str:
    """Render the shared no-build dashboard shell.

    Page renderers deliberately pass already-escaped HTML for ``content`` and
    ``page_actions``. Keeping the shell independent from the status models lets
    the web server build lightweight page-specific views.
    """

    navigation = "".join(_render_navigation_item(item, active) for item in NAVIGATION_ITEMS)
    subtitle_html = f'<p class="page-subtitle">{escape(subtitle)}</p>' if subtitle else ""
    revision_attr = (
        f' data-config-revision="{escape(config_revision, quote=True)}"'
        if config_revision
        else ""
    )
    extra_body_attributes = f" {body_attributes.strip()}" if body_attributes.strip() else ""
    asset_revision = dashboard_asset_revision() or app_version
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light dark">
  <link rel="icon" href="/favicon.ico?v={escape(app_version, quote=True)}" sizes="any">
  <link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png?v={escape(app_version, quote=True)}">
  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v={escape(app_version, quote=True)}">
  <link rel="stylesheet" href="/assets/dashboard.css?v={escape(asset_revision, quote=True)}">
  <title>{escape(title)} · ONLYSAVEmeVODS</title>
</head>
<body data-page="{escape(active, quote=True)}"{revision_attr}{extra_body_attributes}>
  <a class="skip-link" href="#main-content">Skip to content</a>
  <div class="app-shell">
    <aside class="app-sidebar" id="app-sidebar" aria-label="Primary navigation">
      <div class="brand-block">
        <img src="/Favicon.png?v={escape(app_version, quote=True)}" alt="" width="36" height="36">
        <div><strong>ONLYSAVEmeVODS</strong><span>Administration</span></div>
      </div>
      <nav class="primary-nav">{navigation}</nav>
      <div class="sidebar-footer">
        <span class="service-indicator"><span aria-hidden="true"></span>Local service</span>
        <small>Version {escape(app_version)}</small>
      </div>
    </aside>
    <button class="sidebar-scrim" type="button" data-close-navigation tabindex="-1" aria-label="Close navigation"></button>
    <div class="app-workspace">
      <header class="app-topbar">
        <button class="icon-button menu-button" type="button" data-open-navigation aria-controls="app-sidebar" aria-expanded="false">
          <span aria-hidden="true">{MENU_ICON}</span><span class="sr-only">Open navigation</span>
        </button>
        <div class="topbar-context"><span class="topbar-product">ONLYSAVEmeVODS</span><span aria-hidden="true">/</span><span>{escape(title)}</span></div>
        <div class="save-status" data-save-status role="status" aria-live="polite" aria-atomic="true"></div>
      </header>
      <main id="main-content" class="page-content" tabindex="-1">
        <div class="page-heading">
          <div><h1>{escape(title)}</h1>{subtitle_html}</div>
          <div class="page-actions">{page_actions}</div>
        </div>
        {content}
      </main>
    </div>
  </div>
  <dialog class="confirm-dialog" data-confirm-dialog aria-labelledby="confirm-dialog-title">
    <form method="dialog">
      <h2 id="confirm-dialog-title">Confirm action</h2>
      <p data-confirm-message></p>
      <div class="dialog-actions">
        <button class="button secondary" value="cancel">Cancel</button>
        <button class="button danger" value="confirm" data-confirm-submit>Confirm</button>
      </div>
    </form>
  </dialog>
  <script src="/assets/dashboard.js?v={escape(asset_revision, quote=True)}" defer></script>
</body>
</html>
"""


def _render_navigation_item(item: NavigationItem, active: str) -> str:
    current = ' aria-current="page"' if item.key == active else ""
    active_class = " active" if item.key == active else ""
    return (
        f'<a class="nav-item{active_class}" href="{escape(item.href, quote=True)}"{current}>'
        f'<span class="nav-icon" aria-hidden="true">{NAVIGATION_ICONS[item.icon]}</span>'
        f'<span>{escape(item.label)}</span></a>'
    )
