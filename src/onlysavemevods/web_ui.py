from __future__ import annotations

from dataclasses import dataclass
from html import escape


@dataclass(frozen=True, slots=True)
class NavigationItem:
    key: str
    label: str
    href: str
    icon: str


NAVIGATION_ITEMS: tuple[NavigationItem, ...] = (
    NavigationItem("overview", "Overview", "/", "⌂"),
    NavigationItem("streamers", "Streamers", "/streamers", "◉"),
    NavigationItem("settings", "Settings", "/settings", "⚙"),
    NavigationItem("powerchat", "Powerchat", "/powerchat", "$"),
    NavigationItem("activity", "Activity", "/activity", "≋"),
    NavigationItem("tools", "Tools", "/tools", "◇"),
    NavigationItem("about", "About", "/about", "i"),
)


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
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light dark">
  <link rel="icon" href="/favicon.ico?v={escape(app_version, quote=True)}" sizes="any">
  <link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png?v={escape(app_version, quote=True)}">
  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png?v={escape(app_version, quote=True)}">
  <link rel="stylesheet" href="/assets/dashboard.css?v={escape(app_version, quote=True)}">
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
          <span aria-hidden="true">☰</span><span class="sr-only">Open navigation</span>
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
  <script src="/assets/dashboard.js?v={escape(app_version, quote=True)}" defer></script>
</body>
</html>
"""


def _render_navigation_item(item: NavigationItem, active: str) -> str:
    current = ' aria-current="page"' if item.key == active else ""
    active_class = " active" if item.key == active else ""
    return (
        f'<a class="nav-item{active_class}" href="{escape(item.href, quote=True)}"{current}>'
        f'<span class="nav-icon" aria-hidden="true">{escape(item.icon)}</span>'
        f'<span>{escape(item.label)}</span></a>'
    )
