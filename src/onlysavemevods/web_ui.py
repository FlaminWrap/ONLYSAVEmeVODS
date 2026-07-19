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
    NavigationItem("tools", "Tools", "/tools", "pipe_wrench"),
    NavigationItem("about", "About", "/about", "info"),
)

NAVIGATION_ICONS: dict[str, str] = {
    "home": '<svg viewBox="0 0 24 24" focusable="false"><path d="M3.5 10.5 12 3.5l8.5 7"/><path d="M5.5 9v11h13V9"/><path d="M9.5 20v-6h5v6"/></svg>',
    "person": '<svg viewBox="0 0 24 24" focusable="false"><circle cx="12" cy="8" r="3.25"/><path d="M5.5 20c.5-4 2.7-6 6.5-6s6 2 6.5 6"/></svg>',
    "sliders": '<svg viewBox="0 0 24 24" focusable="false"><path d="M4 7h7m4 0h5M4 17h5m4 0h7"/><circle cx="13" cy="7" r="2"/><circle cx="11" cy="17" r="2"/></svg>',
    "currency": '<svg viewBox="0 0 24 24" focusable="false"><path d="M15.5 8.5c-.8-.7-1.9-1-3.2-1-1.8 0-3.1.9-3.1 2.3 0 3.4 6.4 1.7 6.4 5.2 0 1.5-1.4 2.5-3.5 2.5-1.5 0-2.8-.5-3.7-1.4M12 5.5v14"/></svg>',
    "activity": '<svg viewBox="0 0 24 24" focusable="false"><path d="M3 12h4l2.2-5 4.2 10 2.2-5H21"/></svg>',
    # CC0 outline supplied by SVG Repo's "Pipe Wrench".
    "pipe_wrench": '<svg viewBox="0 0 194.799 194.799" focusable="false" data-tool-style="pipe-wrench"><path style="fill:currentColor;stroke:currentColor;stroke-width:6;stroke-linejoin:round" d="M13.865 187.225c-4.012 0-7.836-1.734-10.491-4.758-4.829-5.497-4.43-13.984.908-19.322L138.327 29.101c.938-.938 2.598-.938 3.535 0l20.617 20.617c1.91-1.983 4.885-5.164 6.287-7.149.423-.598.706-1.306.843-2.102.23-1.35-.07-2.631-.846-3.607l-15.528-19.533c-.792-.997-.709-2.429.193-3.327l4.332-4.313c.086-.086.179-.166.277-.239.26-.192 2.652-1.874 6.738-1.874 5.188 0 10.628 2.605 16.167 7.745 8.909 8.266 13.698 16.422 13.852 23.586.09 4.179-1.442 7.984-4.43 11.005-6.303 6.372-17.573 17.589-20.074 20.077-.72 2.701-2.085 5.205-3.975 7.282-4.545 4.996-9.839 9.438-15.766 13.232l1.6 1.6c.469.469.732 1.104.732 1.768s-.264 1.299-.732 1.768l-13.987 13.987c-.977.977-2.559.977-3.535 0l-2.752-2.752-40.112 40.111c-1.42 1.42-3.309 2.203-5.317 2.203s-3.896-.782-5.316-2.203l-2.186-2.185c-1.421-1.42-2.203-3.309-2.203-5.318s.782-3.897 2.203-5.317l33.94-33.941c-2.356-.037-4.388-.164-5.943-.297l-83.218 83.218c-2.586 2.586-6.087 4.036-9.812 4.036ZM140.095 34.404 7.818 166.68c-3.479 3.48-3.781 8.965-.688 12.487 1.707 1.943 4.162 3.058 6.735 3.058 2.389 0 4.634-.93 6.323-2.619l84.049-84.049c.531-.531 1.271-.793 2.021-.72 1.762.18 4.582.395 8.013.395 1.21 0 2.426-.027 3.627-.08l.783-.783-2.78-2.78c-.977-.976-.977-2.559 0-3.535l.34-.34.007-.007 13.987-13.987c.977-.977 2.559-.977 3.535 0l13.155 13.155c5.93-3.68 11.205-8.039 15.692-12.971 1.411-1.551 2.412-3.434 2.906-5.46.01-.084.024-.169.043-.253.936-4.22-.312-8.574-3.335-11.647l1.703-1.833-1.777 1.758-22.061-22.064ZM122.591 97.586 82.48 137.698c-.477.477-.738 1.109-.738 1.782s.262 1.306.738 1.782l2.186 2.185c.953.954 2.61.952 3.563 0l40.111-40.111-5.749-5.75Zm-1.04-8.111 14.845 14.844 10.452-10.451-14.845-14.844-10.452 10.451Zm44.462-36.215c2.487 2.622 4.074 5.867 4.641 9.31 4.581-4.564 11.704-11.674 16.157-16.176 2.042-2.064 3.047-4.547 2.986-7.381-.122-5.697-4.475-12.81-12.254-20.029-5.711-5.298-10.095-6.41-12.767-6.41-2.03 0-3.289.62-3.677.843l-2.56 2.549 14.138 17.783c1.673 2.104 2.333 4.79 1.859 7.561-.262 1.538-.83 2.933-1.687 4.146-1.61 2.259-4.829 5.702-6.836 7.784Z"/></svg>',
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
