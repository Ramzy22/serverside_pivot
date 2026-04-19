# Handoff: serverside-pivot Design System

## Overview

This package documents the complete visual design system for **serverside-pivot** (`DashTanstackPivot`) — an enterprise-grade Python/React pivot table component embedded in Plotly Dash apps. The target users are quant analysts and data engineers building financial analytics dashboards.

The goal of this handoff is to wire the design system tokens and component patterns into the actual `dash_tanstack_pivot` React codebase so that theming, spacing, and typography are driven by a consistent, maintainable token layer.

## About the Design Files

The files in `reference/` are **design references created as HTML prototypes** — they show the intended look, interaction states, and theme variants. They are **not production code to copy directly**. The task is to **recreate these designs within the existing `dash_tanstack_pivot` React component tree**, using the established patterns in `dash_tanstack_pivot/src/lib/`.

The primary source of truth for token values is `reference/colors_and_type.css`. The component structure reference is `reference/AppBar.jsx`, `reference/SidebarPanel.jsx`, `reference/PivotTable.jsx`, and `reference/StatusBar.jsx`.

## Fidelity

**High-fidelity.** The prototypes use exact hex values, font sizes, weights, border radii, and shadow values extracted directly from the production source (`styles.js`). Recreate pixel-perfect using the existing React inline-style pattern already in use throughout `dash_tanstack_pivot/src/lib/`.

---

## Architecture: Where to Wire Tokens

The codebase already has a strong theming foundation. The main integration points are:

| File | What to do |
|---|---|
| `src/lib/utils/styles.js` | **Primary source** — already contains all theme objects. Add CSS custom property injection here (see below). |
| `src/lib/contexts/PivotThemeContext.js` | Expose the `injectCssVars` function from context so any component can consume tokens via `var(--sp-*)` |
| `src/lib/components/PivotAppBar.js` | Wire density tokens to `gridDimensionTokens.density.rowHeights` |
| Global stylesheet | Add `colors_and_type.css` vars as a baseline; override per theme via JS injection |

### Recommended Approach: CSS Variable Injection

When the theme changes, inject CSS custom properties onto `:root` (or a scoped container element) so components can use `var(--sp-primary)` etc. alongside the existing inline-style system. This allows gradual migration.

```js
// Add to styles.js or PivotThemeContext.js
export function injectThemeCssVars(theme, containerEl = document.documentElement) {
  const vars = {
    '--sp-primary':          theme.primary,
    '--sp-border':           theme.border,
    '--sp-header-bg':        theme.headerBg,
    '--sp-surface-bg':       theme.surfaceBg || theme.background,
    '--sp-surface-muted':    theme.surfaceMuted || theme.headerSubtleBg,
    '--sp-surface-inset':    theme.surfaceInset || theme.headerSubtleBg,
    '--sp-hierarchy-bg':     theme.hierarchyBg,
    '--sp-text':             theme.text,
    '--sp-text-sec':         theme.textSec,
    '--sp-text-soft':        theme.textSoft,
    '--sp-hover':            theme.hover,
    '--sp-hover-strong':     theme.hoverStrong || theme.hover,
    '--sp-select':           theme.select,
    '--sp-total-bg':         theme.totalBg,
    '--sp-total-bg-strong':  theme.totalBgStrong,
    '--sp-total-text':       theme.totalText,
    '--sp-total-text-strong':theme.totalTextStrong,
    '--sp-edited-bg':        theme.editedCellBg,
    '--sp-edited-border':    theme.editedCellBorder,
    '--sp-edited-text':      theme.editedCellText,
    '--sp-shadow-sm':        theme.shadowSm || '0 1px 2px rgba(15,23,42,0.05)',
    '--sp-shadow-md':        theme.shadowMd || '0 10px 24px rgba(15,23,42,0.06)',
    '--sp-shadow-inset':     theme.shadowInset || 'inset 0 1px 0 rgba(255,255,255,0.8)',
    '--sp-radius':           theme.radius || '4px',
    '--sp-radius-sm':        theme.radiusSm || '8px',
    '--sp-sidebar-bg':       theme.sidebarBg,
    '--sp-sorted-header-bg': theme.sortedHeaderBg,
    '--sp-sorted-border':    theme.sortedHeaderBorder || theme.primary,
    '--sp-sorted-text':      theme.sortedHeaderText,
  };
  Object.entries(vars).forEach(([k, v]) => {
    if (v !== undefined && v !== null) containerEl.style.setProperty(k, v);
  });
}
```

Call `injectThemeCssVars(theme)` inside `PivotThemeContext` whenever `themeName` changes.

---

## Design Tokens

### Typography

| Token | Value | Usage |
|---|---|---|
| `--sp-font-sans` | `'Inter', ui-sans-serif, system-ui, sans-serif` | All UI text |
| `--sp-font-mono` | `'JetBrains Mono', 'Courier New', monospace` | Numeric values, format preview |
| Base font size | `13px` | Grid cells, body |
| Section label | `11px / 700 / uppercase / 0.06em` | Section sub-headers |
| Panel title | `18px / 600` | Panel headings in demo app |
| Header cell | `13px / 600` | Sticky column headers |
| Button | `12px / 500` | Toolbar buttons |

**Google Fonts import (already in `app.py` external_stylesheets):**
```
https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap
```
Add JetBrains Mono:
```
https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap
```

### Spacing & Layout

| Token | Value |
|---|---|
| AppBar min-height | `60px` |
| Sidebar width | `288px` |
| Cell padding H | `0 12px` |
| Panel padding | `18px` |
| Chip padding | `6px 10px` |
| Content max-width | `1680px` |
| Row height (compact) | `36px` |
| Row height (normal) | `44px` |
| Row height (loose) | `56px` |
| Col width (hierarchy) | `320px` |
| Col width (dimension) | `190px` |
| Col width (measure) | `170px` |

These are already defined as `gridDimensionTokens` in `styles.js` — no changes needed there.

### Border Radius by Theme

| Theme(s) | `radius` | `radiusSm` |
|---|---|---|
| `flash`, `dark` | `16px` | `10px` |
| `blooomberg`, `bloomblerg_black` | `12px` | `8px` |
| `alabaster`, `strata`, `crystal`, `satin` | `8px` | `6–8px` |
| `light`, `material`, `balham` | `4px` | `8px` |

### Shadows (Flash / Light themes)

```css
--sp-shadow-sm:     0 1px 2px rgba(15, 23, 42, 0.05);
--sp-shadow-md:     0 10px 24px rgba(15, 23, 42, 0.06), 0 2px 8px rgba(15, 23, 42, 0.04);
--sp-shadow-inset:  inset 0 1px 0 rgba(255, 255, 255, 0.8);
```

Dark themes use `rgba(0,0,0,…)` with higher opacity (0.35–0.45).

---

## Component Specs

### AppBar (`PivotAppBar.js`)

- **Height:** `min-height: 60px`
- **Layout:** flexbox, space-between, `padding: 0 18px`, `gap: 12px`
- **Background:** `theme.headerBg`
- **Border bottom:** `1px solid theme.border`
- **Box shadow:** `theme.shadowInset` (inset top highlight)
- **Search box:** `background: isDark ? rgba(255,255,255,0.08) : theme.headerSubtleBg`, `border-radius: theme.radiusSm`, `padding: 4px 8px`, `width: 200px`
- **Separator:** `1px solid theme.border`, `height: 20px`
- **Theme pill:** border + `theme.headerSubtleBg` bg, `border-radius: theme.radiusSm`
- **Button hover:** `background: theme.hover`, `transition: background 120ms ease`

### Sidebar (`SidebarPanel.js`)

- **Width:** `288px` fixed
- **Background:** `theme.sidebarBg`
- **Border right:** `1px solid theme.border`
- **Padding:** `18px 14px`
- **Gap between sections:** `18px`
- **Section title:** `11px / 700 / uppercase / 0.06em / color: theme.textSec`
- **Chip:** white bg (dark: `rgba(255,255,255,0.07)`), `border: 1px solid theme.border`, `border-radius: theme.radiusSm`, `padding: 6px 10px`, `box-shadow: theme.shadowSm`, `transition: 120ms ease`
- **Drop zone:** `min-height: 52px`, `border: 1px solid #F3F4F6` (light) / `theme.border` (dark), `border-radius: theme.radiusSm`, `box-shadow: theme.shadowInset` (light only)
- **Value chip:** `background: isDark ? rgba(255,255,255,0.05) : rgba(79,70,229,0.06)`, primary-tinted border

### Table Headers

- **Height:** `38px` (header rows)
- **Font:** `13px / 600 / color: theme.text`
- **Background:** `theme.headerBg`
- **Border:** `1px solid theme.border` (right + bottom)
- **Sorted state:** `background: theme.sortedHeaderBg`, `color: theme.sortedHeaderText`, `border-bottom: 2px solid theme.sortedHeaderBorder`
- **Box shadow:** `theme.shadowInset` on header sticky wrapper
- **Hover:** `theme.hoverStrong` background, `transition: background 120ms`

### Table Cells

- **Font:** `13px`, `line-height: 1.45`
- **Padding:** `0 12px`
- **Background:** `theme.surfaceBg || theme.background`
- **Hierarchy column bg:** `theme.hierarchyBg`
- **Hover row:** entire row background → `theme.hover`
- **Selected cell:** `background: theme.select`, `box-shadow: inset 0 0 0 2px theme.primary`
- **Numeric values:** font-family `JetBrains Mono`, positive `#22C55E` / negative `#EF4444`

### Total Rows

- **Subtotal:** `background: theme.totalBg`, `color: theme.totalText`, `font-weight: 600`
- **Grand Total:** `background: theme.totalBgStrong`, `color: theme.totalTextStrong`, `font-weight: 700`

### Edited Cells

Already implemented in `buildEditedCellVisualStyle()` in `styles.js`. The key visual:
- Left stripe: `inset 3px 0 0 0 theme.editedCellBorder` (direct) / `inset 2px` (propagated)
- Background: `theme.editedCellBg` with radial glow
- Outline: `inset 0 0 0 1px` at low opacity

### Status Bar

- **Height:** `32px`
- **Background:** `theme.headerSubtleBg || theme.headerBg`
- **Border top:** `1px solid theme.border`
- **Font:** `11px`, `color: theme.textSec`
- **Padding:** `0 14px`

---

## Interactions & Behavior

| Interaction | Spec |
|---|---|
| Button hover | `background → theme.hover`, `transition: background 120ms ease` |
| Chip hover | `border-color + background`, `transition: 120ms ease` |
| Row hover | Full row background → `theme.hover` |
| Cell selection | `inset 0 0 0 2px theme.primary` ring + `theme.select` bg |
| Sort click | Toggle asc → desc → null. Immediate re-render via server callback. |
| Sidebar section toggle | `height: 0.3s ease` collapse, `transition: all 0.3s ease-in-out` |
| Density change | Row height updates; re-renders all virtual rows |
| Theme switch | Immediate: update `themeName` state → all inline styles recompute |
| No animations | No page-level transitions, no bounce/spring effects |

---

## Iconography

All icons are **custom inline SVG React components** defined in `src/lib/utils/Icons.js`. No external icon library dependency. Style: Material Design paths, `fill="currentColor"`, 14–18px.

Do not substitute with Heroicons, Lucide, or any other library — the existing icon set covers all needed glyphs.

---

## Assets

| Asset | Location | Notes |
|---|---|---|
| Icon set | `src/lib/utils/Icons.js` | 40 SVG React components |
| Styles/themes | `src/lib/utils/styles.js` | All theme objects + token derivation logic |
| CSS baseline | `reference/colors_and_type.css` | CSS custom properties for wiring |
| Inter font | Google Fonts CDN | Already in `app.py` external_stylesheets |
| JetBrains Mono | Google Fonts CDN | Add to external_stylesheets |

---

## Files in This Package

```
README.md                        ← This file (implement from here)
colors_and_type.css              ← CSS custom properties — wire into PivotThemeContext
AppBar.jsx                       ← AppBar component reference
SidebarPanel.jsx                 ← Sidebar field picker reference  
PivotTable.jsx                   ← Table body reference
StatusBar.jsx                    ← Status bar reference
```

### Preview cards (open in browser for visual reference)

```
preview/colors-flash.html        ← Flash theme palette
preview/colors-dark.html         ← Dark theme palette
preview/colors-bloomberg.html    ← Bloomberg themes
preview/colors-semantic.html     ← Row states, totals, edited cells
preview/colors-all-themes.html   ← All 11 theme swatches
preview/type-scale.html          ← Inter type scale
preview/type-mono.html           ← JetBrains Mono specimens
preview/spacing-tokens.html      ← Radius, shadows, density
preview/components-buttons.html  ← Button variants
preview/components-chips.html    ← Field chips + drop zones
preview/components-table.html    ← Table row states
preview/components-appbar.html   ← AppBar specimen
preview/components-sidebar.html  ← Sidebar specimen
preview/components-filters.html  ← Filter popover + context menu
```

### Full interactive prototype

```
index.html                       ← Full pivot demo (open in browser)
```

---

## Implementation Checklist

- [ ] Add `injectThemeCssVars()` to `styles.js`
- [ ] Call it in `PivotThemeContext` on theme change
- [ ] Add JetBrains Mono to `app.py` external_stylesheets
- [ ] Audit existing `fontFamily` strings — consolidate to `--sp-font-sans` / `--sp-font-mono`
- [ ] Add `--sp-font-sans` / `--sp-font-mono` fallback in global CSS
- [ ] Verify `theme.radiusSm` is used consistently for chips, buttons, drop zones
- [ ] Verify `theme.shadowInset` is applied to AppBar and sticky header wrapper
- [ ] Confirm `theme.hoverStrong` fallback to `theme.hover` everywhere
- [ ] Test all 11 themes after injection — check for missing token fallbacks
