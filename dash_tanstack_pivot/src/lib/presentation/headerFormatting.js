import { formatDisplayLabel } from '../utils/helpers';

const HEADER_STYLE_KEYS = new Set([
    'background', 'backgroundColor', 'color', 'border', 'borderColor',
    'borderTop', 'borderRight', 'borderBottom', 'borderLeft', 'boxShadow',
    'fontWeight', 'fontStyle', 'textDecoration', 'textTransform', 'letterSpacing',
]);
const HEADER_STYLE_CONFIG_KEYS = new Set([
    ...HEADER_STYLE_KEYS, 'bg', 'fill', 'foreground', 'fg', 'textColor',
    'headerBg', 'headerBackground', 'headerBackgroundColor', 'headerColor',
    'headerText', 'headerTextColor', 'style',
]);

/** Pure header-formatting policy; deliberately independent of React state. */
export function normalizeHeaderStyle(rawStyle) {
    if (!rawStyle) return {};
    if (typeof rawStyle === 'string') return rawStyle.trim() ? { background: rawStyle.trim() } : {};
    if (typeof rawStyle !== 'object' || Array.isArray(rawStyle)) return {};
    const inlineStyle = rawStyle.style && typeof rawStyle.style === 'object' && !Array.isArray(rawStyle.style)
        ? rawStyle.style : null;
    const source = inlineStyle ? { ...rawStyle, ...inlineStyle } : rawStyle;
    const style = {};
    HEADER_STYLE_KEYS.forEach((key) => {
        if (source[key] !== undefined && source[key] !== null && source[key] !== '') style[key] = source[key];
    });
    const background = source.background || source.backgroundColor || source.bg || source.fill
        || source.headerBg || source.headerBackground || source.headerBackgroundColor || source.headerColor;
    if (background !== undefined && background !== null && background !== '') {
        style.background = background;
        delete style.backgroundColor;
    }
    const color = source.color || source.textColor || source.foreground || source.fg
        || source.headerText || source.headerTextColor;
    if (color !== undefined && color !== null && color !== '') style.color = color;
    return style;
}

export function hasHeaderStyleKeys(value) {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
        && Object.keys(value).some((key) => HEADER_STYLE_CONFIG_KEYS.has(key));
}

export function addHeaderCandidate(candidates, rawValue) {
    if (rawValue === undefined || rawValue === null) return;
    const value = String(rawValue).trim();
    if (!value) return;
    candidates.add(value);
    candidates.add(value.toLowerCase());
    const display = formatDisplayLabel(value);
    if (display) {
        candidates.add(display);
        candidates.add(String(display).toLowerCase());
    }
}

export function headerRuleTargetMatches(rule, candidateKeys) {
    if (!rule || typeof rule !== 'object') return false;
    if (rule.all || rule.default || rule.column === '*' || rule.id === '*' || rule.field === '*') return true;
    const rawTargets = [rule.column, rule.columnId, rule.id, rule.field, rule.header,
        rule.headerLabel, rule.label, rule.group, rule.groupValue];
    if (Array.isArray(rule.columns)) rawTargets.push(...rule.columns);
    if (Array.isArray(rule.ids)) rawTargets.push(...rule.ids);
    const targets = rawTargets.filter((target) => target !== undefined && target !== null && String(target).trim())
        .map((target) => String(target).trim());
    if (targets.length === 0) return true;
    return targets.some((target) => candidateKeys.has(target)
        || candidateKeys.has(target.toLowerCase())
        || candidateKeys.has(formatDisplayLabel(target))
        || candidateKeys.has(String(formatDisplayLabel(target)).toLowerCase()));
}

export function headerRuleKindMatches(rule, level, headerKind) {
    if (!rule || typeof rule !== 'object') return false;
    if (rule.level !== undefined && rule.level !== null && Number(rule.level) !== Number(level)) return false;
    if (rule.kind || rule.type) {
        const expected = String(rule.kind || rule.type).trim().toLowerCase();
        if (expected && expected !== headerKind) return false;
    }
    return true;
}
