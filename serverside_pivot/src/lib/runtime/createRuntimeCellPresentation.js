/** Creates pure cell validation and conditional-format presentation helpers.  @param {any} p0 @returns {any} */
export function createRuntimeCellPresentation({ conditionalFormatting }) {
  /** @param {any} [value] @param {any} [rule] @returns {any} */
  const validateCell = (value, rule) => {
    if (!rule) return true;
    if (rule.type === 'regex') return new RegExp(rule.pattern).test(value);
    if (rule.type === 'numeric') return !isNaN(parseFloat(value));
    if (rule.type === 'required') return value !== null && value !== '' && value !== undefined;
    return true;
  };

  /** @param {any} [colId] @param {any} [value] @returns {any} */
  const getRuleBasedStyle = (colId, value) => {
    if (typeof value !== 'number') return {};
    /** @type {any} */
    const rules = conditionalFormatting.filter(/** @param {any} [rule] @returns {any} */ rule => rule.column === colId || !rule.column);
    /** @type {Record<string, any>} */
    let style = {};
    for (const rule of rules) {
      let matches = false;
      if (rule.condition === '>') matches = value > rule.value;
      else if (rule.condition === '<') matches = value < rule.value;
      else if (rule.condition === '>=') matches = value >= rule.value;
      else if (rule.condition === '<=') matches = value <= rule.value;
      else if (rule.condition === '==') matches = value === rule.value;
      if (matches) style = { ...style, ...rule.style };
    }
    return style;
  };

  return { getRuleBasedStyle, validateCell };
}
