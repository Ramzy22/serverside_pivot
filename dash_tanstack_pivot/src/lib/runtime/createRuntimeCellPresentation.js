/** Creates pure cell validation and conditional-format presentation helpers. */
export function createRuntimeCellPresentation({ conditionalFormatting }) {
  const validateCell = (value, rule) => {
    if (!rule) return true;
    if (rule.type === 'regex') return new RegExp(rule.pattern).test(value);
    if (rule.type === 'numeric') return !isNaN(parseFloat(value));
    if (rule.type === 'required') return value !== null && value !== '' && value !== undefined;
    return true;
  };

  const getRuleBasedStyle = (colId, value) => {
    if (typeof value !== 'number') return {};
    const rules = conditionalFormatting.filter(rule => rule.column === colId || !rule.column);
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
