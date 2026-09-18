import { GRAND_TOTAL_ROW_ID, applyRowPinning } from '../hooks/usePivotNormalization';

/** Creates the single UI command that updates row pinning and emits Dash state. */
export function createRuntimeRowPinCommand({
  requestLayoutHistoryCapture,
  setGrandTotalPinOverride,
  setProps,
  setRowPinningWithHistory,
}) {
  const handlePinRow = (rowId, pinState) => {
    requestLayoutHistoryCapture('layout:row-pinning');
    if (rowId === GRAND_TOTAL_ROW_ID) {
      setGrandTotalPinOverride(pinState === 'top' || pinState === 'bottom' ? pinState : false);
    }
    setRowPinningWithHistory(previous => applyRowPinning(previous, rowId, pinState), 'layout:row-pinning');
    if (setProps) {
      setProps({ rowPinned: { rowId, pinState, timestamp: Date.now() } });
    }
  };

  return { handlePinRow };
}
