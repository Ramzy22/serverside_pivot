import { useEffect, useRef, useState } from 'react';

export const EXPORT_ROW_LIMIT = 100_000;
export const COPY_ROW_LIMIT = 10_000;

/** State and request identity owned by the export/copy domain. */
export function useRuntimeExportState() {
  const [isExporting, setIsExporting] = useState(false);
  const [isCopying, setIsCopying] = useState(false);
  const [exportProgress, setExportProgress] = useState(/** @type {import('./runtimeDomains').ExportJobProgress | number | null} */ (null));
  const exportJobControllerRef = useRef(/** @type {import('./runtimeDomains').ExportJobController | null} */ (null));
  useEffect(() => () => exportJobControllerRef.current?.dispose(), []);
  const pendingCopyIntentRef = useRef(null);
  const exportRequestSequenceRef = useRef(0);
  const activeExportRequestIdRef = useRef(null);
  const cancelledExportRequestIdsRef = useRef(new Set());
  const [partModal, setPartModal] = useState(null);

  return {
    COPY_ROW_LIMIT,
    EXPORT_ROW_LIMIT,
    activeExportRequestIdRef,
    cancelledExportRequestIdsRef,
    exportProgress,
    exportJobControllerRef,
    exportRequestSequenceRef,
    isCopying,
    isExporting,
    partModal,
    pendingCopyIntentRef,
    setExportProgress,
    setIsCopying,
    setIsExporting,
    setPartModal,
  };
}
