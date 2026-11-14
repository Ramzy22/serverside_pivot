import { useCallback, useEffect } from 'react';
import { createRuntimeRequest } from './runtimeContract';

const FILTER_OPTION_PAGE_SIZE = 250;

/**
 * Owns filter-popover state transitions and its server option request.
 * The request is emitted as the V1 runtime envelope before it crosses the
 * legacy state-override adapter.
  @param {any} p0 @returns {any} */
export function useRuntimeFilterPopover({
  activeFilterCol,
  dispatchServerSideRuntimeSetProps,
  filterOptionMeta,
  filterOptions,
  pendingServerFilterOptionsRef,
  serverSide,
  setActiveFilterCol,
  setFilterAnchorEl,
  setFilters,
  setTransportFilterOptionMetaState,
  showFilters,
}) {
  /** @type {(...args: any[]) => any} */
  const handleHeaderFilter = useCallback((columnId, filterValue) => {
    setFilters(/** @param {any} [previous] @returns {any} */ previous => {
      /** @type {Record<string, any>} */
      const next = { ...previous };
      if (filterValue === null || filterValue.conditions.length === 0) delete next[columnId];
      else next[columnId] = filterValue;
      return next;
    });
  }, [setFilters]);

  /** @type {(...args: any[]) => any} */
  const requestFilterOptions = useCallback((columnId, options = {}) => {
    if (!serverSide || !columnId) return false;
    /** @type {any} */
    const search = options && options.search !== undefined && options.search !== null ? String(options.search) : '';
    /** @type {any} */
    const requestedOffset = Number(options && options.offset);
    /** @type {any} */
    const offset = Number.isFinite(requestedOffset) && requestedOffset > 0 ? Math.floor(requestedOffset) : 0;
    const limit = FILTER_OPTION_PAGE_SIZE;
    const requestKey = `${columnId}::${search}::${offset}`;
    /** @type {any} */
    const previousMeta = filterOptionMeta && filterOptionMeta[columnId] ? filterOptionMeta[columnId] : null;
    if (offset === 0 && Object.prototype.hasOwnProperty.call(filterOptions || {}, columnId) && previousMeta && previousMeta.search === search && previousMeta.loading !== true) {
      pendingServerFilterOptionsRef.current = null;
      return false;
    }
    if (pendingServerFilterOptionsRef.current === requestKey) return false;
    pendingServerFilterOptionsRef.current = requestKey;
    setTransportFilterOptionMetaState(/** @param {any} [previous] @returns {any} */ previous => ({
      ...(previous || {}),
      [columnId]: {
        ...(previous && previous[columnId] || {}),
        search,
        offset,
        limit,
        loading: true,
      },
    }));
    const nonce = `${Date.now()}:${requestKey}`;
    dispatchServerSideRuntimeSetProps({
      runtimeRequest: createRuntimeRequest({
        kind: 'filter_options',
        requestId: nonce,
        payload: { columnId, search, offset, limit, nonce },
      }),
    });
    return true;
  }, [dispatchServerSideRuntimeSetProps, filterOptionMeta, filterOptions, pendingServerFilterOptionsRef, serverSide, setTransportFilterOptionMetaState]);

  /** @type {(...args: any[]) => any} */
  const handleFilterClick = useCallback((event, columnId) => {
    if (!showFilters) return;
    event.stopPropagation();
    setActiveFilterCol(columnId);
    setFilterAnchorEl(event.currentTarget);
    requestFilterOptions(columnId);
  }, [requestFilterOptions, setActiveFilterCol, setFilterAnchorEl, showFilters]);

  useEffect(() => {
    if (!serverSide || !activeFilterCol) {
      pendingServerFilterOptionsRef.current = null;
      return;
    }
    if (filterOptions && filterOptions[activeFilterCol]) {
      pendingServerFilterOptionsRef.current = null;
      return;
    }
    requestFilterOptions(activeFilterCol);
  }, [activeFilterCol, filterOptions, pendingServerFilterOptionsRef, requestFilterOptions, serverSide]);

  /** @type {(...args: any[]) => any} */
  const closeFilterPopover = useCallback(() => {
    setActiveFilterCol(null);
    setFilterAnchorEl(null);
  }, [setActiveFilterCol, setFilterAnchorEl]);

  return {
    closeFilterPopover,
    handleFilterClick,
    handleHeaderFilter,
    requestFilterOptions,
  };
}
