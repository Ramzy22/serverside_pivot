/**
 * ReactTanStackGrid.tsx - Reference Implementation
 * 
 * Demonstrates how to connect TanStack Table to the Pivot Engine backend
 * with WebSocket-based real-time invalidation.
 */
import React, { useMemo, useEffect } from 'react';
import { 
  useQuery, 
  useQueryClient,
  QueryClient, 
  QueryClientProvider 
} from '@tanstack/react-query';
import {
  useReactTable,
  getCoreRowModel,
  ColumnDef,
  flexRender,
} from '@tanstack/react-table';

const API_BASE = 'http://localhost:8000';
const API_KEY = 'your-secret-key-here';

// 1. Data Fetcher
const fetchPivotData = async (tableState: any) => {
  const response = await fetch(`${API_BASE}/pivot/tanstack`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
    },
    body: JSON.stringify({
      operation: 'get_data',
      table: 'sales', // Your table name
      columns: tableState.columnDefs,
      sorting: tableState.sorting,
      filters: tableState.columnFilters,
      grouping: tableState.grouping,
      pagination: tableState.pagination,
    }),
  });
  
  if (!response.ok) throw new Error('Network response was not ok');
  return response.json();
};

// 2. Main Grid Component
export const PivotGrid = () => {
  const queryClient = useQueryClient();
  
  // Real-time updates via WebSocket
  useEffect(() => {
    const ws = new WebSocket(`ws://localhost:8000/ws/pivot/client-123`);
    
    ws.onopen = () => {
      ws.send(JSON.stringify({ type: 'subscribe', table_name: 'sales' }));
    };

    ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.type === 'data_update') {
        console.log('Data change detected, invalidating queries...');
        // Trigger TanStack Query refetch
        queryClient.invalidateQueries({ queryKey: ['pivot', 'sales'] });
      }
    };

    return () => ws.close();
  }, [queryClient]);

  // Table State (Simplified)
  const columns = useMemo<ColumnDef<any>[]>(() => [
    { id: 'region', header: 'Region', accessorKey: 'region' },
    { id: 'category', header: 'Category', accessorKey: 'category' },
    { 
      id: 'total_sales', 
      header: 'Sales', 
      accessorKey: 'total_sales',
      // Metadata for backend aggregation
      meta: { aggregationFn: 'sum', aggregationField: 'amount' } 
    },
  ], []);

  const { data, isLoading } = useQuery({
    queryKey: ['pivot', 'sales'],
    queryFn: () => fetchPivotData({ columnDefs: columns }),
  });

  const table = useReactTable({
    data: data?.data?.data || [],
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualPagination: true,
    manualSorting: true,
    manualFiltering: true,
  });

  if (isLoading) return <div>Loading high-scale data...</div>;

  return (
    <div className="p-4">
      <table className="min-w-full border">
        <thead>
          {table.getHeaderGroups().map(headerGroup => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map(header => (
                <th key={header.id} className="border p-2">
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map(row => (
            <tr key={row.id}>
              {row.getVisibleCells().map(cell => (
                <td key={cell.id} className="border p-2">
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
