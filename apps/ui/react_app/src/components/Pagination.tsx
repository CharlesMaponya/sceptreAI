import { Button } from "./ui";

export function Pagination({ page, hasNext, onChange, label = "Results", loading = false }: {
  page: number; hasNext: boolean; onChange: (page: number) => void; label?: string; loading?: boolean;
}) {
  if (page === 0 && !hasNext) return null;
  return <nav className="pagination" aria-label={`${label} pagination`}>
    <Button variant="secondary" disabled={page === 0 || loading} onClick={() => onChange(page - 1)}>Previous</Button>
    <span aria-live="polite">Page {page + 1}</span>
    <Button variant="secondary" disabled={!hasNext || loading} onClick={() => onChange(page + 1)}>Next</Button>
  </nav>;
}
